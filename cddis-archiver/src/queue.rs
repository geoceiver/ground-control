use std::time::Duration;

use ground_control::gpst::current_gpst_seconds;
use reqwest::Body;
use restate_sdk::prelude::*;
use futures_util::{StreamExt, TryStreamExt};
use rusty_s3::{actions::CreateMultipartUpload, S3Action};

use tracing::{info, warn};
use crate::{r2::{r2_cddis_bucket, r2_credentials, r2_get_archived_directory_listing, r2_put_archived_directory_listing}, utils::build_reqwest_client};

//const CHUNK_SIZE:usize = 100_000;
const MAX_FILE_SIZE:u64 = 100_000_000;

// Error objects

#[derive(serde::Serialize, serde::Deserialize,Debug, PartialEq, Clone)]
pub enum FileError {
    FileNotFound,
    FileTooLarge(u64),
    InvalidType,
    HashMismatch(String), // received hash value
    UploadError
}

#[derive(serde::Serialize, serde::Deserialize,Debug, PartialEq, Clone)]
pub struct CDDISFileRequestError{
    error:FileError,
    file_request:CDDISFileRequest,
    cddis_size:u64
}


#[derive(serde::Serialize, serde::Deserialize,Debug, PartialEq, Clone)]
pub struct CDDISFileRequest {
    pub request_queue:CDDISFileQueueData,
    pub week:u32,
    pub path:String,
    pub hash:String,
    pub archive_path:String,
    pub process_files:bool,
}

#[derive(serde::Serialize, serde::Deserialize,Debug, PartialEq, Clone)]
pub struct CDDISFileQueueData {
    pub request_id:String,
    //pub week:u32,
    pub queue_num:u32,
    //pub enqueued_files:u32,
    //pub awakeable_id:String
}

impl CDDISFileQueueData {
    pub fn get_key(&self) -> String {
        format!("cddis_queue_{}_{}", self.request_id, self.queue_num)
    }
}


#[derive(serde::Serialize, serde::Deserialize,Debug, PartialEq, Clone)]
pub struct CDDISFileQueueStatus {
    queue:CDDISFileQueueData,
    completed_files:u32,
    file_errors:Vec<CDDISFileRequestError>,
    time_started:f64,
    time_completed:Option<f64>,
    last_udpate:Option<f64>
}

#[restate_sdk::object]
pub trait CDDISArchiverFileQueue {
    async fn archive_file(file_request:Json<CDDISFileRequest>) -> Result<(), HandlerError>;

    async fn update_archive_manifest(file_request:Json<CDDISFileRequest>) -> Result<(), HandlerError>;

    #[shared]
    async fn get_status() -> Result<Json<CDDISFileQueueStatus>, HandlerError>;
}

pub struct CDDISArchiverFileQueueImpl;

impl CDDISArchiverFileQueue for CDDISArchiverFileQueueImpl {


    // this fucntion seralizes status update messages on queue_id_week
    async  fn update_archive_manifest(&self, ctx: ObjectContext<'_>, file_request:Json<CDDISFileRequest>) -> Result<(),HandlerError> {

        let file_request = file_request.into_inner();
        let mut directory_listing = ctx.run(||r2_get_archived_directory_listing(file_request.week)).await?.into_inner();

        warn!("xx {} adding file to manifest: {} ({})", ctx.key(), &file_request.archive_path, directory_listing.files.len());

        directory_listing.add_file(file_request.archive_path.to_string(), file_request.hash);

        warn!("yy {} adding file to manifest: {} ({})", ctx.key(), &file_request.archive_path, directory_listing.files.len());

        r2_put_archived_directory_listing(file_request.week, &directory_listing).await?;

        Ok(())
    }

    async  fn archive_file(&self, ctx: ObjectContext<'_> ,file_request:Json<CDDISFileRequest>) -> Result<(),HandlerError> {

        let file_request:CDDISFileRequest = file_request.into_inner();

        let status_response = ctx.get::<Json<CDDISFileQueueStatus>>("status").await?;

        let mut status;
        if !status_response.is_some() {
            let time_started = ctx.run(||current_gpst_seconds()).await?;
            status = CDDISFileQueueStatus {
                queue: file_request.request_queue.clone(),
                completed_files: 0,
                file_errors: Vec::new(),
                time_started,
                time_completed: None,
                last_udpate: None,
            };
            ctx.set("status", Json(status.clone()));
        }
        else {
            status = status_response.unwrap().into_inner();
        }

        let client = build_reqwest_client()?;

        let response = client.get(&file_request.path)
            .bearer_auth(std::env::var("EARTHDATA_TOKEN").unwrap())
            .send().await?;

        let content_length = response.content_length();
        if content_length.is_some() && content_length.unwrap() > MAX_FILE_SIZE {

            warn!("file {} too large: {}", file_request.archive_path.to_string(), content_length.unwrap());

            let file_error = CDDISFileRequestError {
                error:FileError::FileTooLarge(content_length.unwrap()),
                file_request: file_request.clone(),
                cddis_size: content_length.unwrap() as u64
            };
            status.file_errors.push(file_error);

        }
        else {

            let bucket = r2_cddis_bucket();
            let credentials = r2_credentials();

            let put_object = bucket.put_object(Some(&credentials), &file_request.archive_path);
            let put_url = put_object.sign(Duration::from_secs(30));

            // pushing buffer directly...TODO add support for R2's busted multipart uplaods implementation...
            let result = client.put(put_url).body(response.bytes().await?).send().await;

            if !result.is_ok() ||
                !result.unwrap().status().is_success() {

                warn!("file upload error for: {}", file_request.archive_path);

                // TODO need to refine error logging
                let file_error = CDDISFileRequestError {
                    error:FileError::UploadError,
                    file_request: file_request.clone(),
                    cddis_size: content_length.unwrap() as u64
                };
                status.file_errors.push(file_error);
            }
            else {

                // send success message to update_manifest to serialize updates per week directory
                let week_key = format!("{}_{}", status.queue.get_key(), file_request.week);

                info!("sending update manifest for {} ({})", file_request.archive_path, week_key);

                ctx.object_client::<CDDISArchiverFileQueueClient>(week_key).update_archive_manifest(Json(file_request.clone())).send();
            }

        }

        status.completed_files += 1;

        ctx.set("status", Json(status.clone()));

        // if status.completed_files == status.queue.enqueued_files {
        //     ctx.resolve_awakeable(&status.queue.awakeable_id, "".to_string());
        // }

        info!("{}: {}", file_request.archive_path, status.completed_files);

        // commmenting out multipart uploads pending implementation on r2
        //

        // let stream = response.bytes_stream().map(|chunk| {
        //         chunk.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
        //     });

        // let stream_reader = StreamReader::new(stream);
        // let mut buffered_reader = BufReader::new(stream_reader);



        // let multpart_upload = bucket.create_multipart_upload(Some(&credentials), &file_request.archive_path);
        // let create_multipart_url = multpart_upload.sign(Duration::from_secs(30));

        // let response = client.post(create_multipart_url).send().await?;
        // info!("{:?}", &response);
        // let resposne_text = response.text().await?;
        // info!("{}", resposne_text);



        // let multipart_response = CreateMultipartUpload::parse_response(resposne_text)?;
        // let upload_id = multipart_response.upload_id();

        // let mut stream_buffer = [0; CHUNK_SIZE];

        // let mut total_bytes_uploaded = 0;
        // let mut chunk_num = 1;

        // let mut etags:Vec<String> = Vec::new();
        // let mut current_chunk:Vec<u8> = Vec::new();

        // loop {
        //     let len = buffered_reader.read(&mut stream_buffer).await?;

        //     current_chunk.append(&mut stream_buffer[..len].to_vec());

        //     if current_chunk.len() >= CHUNK_SIZE ||
        //         (len == 0 && current_chunk.len() > 0) {

        //         let upload_part = bucket.upload_part(Some(&credentials), &file_request.archive_path, chunk_num, upload_id);
        //         let upload_part_url = upload_part.sign(Duration::from_secs(30));

        //         let chunk_len;
        //         if current_chunk.len() > CHUNK_SIZE {
        //             chunk_len = CHUNK_SIZE;
        //         }
        //         else {
        //             chunk_len = current_chunk.len()
        //         }

        //         let stream_slice:Vec<u8> = current_chunk[..chunk_len].to_vec();
        //         let chunk_body = Body::from(stream_slice);
        //         let response = client.put(upload_part_url)
        //             .body(chunk_body)
        //             .send().await?;

        //         if !response.status().is_success() {
        //             info!("uploaded chunk {} size: {}", chunk_num, len);
        //         }

        //         let upload_part_etag = response.headers().get("etag").unwrap()
        //             .to_str().unwrap().to_owned();

        //         etags.push(upload_part_etag);

        //         total_bytes_uploaded += len;
        //         chunk_num += 1;
        //         info!("uploaded chunk {} size: {}", chunk_num, len);
        //     }

        //     if len == 0 {
        //         break;
        //     }
        // }

        // let etag_strs = etags.iter().map(|s|s.as_str());
        // let complete_upload = bucket.complete_multipart_upload(Some(&credentials), &file_request.archive_path, upload_id, etag_strs);
        // let complete_upload_url = complete_upload.sign(Duration::from_secs(60));
        // let complete_body = complete_upload.body();

        // let response = client.post(complete_upload_url).body(complete_body)
        //     .send().await?;

        // if !response.status().is_success() {
        //     let response_text = response.text().await?;
        //     info!("upload complete failed: {}", response_text);
        //     //info!("{:?}", response.status());
        // }

        // info!("total uploaded bytes: {}", total_bytes_uploaded);

        Ok(())
    }

    async  fn get_status(&self, ctx: SharedObjectContext<'_>) -> Result<Json<CDDISFileQueueStatus> ,HandlerError> {

        let status = ctx.get::<Json<CDDISFileQueueStatus>>("status").await?;
        if status.is_some() {
            return Ok(status.unwrap());
        }

        Err(TerminalError::new(format!("Status not found for queue_id: {}", ctx.key())).into())
    }

}
