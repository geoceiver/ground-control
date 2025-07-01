use ground_control::{data::sp3::{Sp3DataClient, Sp3File}, gpst::current_gpst_seconds};
use object_store::{path::Path, ObjectStore, PutPayload};
use restate_sdk::prelude::*;

use tracing::warn;
use crate::{r2::{r2_cddis_bucket, r2_get_archived_directory_listing, r2_put_archived_directory_listing}, utils::build_reqwest_client};

const MAX_FILE_SIZE:u64 = 100_000_000;

// Error objects
#[derive(serde::Serialize, serde::Deserialize,Debug, PartialEq, Clone)]
pub enum FileError {
    FileNotFound,
    FileTooLarge(u64),    // received file size
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
    pub queue_num:u32,
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
pub trait CDDISFileQueue {
    async fn archive_file(file_request:Json<CDDISFileRequest>) -> Result<(), HandlerError>;

    async fn update_archive_manifest(file_request:Json<CDDISFileRequest>) -> Result<(), HandlerError>;

    #[shared]
    async fn get_status() -> Result<Json<CDDISFileQueueStatus>, HandlerError>;
}

pub struct CDDISFileQueueImpl;

impl CDDISFileQueue for CDDISFileQueueImpl {

    // this function seralizes status update messages for a CDDIS week directory
    // across all archival tasks to prevent parallel requests from overwriting manifest udpates
    async  fn update_archive_manifest(&self, ctx: ObjectContext<'_>, file_request:Json<CDDISFileRequest>) -> Result<(),HandlerError> {

        let file_request = file_request.into_inner();
        let mut directory_listing = ctx.run(||r2_get_archived_directory_listing(file_request.week)).await?.into_inner();

        directory_listing.add_file(file_request.archive_path.to_string(), file_request.hash);
        r2_put_archived_directory_listing(file_request.week, &directory_listing).await?;

        Ok(())
    }

    async  fn archive_file(&self, ctx: ObjectContext<'_> ,file_request:Json<CDDISFileRequest>) -> Result<(),HandlerError> {

        let file_request:CDDISFileRequest = file_request.into_inner();
        let status_response = ctx.get::<Json<CDDISFileQueueStatus>>("status").await?;

        let mut status;
        if status_response.is_none() {
            let time_started = ctx.run(current_gpst_seconds).await?;
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
            .send().await;

        let response = response.unwrap();
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

            let r2_bucket = r2_cddis_bucket()?;
            let response = r2_bucket.put(&Path::from_absolute_path(&file_request.archive_path)?, PutPayload::from_bytes(response.bytes().await?)).await;

            if response.is_err() {

                let file_error = CDDISFileRequestError {
                    error:FileError::UploadError,
                    file_request: file_request.clone(),
                    cddis_size: content_length.unwrap() as u64
                };

                status.file_errors.push(file_error);
            }
            else {

                if file_request.process_files  {
                    let sp3_file = Sp3File {source:"cddis".to_string(), archive_path: file_request.archive_path.clone()};
                    if sp3_file.is_sp3() {
                        ctx.object_client::<Sp3DataClient>("cddis").process_sp3_file(Json(sp3_file)).send();
                    }
                }

                // send success message to update_manifest to serialize updates per week directory
                let week_key = format!("cddis_queue_{}", file_request.week);

                ctx.object_client::<CDDISFileQueueClient>(week_key).update_archive_manifest(Json(file_request.clone())).send();
            }
        }

        status.completed_files += 1;

        ctx.set("status", Json(status.clone()));
        Ok(())
    }

    async  fn get_status(&self, ctx: SharedObjectContext<'_>) -> Result<Json<CDDISFileQueueStatus> ,HandlerError> {

        let status = ctx.get::<Json<CDDISFileQueueStatus>>("status").await?;
        if let Some(status) = status {
            return Ok(status);
        }

        Err(TerminalError::new(format!("Status not found for queue_id: {}", ctx.key())).into())
    }

}
