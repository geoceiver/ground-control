
use std::env;
use object_store::{aws::{AmazonS3, AmazonS3Builder}, path::Path, Error::NotFound, ObjectStore, PutPayload};
use restate_sdk::{errors::{HandlerError, TerminalError}, serde::{Json, Serialize}};

use crate::{archiver::DirectoryListing, cddis::get_archive_file_path};

pub fn r2_cddis_bucket() -> Result<AmazonS3, object_store::Error> {

    let key = env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID is set and a valid String");
    let secret = env::var("AWS_SECRET_ACCESS_KEY").expect("AWS_ACCESS_KEY_ID is set and a valid String");

    AmazonS3Builder::new()
        .with_url("https://35bb40698ef5bd005fe8af515201e351.r2.cloudflarestorage.com/geoceiver-archive-enam")
        .with_region("enam")
        .with_access_key_id(key)
        .with_secret_access_key(secret)
        .build()
}


// pub async fn r2_get_object_url(path:&str) -> Url {


//     let get_object = r2_bucket.get(path).await?; //bucket.get_object(Some(&credentials), path);
//     let presigned_url_duration = Duration::from_secs(60);
//     get_object.sign(presigned_url_duration)
// }

// pub fn r2_put_object_url(path:&str) -> Url {
//     let bucket = r2_cddis_bucket();
//     let credentials = r2_credentials();

//     let put_object = bucket.put_object(Some(&credentials), path);
//     let presigned_url_duration = Duration::from_secs(60);
//     put_object.sign(presigned_url_duration)
// }



// pub fn r2_object_head(path:&str) -> Url {
//     let bucket = r2_cddis_bucket();
//     let credentials = r2_credentials();

//     let head_object = bucket.head_object(Some(&credentials), path);
//     let presigned_url_duration = Duration::from_secs(60);
//     head_object.sign(presigned_url_duration)
// }


pub async fn r2_get_archived_directory_listing(week:u32) -> Result<Json<DirectoryListing>, HandlerError> {

    let listing_path = get_archive_file_path(week, "sha512.json");

    let r2_bucket = r2_cddis_bucket()?;
    let response = r2_bucket.get(&Path::from_absolute_path(listing_path)?).await;
    let archived_listing:DirectoryListing;

    if response.is_ok() {
        let data = response.unwrap().bytes().await?;
        archived_listing = serde_json::from_slice(data.as_ref())?;
        if archived_listing.week.is_none() {
            let mut updated_archived_listing = archived_listing.clone();
            updated_archived_listing.week = Some(week);
            r2_put_archived_directory_listing(week, &updated_archived_listing);
        }
        return Ok(Json(archived_listing));
    }
    else if response.is_err() {
        match response.err().unwrap() {
            NotFound { path: _, source: _ } => return Ok(Json(DirectoryListing::new(week))),
            _ => (),
        }
    }

    return Err(TerminalError::new("Unable to load archived directory listing.").into());
}

pub async fn r2_put_archived_directory_listing(week:u32, archived_listing:&DirectoryListing) -> Result<(),HandlerError> {

    let listing_path = get_archive_file_path(week, "sha512.json");

    let r2_bucket = r2_cddis_bucket()?;
    let payload = PutPayload::from_bytes(Json(archived_listing).serialize()?);
    let response = r2_bucket.put(&Path::from_absolute_path(listing_path)?, payload).await;

    if response.is_ok() {
        return Ok(());
    }

    return Err(TerminalError::new("Unable to save archived directory listing.").into());
}


// move this to sp3 processing endpoint ?
//
//
// pub async fn r2_get_gz_object_buffer(path:&str) -> Result<BufReader<GzDecoder<Reader<Bytes>>>, anyhow::Error>  {

//     let sp3_url = s3_get_object_url(path);
//     let client = build_reqwest_client()?;
//     let sp3_request = client.get(sp3_url).header(ACCEPT_ENCODING, "gzip").send().await?;
//     if sp3_request.status().is_success() {
//         let sp3_reader = sp3_request.bytes().await?.reader();
//         let fd = GzDecoder::new(sp3_reader);
//         let buf = BufReader::new(fd);
//         return Ok(buf);
//     }

//     error!("File not found: {}", path);
//     Err(anyhow::anyhow!("File not found: {}", path))
// }
