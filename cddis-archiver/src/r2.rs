
use std::env;
use object_store::{aws::{AmazonS3, AmazonS3Builder}, path::Path, Error::NotFound, ObjectStore, PutPayload};
use restate_sdk::{errors::{HandlerError, TerminalError}, serde::{Json, Serialize}};

use crate::{archiver::DirectoryListing, cddis::get_cddis_archive_file_path};

const WEEKLY_INDEX_FILENAME:&str = "sha512.json";

pub fn r2_cddis_bucket() -> Result<AmazonS3, object_store::Error> {

    let r2_path = env::var("R2_PATH").expect("R2 path is set and a vaild String");
    let r2_region = env::var("R2_REGION").expect("R2 region is set and a vaild String");
    let key = env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID is set and a valid String");
    let secret = env::var("AWS_SECRET_ACCESS_KEY").expect("AWS_ACCESS_KEY_ID is set and a valid String");

    AmazonS3Builder::new()
        .with_url(r2_path)
        .with_region(r2_region)
        .with_access_key_id(key)
        .with_secret_access_key(secret)
        .build()
}

pub async fn r2_get_archived_directory_listing(gps_week:u32) -> Result<Json<DirectoryListing>, HandlerError> {

    let listing_path = get_cddis_archive_file_path(gps_week, WEEKLY_INDEX_FILENAME);

    let r2_bucket = r2_cddis_bucket()?;
    let response = r2_bucket.get(&Path::from_absolute_path(listing_path)?).await;
    let archived_listing:DirectoryListing;

    if response.is_ok() {
        let data = response.unwrap().bytes().await?;
        archived_listing = serde_json::from_slice(data.as_ref())?;
        if archived_listing.week.is_none() {
            let mut updated_archived_listing = archived_listing.clone();
            updated_archived_listing.week = Some(gps_week);
            r2_put_archived_directory_listing(gps_week, &updated_archived_listing).await?;
        }
        return Ok(Json(archived_listing));
    }
    else if response.is_err() {
        if let NotFound { path: _, source: _ } = response.err().unwrap() { return Ok(Json(DirectoryListing::new(gps_week))) }
    }

    Err(TerminalError::new("Unable to load archived directory listing.").into())
}

pub async fn r2_put_archived_directory_listing(gps_week:u32, archived_listing:&DirectoryListing) -> Result<(),HandlerError> {

    let listing_path = get_cddis_archive_file_path(gps_week, WEEKLY_INDEX_FILENAME);

    let r2_bucket = r2_cddis_bucket()?;
    let payload = PutPayload::from_bytes(Json(archived_listing).serialize()?);
    let response = r2_bucket.put(&Path::from_absolute_path(listing_path)?, payload).await;

    if response.is_ok() {
        return Ok(());
    }

    Err(TerminalError::new("Unable to save archived directory listing.").into())
}
