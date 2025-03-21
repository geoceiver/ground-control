use std::{env, time::Duration};

use reqwest::{StatusCode, Url};
use restate_sdk::{errors::{HandlerError, TerminalError}, serde::Json};
use rusty_s3::{Bucket, Credentials, S3Action as _, UrlStyle};

use crate::{archiver::DirectoryListing, cddis::get_archive_file_path, utils::build_reqwest_client};


pub fn r2_cddis_bucket() -> Bucket {
    // setting up a bucket
    let endpoint = "https://35bb40698ef5bd005fe8af515201e351.r2.cloudflarestorage.com".parse().expect("endpoint is a valid Url");
    let path_style = UrlStyle::VirtualHost;
    let name = "geoceiver-archive-enam";
    let region = "enam";
    Bucket::new(endpoint, path_style, name, region).expect("Url has a valid scheme and host")
}

pub fn r2_credentials() -> Credentials {
    // setting up the credentials
    let key = env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID is set and a valid String");
    let secret = env::var("AWS_SECRET_ACCESS_KEY").expect("AWS_ACCESS_KEY_ID is set and a valid String");
    Credentials::new(key, secret)
}

pub fn r2_get_object_url(path:&str) -> Url {
    let bucket = r2_cddis_bucket();
    let credentials = r2_credentials();

    let get_object = bucket.get_object(Some(&credentials), path);
    let presigned_url_duration = Duration::from_secs(60);
    get_object.sign(presigned_url_duration)
}

pub fn r2_put_object_url(path:&str) -> Url {
    let bucket = r2_cddis_bucket();
    let credentials = r2_credentials();

    let put_object = bucket.put_object(Some(&credentials), path);
    let presigned_url_duration = Duration::from_secs(60);
    put_object.sign(presigned_url_duration)
}



pub fn r2_object_head(path:&str) -> Url {
    let bucket = r2_cddis_bucket();
    let credentials = r2_credentials();

    let head_object = bucket.head_object(Some(&credentials), path);
    let presigned_url_duration = Duration::from_secs(60);
    head_object.sign(presigned_url_duration)
}


pub async fn r2_get_archived_directory_listing(week:u32) -> Result<Json<DirectoryListing>, HandlerError> {

    let listing_path = get_archive_file_path(week, "sha512.json");

    let listing_url = r2_get_object_url(listing_path.as_str());

    let client = build_reqwest_client()?;
    let response = client.get(listing_url).send().await?;

    let archived_listing:DirectoryListing;
    let status_code = response.status();
    if status_code.is_success() {
        let txt = response.text().await?.to_string();
        archived_listing = serde_json::from_str(&txt)?;
        if archived_listing.week.is_none() {
            let mut updated_archived_listing = archived_listing.clone();
            updated_archived_listing.week = Some(week);
            r2_put_archived_directory_listing(week, &updated_archived_listing);
        }
        return Ok(Json(archived_listing));
    }
    else if status_code == StatusCode::NOT_FOUND {
        return Ok(Json(DirectoryListing::new(week)));
    }

    return Err(TerminalError::new("Unable to load archived directory listing.").into());
}

pub async fn r2_put_archived_directory_listing(week:u32, archived_listing:&DirectoryListing) -> Result<(),HandlerError> {

    let listing_path = get_archive_file_path(week, "sha512.json");

    let listing_url = r2_put_object_url(listing_path.as_str());

    let client = build_reqwest_client()?;
    let response = client.put(listing_url).json(archived_listing).send().await?;

    if response.status().is_success() {
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
