pub mod cddis;
pub mod sources;

use std::{env, io::BufReader, time::Duration};
use bytes::{buf::Reader, Buf, Bytes};
use flate2::bufread::GzDecoder;
use hifitime::Epoch;
use reqwest::{header::ACCEPT_ENCODING, Url};
use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use tracing::error;

pub fn gpst_week(epoch:&Epoch) -> u64 {
    let gpst_week = (epoch.to_gpst_days() / 7.0).floor() as u64;
    gpst_week
}


pub fn build_reqwest_client() -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder().use_rustls_tls().pool_max_idle_per_host(0).build()
}

fn s3_cddis_bucket() -> Bucket {
    // setting up a bucket
    let endpoint = "https://35bb40698ef5bd005fe8af515201e351.r2.cloudflarestorage.com".parse().expect("endpoint is a valid Url");
    let path_style = UrlStyle::VirtualHost;
    let name = "cddis-deep-archive";
    let region = "enam";
    Bucket::new(endpoint, path_style, name, region).expect("Url has a valid scheme and host")
}

fn s3_credentials() -> Credentials {
    // setting up the credentials
    let key = env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID is set and a valid String");
    let secret = env::var("AWS_SECRET_ACCESS_KEY").expect("AWS_ACCESS_KEY_ID is set and a valid String");
    Credentials::new(key, secret)
}

pub fn s3_get_object_url(path:&str) -> Url {
    let bucket = s3_cddis_bucket();
    let credentials = s3_credentials();

    let get_object = bucket.get_object(Some(&credentials), path);
    let presigned_url_duration = Duration::from_secs(60);
    get_object.sign(presigned_url_duration)
}

pub async fn s3_get_gz_object_buffer(path:&str) -> Result<BufReader<GzDecoder<Reader<Bytes>>>, anyhow::Error>  {

    let sp3_url = s3_get_object_url(path);
    let client = build_reqwest_client()?;
    let sp3_request = client.get(sp3_url).header(ACCEPT_ENCODING, "gzip").send().await?;
    if sp3_request.status().is_success() {
        let sp3_reader = sp3_request.bytes().await?.reader();
        let fd = GzDecoder::new(sp3_reader);
        let buf = BufReader::new(fd);
        return Ok(buf);
    }

    error!("File not found: {}", path);
    Err(anyhow::anyhow!("File not found: {}", path))
}

pub fn s3_put_object_url(path:&str) -> Url {
    let bucket = s3_cddis_bucket();
    let credentials = s3_credentials();

    let get_object = bucket.put_object(Some(&credentials), path);
    let presigned_url_duration = Duration::from_secs(60);
    get_object.sign(presigned_url_duration)
}


pub fn s3_object_head(path:&str) -> Url {
    let bucket = s3_cddis_bucket();
    let credentials = s3_credentials();

    let head_object = bucket.head_object(Some(&credentials), path);
    let presigned_url_duration = Duration::from_secs(60);
    head_object.sign(presigned_url_duration)
}
