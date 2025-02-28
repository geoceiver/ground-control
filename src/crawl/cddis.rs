use std::collections::BTreeMap;
use std::env;
use std::{fmt::Debug, time::Duration};
use futures::StreamExt;
use hifitime::Epoch;
use reqwest::Url;
use restate_sdk::prelude::*;
use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_with::serde_as;
use tracing::info;


const MIN_GPST_WEEKS: u64 = 2238; // CDDIS format changed for products prior to week 2238
const WEEK_LOOKBACK_PERIOD: u64 = 12; // Look back 12 weeks for updated productss
const UPDATE_FREQUENCY:f64 = 30.0; //60 * 10; // update every ten minutes
const CDDIS_PATH:&str = "https://cddis.nasa.gov/archive/gnss/products";

fn s3_bucket() -> Bucket {
    // setting up a bucket
    let endpoint = "https://ewr1.vultrobjects.com".parse().expect("endpoint is a valid Url");
    let path_style = UrlStyle::VirtualHost;
    let name = "cddis-archive";
    let region = "vultr-ewr1-1";
    Bucket::new(endpoint, path_style, name, region).expect("Url has a valid scheme and host")
}

fn s3_credentials() -> Credentials {
    // setting up the credentials
    let key = env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID is set and a valid String");
    let secret = env::var("AWS_SECRET_ACCESS_KEY").expect("AWS_ACCESS_KEY_ID is set and a valid String");
    Credentials::new(key, secret)
}

fn s3_get_object_url(path:&str) -> Url {
    let bucket = s3_bucket();
    let credentials = s3_credentials();

    let get_object = bucket.get_object(Some(&credentials), path);
    let presigned_url_duration = Duration::from_secs(60);
    get_object.sign(presigned_url_duration)
}

fn s3_put_object_url(path:&str) -> Url {
    let bucket = s3_bucket();
    let credentials = s3_credentials();

    let get_object = bucket.put_object(Some(&credentials), path);
    let presigned_url_duration = Duration::from_secs(60);
    get_object.sign(presigned_url_duration)
}

pub fn gpst_week(epoch:&Epoch) -> u64 {
    let gpst_week = (epoch.to_gpst_days() / 7.0).floor() as u64;
    gpst_week
}

fn get_cddis_week_path(week:u64) -> String {
    format!("{}/{}", CDDIS_PATH, week)
}

fn get_cddis_file_path(file_path:&str) -> String {
    format!("{}/{}", CDDIS_PATH, file_path)
}

// async so it can be wraped in a restate run closure
async fn current_gpst_seconds() -> Result<f64, HandlerError> {
    Ok(Epoch::now()?.to_gpst_seconds())
}

// async so it can be wraped in a restate run closure
async fn current_gpst_week() -> Result<u64, HandlerError> {
    Ok(gpst_week(&Epoch::now()?))
}

async fn get_archived_directory_listing(week:u64) -> Result<Json<DirectoryListing>, HandlerError> {

    let listing_path = format!("{}/sha512.json", week);

    let listing_url = s3_get_object_url(listing_path.as_str());

    let client = reqwest::Client::builder().pool_max_idle_per_host(0).build()?;
    let listing_response = client.get(listing_url).send().await;

    let archived_listing:DirectoryListing;
    if listing_response.is_ok() {
        archived_listing = serde_json::from_str(listing_response.unwrap().text().await?.as_str())?;
    }
    else {
        archived_listing = DirectoryListing::default();
    }

    Ok(Json(archived_listing))
}
async fn put_archived_directory_listing(week:u64, archived_listing:&DirectoryListing) -> Result<(), HandlerError> {

    let listing_path = format!("{}/sha512.json", week);

    let listing_url = s3_put_object_url(listing_path.as_str());

    let client = reqwest::Client::builder().pool_max_idle_per_host(0).build()?;
    let body = json!(archived_listing);
    client.put(listing_url).body(body.to_string()).send().await?;


    Ok(())
}

async fn get_current_directory_listing(week:u64) -> Result<Json<DirectoryListing>, HandlerError> {

    let client = reqwest::Client::builder().pool_max_idle_per_host(0).build()?;
    let response = client.get(format!("{}/SHA512SUMS", get_cddis_week_path(week)))
        .bearer_auth(std::env::var("EARTHDATA_TOKEN").unwrap())
        .send()
        .await?;

    let directory_listing_text = response.text().await?;

    let mut directory_listing = DirectoryListing::default();

    for line in directory_listing_text.lines() {
        let line_parts:Vec<&str> = line.split_whitespace().collect();

        if line_parts.len() == 2 {
            // store week/name.ext format
            let file_path = format!("{}/{}", week, line_parts.get(1).unwrap().to_string());
            let hash = line_parts.get(0).unwrap().to_string();
            directory_listing.files.insert(file_path, hash);
        }
    }

    Ok(Json(directory_listing))
}

//async fn copy_file_to_s3(file_request:&FileRequest) -> Result<(), anyhow::Error> {


    // let client = reqwest::Client::new();

    // let response = client.get(get_cddis_file_path(&file_request.file_path))
    //     .bearer_auth(std::env::var("EARTHDATA_TOKEN").unwrap()).

    // let response = client.put(get_cddis_file_path(&file_request.file_path))
    //     .bearer_auth(std::env::var("EARTHDATA_TOKEN").unwrap()).await

    // let url = "http://localhost:9000".parse().unwrap();
    //     let key = "minioadmin";
    //     let secret = "minioadmin";
    //     let region = "minio";

    //     let bucket = Bucket::new(url, UrlStyle::Path, "test", region).unwrap();
    //     let credential = Credentials::new(key, secret);

    //     let mut action = GetObject::new(&bucket, Some(&credential), "img.jpg");
    //     action
    //         .query_mut()
    //         .insert("response-cache-control", "no-cache, no-store");
    //     let signed_url = action.sign(ONE_HOUR);

    // let response_bytes = response.bytes().await?;

    // let hash = Sha512::digest(&response_bytes);
    // let hex_hash = base16ct::lower::encode_string(&hash);

    // if !hex_hash.eq(&file_request.hash) {
    //     error!("File hash mismatch for {}", &file_request.file_path);
    //     return Err(anyhow!("Hash mismatch"));
    // }

//     Ok(())
// }

#[serde_as]
#[derive(Default, Serialize, Deserialize, Debug)]
struct DirectoryListing {
    #[serde_as(as = "Vec<(_, _)>")]
    files: BTreeMap<String, String>,
}

#[derive(serde::Serialize, serde::Deserialize,Debug, PartialEq, Clone)]
pub struct WeekList {
    weeks:Vec<u64>
}

impl WeekList {
    pub fn new(weeks:Vec<u64>) -> Self {
        Self {
            weeks
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize,Debug, PartialEq, Clone)]
pub struct FileRequest {
    file_path:String,
    hash:String
}

#[derive(serde::Serialize, serde::Deserialize,Debug, PartialEq, Clone)]
pub struct DownloadRequest {
    weeks:Option<WeekList>,
    archive:Option<bool>,
    parallelism:Option<u32>
}


#[restate_sdk::object]
pub trait CDDISArchiveFile {
    async fn download_file(file_request: Json<FileRequest>) -> Result<(), HandlerError>;
}

pub struct CDDISArchiveFileImpl;

impl CDDISArchiveFile for CDDISArchiveFileImpl {

    async fn download_file(&self, ctx: ObjectContext<'_>, file_request: Json<FileRequest>) -> Result<(), HandlerError> {

        let file_request = file_request.into_inner();

        info!("starting download: {}", file_request.file_path);

        let client = reqwest::Client::builder().pool_max_idle_per_host(0).build()?;

        let mut stream = client.get(get_cddis_file_path(&file_request.file_path))
             .bearer_auth(std::env::var("EARTHDATA_TOKEN").unwrap()).send().await?.bytes_stream();


        let upload_url = s3_put_object_url(file_request.file_path.as_str());

        let mut size_bytes =0;
        let mut bytes:Vec<u8> = Vec::new();
        while let Some(item) = stream.next().await {
            let chunk = item?;
            size_bytes += chunk.len();
            bytes.append(&mut chunk.to_vec());
        }

        let client = reqwest::Client::builder().pool_max_idle_per_host(0).build()?;
        //let body = json!(archived_listing);
        client.put(upload_url).body(bytes).send().await?;

        info!("finished download: {}, {} bytes", file_request.file_path, size_bytes);

        // let stream = reqwest::get("http://httpbin.org/ip")
        //     .await?
        //     .bytes_stream();

        //copy_file_to_s3(&file_request).await?;

        // let bucket_name = "cddis-archive";
        // let region = Region::Custom {
        //     region: "vultr-ewr1-1".to_owned(),
        //     endpoint: "https://ewr1.vultrobjects.com".to_owned(),
        // };

        // let credentials = Credentials::from_env().unwrap();

        // let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
        // let s3_result = bucket.put_object(&file_request.file_path, &response_bytes).await;

        // if s3_result.is_err() {
        //     error!("File upload failure {}", &file_request.file_path);
        //     return Err(HandlerError::from(TerminalError::new("File upload failure")));
        // }
        // ctx.set("last_update", Epoch::now()?.to_gpst_seconds());
        // info!("finished download: {}", file_request.file_path);

        Ok(())
    }
}

#[restate_sdk::object]
pub trait CDDISArchiveWeek {
    async fn download_week() -> Result<(), HandlerError>;
}

pub struct CDDISArchiveWeekImpl;

impl CDDISArchiveWeek for CDDISArchiveWeekImpl {

    async fn download_week(&self, ctx: ObjectContext<'_>) -> Result<(), HandlerError> {

        let week:u64 = ctx.key().parse::<u64>()?;

        info!("start week {}", week );

        let mut archived_listing = ctx.run(||get_archived_directory_listing(week)).await?.into_inner();
        let current_listing = ctx.run(||get_current_directory_listing(week)).await?.into_inner();


        let mut file_archive_count = 0;
        for (file_path, hash) in current_listing.files.iter() {

            if !archived_listing.files.contains_key(file_path) ||
                hash != archived_listing.files.get(file_path).unwrap()   {

                let file_request = FileRequest {file_path:file_path.clone(), hash:hash.clone()};
                let file_request_response = ctx.object_client::<CDDISArchiveFileClient>(file_path).download_file(Json(file_request)).call().await;

                if file_request_response.is_ok() {
                    archived_listing.files.insert(file_path.clone(), hash.clone());
                    file_archive_count += 1;
                }
            }
        }

        ctx.set("total_files", archived_listing.files.len() as u64);
        ctx.set("last_update", Epoch::now()?.to_gpst_seconds());
        info!("finished week {}: {}/{} files archived, {} new/changed", week, archived_listing.files.len(), current_listing.files.len(), file_archive_count);

        put_archived_directory_listing(week, &archived_listing).await?;

        Ok(())
    }
}


#[restate_sdk::workflow]
pub trait CDDISArchiveWorkflow {
    async fn run(directory: Json<DownloadRequest>) -> Result<(), HandlerError>;

    async fn download_weeks(weeks: Json<WeekList>) -> Result<(), HandlerError>;
}

pub struct CDDISArchiveWorkflowImpl;

impl CDDISArchiveWorkflow for CDDISArchiveWorkflowImpl {

    async fn run(&self, mut ctx: WorkflowContext<'_>, download_request:Json<DownloadRequest>) -> Result<(), HandlerError> {

        let last_update_started = ctx.run(||current_gpst_seconds()).await.unwrap();
        let current_week = ctx.run(||current_gpst_week()).await.unwrap();
        let download_request = download_request.into_inner();


        let first_week;
        if download_request.archive.is_some_and(|a|a==true) {
            first_week = MIN_GPST_WEEKS;
        }
        else {
            first_week = current_week - WEEK_LOOKBACK_PERIOD;
        }

        let weeks:Vec<u64> = (first_week..current_week).collect();

        let parallelism:usize;
        if download_request.parallelism.is_some_and(|p| p > 0) {
            parallelism = download_request.parallelism.unwrap() as usize;
        }
        else {
            parallelism = 1;
        }

        let week_chunks:Vec<Vec<u64>> = weeks.chunks(weeks.len() / parallelism).map(|chunk| chunk.to_vec()).collect();

        let mut job_status_list = Vec::new();
        for parallel_job in 0..parallelism {

            let job_weeks = week_chunks.get(parallel_job).unwrap().clone();

            info!("starting job {} with weeks: {}", parallel_job,
                job_weeks.iter()
                .map(|x|x.to_string()).collect::<Vec<String>>().join(","));

            let download_uuid = ctx.rand_uuid();

            let promise = ctx.workflow_client::<CDDISArchiveWorkflowClient>(download_uuid)
                .download_weeks(Json(WeekList::new(job_weeks))).call();

            job_status_list.push(promise);

        }

        // rust sdk only supports sequential awaits on promises
        for promise in job_status_list {
            let _ = promise.await; // allow failures but need to log/re-try out of workflow?
        }

        let last_update_completed = ctx.run(||current_gpst_seconds()).await.unwrap();
        let update_duration = last_update_completed - last_update_started;

        info!("update took {} seconds", update_duration);
        let mut seconds_until_next_update = UPDATE_FREQUENCY - update_duration;
        if seconds_until_next_update < 0.0 {
            seconds_until_next_update = 0.0;
        }
        info!("next update in {} seconds", seconds_until_next_update);

        let next_job_id = ctx.rand_uuid();
        ctx.workflow_client::
            <CDDISArchiveWorkflowClient>(next_job_id)
            .run(Json(download_request))
            .send_with_delay(Duration::from_secs(seconds_until_next_update as u64));

        Ok(())
    }

    async fn download_weeks(&self, ctx: WorkflowContext<'_>, weeks: Json<WeekList>) -> Result<(), HandlerError> {

        let weeks = weeks.into_inner();

        for week in weeks.weeks {
            ctx.object_client::<CDDISArchiveWeekClient>(week.to_string()).download_week().call().await?;
        }

        Ok(())
    }

}
