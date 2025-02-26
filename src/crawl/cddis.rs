use std::{collections::HashMap, fmt::Debug, time::Duration};
use bytes::Bytes;
use hifitime::Epoch;
use restate_sdk::prelude::*;
use s3::Region;
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_with::serde_as;
use sha2::{Sha512, Digest};
use tracing::{error, info};

use s3::bucket::Bucket;
use s3::creds::Credentials;


const MIN_GPST_WEEKS: u64 = 2238; // CDDIS format changed for products prior to week 2238
const WEEK_LOOKBACK_PERIOD: u64 = 12; // Look back 12 weeks for updated productss
const UPDATE_FREQUENCY:f64 = 30.0; //60 * 10; // update every ten minutes
const CDDIS_PATH:&str = "https://cddis.nasa.gov/archive/gnss/products";


// async so it can be wraped in a restate run closure
pub async fn current_time_seconds() -> Result<f64, HandlerError> {
    Ok(current_time().await?.to_gpst_seconds())
}

// async so it can be wraped in a restate run closure
pub async fn current_time() -> Result<Epoch, HandlerError> {
    Ok(Epoch::now()?)
}

// async so it can be wraped in a restate run closure
pub async fn current_gpst_week() -> Result<u64, HandlerError> {
   Ok(gpst_week(&current_time().await?))
}

pub fn gpst_week(epoch:&Epoch) -> u64 {
    let gpst_week = (epoch.to_gpst_days() / 7.0).floor() as u64;
    gpst_week
}

fn get_cddis_week_path(week:u64) -> String {
    format!("{}/{}", CDDIS_PATH, week)
}

async fn get_cddis_sha2_directory_listing(week:u64) -> Result<DirectoryListing, anyhow::Error> {

    let client = reqwest::Client::new();
    let response = client.get(format!("{}/SHA512SUMS", get_cddis_week_path(week)))
        .bearer_auth(std::env::var("EARTHDATA_TOKEN").unwrap())
        .send()
        .await?;

    let directory_listing_text = response.text().await?;

    let mut directory_listing = DirectoryListing::default();

    for line in directory_listing_text.lines() {
        let line_parts:Vec<&str> = line.split_whitespace().collect();

        if line_parts.len() == 2 {
            let file = line_parts.get(1).unwrap().to_string();
            let hash = line_parts.get(0).unwrap().to_string();
            directory_listing.files.insert(file, hash);
        }
    }

    Ok(directory_listing)
}

async fn get_cddis_file_with_hash(week:u64, file:&str) -> Result<(Bytes, String), anyhow::Error>{

    let client = reqwest::Client::new();

    let response = client.get(format!("{}/{}", get_cddis_week_path(week), file))
        .bearer_auth(std::env::var("EARTHDATA_TOKEN").unwrap())
        .send()
        .await?;

    let resposne_bytes = response.bytes().await?;
    let hash = Sha512::digest(&resposne_bytes);
    let hex_hash = base16ct::lower::encode_string(&hash);

    Ok((resposne_bytes, hex_hash))
}


#[serde_as]
#[derive(Default, Serialize, Deserialize, Debug)]
struct DirectoryListing {
    #[serde_as(as = "Vec<(_, _)>")]
    files: HashMap<String, String>,
}

#[derive(serde::Serialize, serde::Deserialize,Debug, PartialEq, Clone)]
struct WeekList {
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
struct DownloadRequest {
    cddis_path:String,
    archive:Option<bool>,
    parallelism:Option<u32>
}


#[restate_sdk::workflow]
pub trait CDDISWeekDownloadWorkflow {
    async fn run(directory: Json<WeekList>) -> Result<bool, anyhow::Error>;
}

pub struct CDDISWeekDownloadWorkflowImpl;

impl CDDISWeekDownloadWorkflow for CDDISWeekDownloadWorkflowImpl {

    async fn run(&self, ctx: WorkflowContext<'_>,week_list:Json<WeekList>) -> Result<bool, anyhow::Error> {

        let week_list = week_list.into_inner();


        let bucket_name = "cddis-archive";
        let region = Region::Custom {
            region: "vultr-ewr1-1".to_owned(),
            endpoint: "https://ewr1.vultrobjects.com".to_owned(),
        };

        let credentials = Credentials::from_env().unwrap();
        info!("{:?}", credentials);

        let bucket = Bucket::new(bucket_name, region, credentials).unwrap();


        let key = ctx.key();
        for week in week_list.weeks {

            info!("starrting downloads for week {} ({})", week, key);

            let listing_path = format!("{}/sha512.json", week);
            let listing_response = bucket.get_object(&listing_path).await;

            let mut archived_listing:DirectoryListing;
            if listing_response.is_ok() {
                archived_listing = serde_json::from_str(listing_response.unwrap().as_str().unwrap())?;

            }
            else {
                archived_listing = DirectoryListing::default();
            }

            let current_listing = get_cddis_sha2_directory_listing(week).await?;

            let mut i = 0;
            let total_files = current_listing.files.keys().len();
            for file in current_listing.files.keys() {

                if !archived_listing.files.contains_key(file) ||
                    current_listing.files.get(file) != archived_listing.files.get(file)   {

                    let (data, hash) = get_cddis_file_with_hash(week, file).await?;

                    if current_listing.files.get(file).unwrap().eq(&hash)  {
                        info!("{}/{} downloaded file {} with matching hash", i, total_files, file);
                        archived_listing.files.insert(file.clone(), hash);
                        let path = format!("{}/{}", week, file);
                        let s3_result = bucket.put_object(&path, &data).await;
                        if s3_result.is_ok() {
                            info!("uploaded file {}", path);
                        }
                        else {
                            error!("file upload error: {}", s3_result.err().unwrap().to_string());
                        }
                    }
                    else {
                        error!("downloaded file {} with hash{}\nCDDIS hash is {}", file, hash, current_listing.files.get(file).unwrap())
                    }
                    i += 1;
                }

                if i > 1 {
                    break;
                }
            }

            bucket.put_object(&listing_path, json!(archived_listing).to_string().as_bytes()).await?;

            info!("completeded downloads for week {} ({})", week, key);
        }
        Ok(true)
    }
}

#[restate_sdk::workflow]
pub trait CDDISProductDownloadWorkflow {
    async fn run(directory: Json<DownloadRequest>) -> Result<bool, anyhow::Error>;

    // #[shared]
    // async fn status(secret: String) -> Result<(), anyhow::Error>;
}

pub struct CDDISProductDownloadWorkflowImpl;

impl CDDISProductDownloadWorkflow for CDDISProductDownloadWorkflowImpl {

    async fn run(&self, mut ctx: WorkflowContext<'_>, download_request:Json<DownloadRequest>) -> Result<bool, anyhow::Error> {

        let last_update_started = ctx.run(||current_time_seconds()).await.unwrap();
        let current_week = ctx.run(||current_gpst_week()).await.unwrap();
        let download_request = download_request.into_inner();
        //let mut week_list:WeekList = WeekList::default();

        // need to handle archive initialization vs update

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
            // let (id, promise) = ctx.awakeable::<String>();
            // job_status_list.push(promise);
            let promise = ctx.workflow_client::<CDDISWeekDownloadWorkflowClient>(download_uuid)
                .run(Json(WeekList::new(job_weeks))).call();

            job_status_list.push(promise);

        }

        // rust sdk only supports sequential awaits on promises
        for promise in job_status_list {
            let _ = promise.await;
        }
        //let _ = futures::future::try_join_all(job_status_list).await;

        let last_update_completed = ctx.run(||current_time_seconds()).await.unwrap();
        let update_duration = last_update_completed - last_update_started;

        info!("update took {} seconds", update_duration);
        let mut seconds_until_next_update = UPDATE_FREQUENCY - update_duration;
        if seconds_until_next_update < 0.0 {
            seconds_until_next_update = 0.0;
        }
        info!("next update in {} seconds", seconds_until_next_update);

        let next_job_id = ctx.rand_uuid();
        ctx.workflow_client::
            <CDDISProductDownloadWorkflowClient>(next_job_id)
            .run(Json(download_request))
            .send_with_delay(Duration::from_secs(seconds_until_next_update as u64));

        Ok(true)
    }

    // fn status(&self, _ctx:SharedWorkflowContext, secret:String) -> Result<(), anyhow::Error>{

    //     Ok(())
    // }

}
