use restate_sdk::prelude::*;
use std::collections::BTreeMap;
use std::{fmt::Debug, time::Duration};
use anyhow::anyhow;
use hifitime::Epoch;
use regex::Regex;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_with::serde_as;
use tracing::{error, info, warn};

use crate::data::s3_object_head;

use super::sources::{OrbitSourceClient, RinexSource};
use super::{build_reqwest_client, gpst_week, s3_get_object_url, s3_put_object_url};

const MIN_GPST_WEEKS: u64 = 2238; // CDDIS format changed for products prior to week 2238
const WEEK_LOOKBACK_PERIOD: u64 = 12; // Look back 12 weeks for updated productss
const UPDATE_FREQUENCY:f64 = 60.0 * 10.0; // update every ten minutes
const CDDIS_PATH:&str = "https://cddis.nasa.gov/archive/gnss/products";

pub fn cddis_path_parser(path:&str) -> Option<regex::Captures<'_>> {
    let re = Regex::new(r"^(?<WEEK>\d{4})\/(?<AC>.{3})0(?<PROJ>.{3})(?<TYP>.{3})_(?<TIME>[0-9]{11})_(?<PER>.*)_(?<SMP>.*)_(?<CNT>.*)\.(?<FMT>.*)\.gz$").unwrap();
    return re.captures(path);
}

fn get_cddis_week_path(week:u64) -> String {
    format!("{}/{}", CDDIS_PATH, week)
}

pub fn get_cddis_file_path(file_path:&str) -> String {
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

async fn get_archived_directory_listing(week:u64) -> Result<DirectoryListing, anyhow::Error> {

    let listing_path = format!("{}/sha512.json", week);

    let listing_url = s3_get_object_url(listing_path.as_str());

    let client = build_reqwest_client()?;
    let response = client.get(listing_url).send().await?;

    let archived_listing:DirectoryListing;
    let status_code = response.status();
    if status_code.is_success() {
        archived_listing = serde_json::from_str(response.text().await?.as_str())?;
        return Ok(archived_listing);
    }
    else if status_code == StatusCode::NOT_FOUND {
        return Ok(DirectoryListing::default());
    }

    return Err( anyhow!("Unable to load archived directory listing."));
}
async fn put_archived_directory_listing(week:u64, archived_listing:&DirectoryListing) -> Result<(), anyhow::Error> {

    let listing_path = format!("{}/sha512.json", week);

    let listing_url = s3_put_object_url(listing_path.as_str());

    let client = build_reqwest_client()?;
    let body = json!(archived_listing);
    let response = client.put(listing_url).body(body.to_string()).send().await?;

    if response.status().is_success() {
         return Ok(());
    }

    Err( anyhow!("Unable to put archived directory listing."))
}

async fn get_current_directory_listing(week:u64) -> Result<DirectoryListing, anyhow::Error> {

    let client = build_reqwest_client()?;
    let response = client.get(format!("{}/SHA512SUMS", get_cddis_week_path(week)))
        .bearer_auth(std::env::var("EARTHDATA_TOKEN").unwrap())
        .send()
        .await?;

    if response.status().is_success() {
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

        return Ok(directory_listing);
    }

    Err( anyhow!("Unable to get current directory listing."))
}

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

#[derive(serde::Serialize, serde::Deserialize,Debug, PartialEq, Clone)]
pub struct FileRequest {
    week:u64,
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
pub trait CDDISArchiveWeek {
    async fn download_week() -> Result<(), HandlerError>;
    async fn download_file(file_request:Json<FileRequest>) -> Result<(), HandlerError>;
}

pub struct CDDISArchiveWeekImpl;

impl CDDISArchiveWeek for CDDISArchiveWeekImpl {

    async fn download_week(&self, ctx: ObjectContext<'_>) -> Result<(), HandlerError> {

        let week:u64 = ctx.key().parse::<u64>()?;

        info!("start week {}", week );

        let archived_listing = get_archived_directory_listing(week).await?;
        let current_listing = get_current_directory_listing(week).await?;

        for (file_path, hash) in current_listing.files.iter() {

            // let head_url = s3_object_head(file_path);
            // let client = build_reqwest_client().unwrap();
            // let result = client.head(head_url).send().await.unwrap();
            // let file_archived;
            // if result.status().is_success() {
            //     file_archived = true;
            //     info!("file archived: {}", file_path);
            // }
            // else {
            //     file_archived = false;
            //     warn!("file not archived: {}", file_path);
            // }


            if !archived_listing.files.contains_key(file_path) ||
                hash != archived_listing.files.get(file_path).unwrap()
             {

                let file_request = FileRequest {file_path:file_path.clone(), hash:hash.clone(), week};
                ctx.object_client::<CDDISArchiveWeekClient>(week.to_string()).download_file(Json(file_request)).send();

            }
        }

        Ok(())
    }

    async fn download_file(&self, ctx: ObjectContext<'_>, file_request: Json<FileRequest>) -> Result<(), HandlerError> {

        let file_request = file_request.into_inner();

        info!("starting download: {}", file_request.file_path);

        let client = build_reqwest_client()?;
        let response = client.get(get_cddis_file_path(&file_request.file_path))
             .bearer_auth(std::env::var("EARTHDATA_TOKEN").unwrap()).send().await?;//.bytes().await?;

        if response.status().is_success() {

            let bytes = response.bytes().await?;

            let upload_url = s3_put_object_url(file_request.file_path.as_str());
            let client = build_reqwest_client()?;
            let response = client.put(upload_url).body(bytes).send().await?;
            if ! response.status().is_success() {
                error!("failed to upload file: {:?}", response.error_for_status_ref())
            }

            info!("uploaded file: {}", file_request.file_path);

            let mut archived_listing = get_archived_directory_listing(file_request.week).await?;
            archived_listing.files.insert(file_request.file_path.to_string(), file_request.hash);
            put_archived_directory_listing(file_request.week, &archived_listing).await?;

            // let path_parts = cddis_path_parser(file_request.file_path.as_str());

            // if path_parts.is_some() {
            //     let path_parts  = path_parts.unwrap();
            //     let rinex_file = RinexSource {
            //         path:file_request.file_path.clone(),
            //         ac:path_parts["AC"].to_string(),
            //         solution:path_parts["TYP"].to_string(),
            //         solution_time:path_parts["TIME"].to_string(),
            //         content_type:path_parts["CNT"].to_string(),
            //         source:"CDDIS".to_string(),
            //         collected_at: Epoch::now()?.to_gpst_seconds(),
            //         sv_coverage: None
            //     };
            //     if path_parts["TYP"].eq("ULT") && path_parts["CNT"].eq("ORB") && path_parts["FMT"].eq("SP3") {
            //         ctx.object_client::<OrbitSourceClient>(rinex_file.get_source_key())
            //             .process_sp3(Json(rinex_file)).send();
            //     }
            // }

            return Ok(());
        }
        else if response.status() == StatusCode::NOT_FOUND {
            return Err(TerminalError::new(format!("File not found {}", file_request.file_path)).into());
        }

        Err(HandlerError::from(anyhow!("Unable to download file from CDDIS archive: {}", file_request.file_path)))
    }
}

#[restate_sdk::workflow]
pub trait CDDISArchiveWorkflow {
    async fn run(directory: Json<DownloadRequest>) -> Result<(), HandlerError>;
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

        let weeks:Vec<u64> = (first_week..=current_week).rev().collect();

        for week in weeks {
            ctx.object_client::<CDDISArchiveWeekClient>(week.to_string()).download_week().send();
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

}
