use std::{collections::BTreeMap, fmt::Debug, time::Duration};
use ground_control::gpst::{current_gpst_seconds, current_gpst_week};
use restate_sdk::{prelude::*,serde::Json,errors::{HandlerError, TerminalError}};
use serde_with::serde_as;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::{cddis::{get_cddis_archive_file_path, get_cddis_directory_listing, get_cddis_file_path}, queue::{CDDISFileQueueClient, CDDISFileQueueData, CDDISFileRequest}, r2::r2_get_archived_directory_listing};

const MIN_GPST_WEEKS: u32 = 2238; // CDDIS format changed for products prior to week 2238


// Data structure for comparing CDDIS file hash indexes with the current R2 object store data set
// This object is seralized via JSON serde and stored in the R2 directory
//
#[serde_as]
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct DirectoryListing {
    pub week:Option<u32>,
    #[serde_as(as = "Vec<(_, _)>")]
    pub files: BTreeMap<String, String>,
}

impl DirectoryListing {

    pub fn new(week:u32) -> Self {
        DirectoryListing {week:Some(week), files:BTreeMap::new()}
    }

    pub fn contains(&self, item:&DirectoryListingItem) -> DirectoryListingItemStatus {

        if self.files.contains_key(&item.filename) {
            let hash = self.files.get(&item.filename).unwrap();

            if hash.eq(&item.hash) {
                DirectoryListingItemStatus::Found
            }
            else {
                DirectoryListingItemStatus::HashChanged
            }
        }
        else {
            DirectoryListingItemStatus::NotFound
        }
    }

    pub fn directory_items(self) -> impl Iterator<Item = DirectoryListingItem> {
        self.files.iter().map(|f|
            DirectoryListingItem {week:self.week.unwrap(), filename: f.0.to_string(), hash:f.1.to_string()})
        .collect::<Vec<DirectoryListingItem>>()
        .into_iter()
    }

    pub fn file_count(&self) -> u32 {
        self.files.len() as u32
    }

    pub fn add_file(&mut self, filename_str: String, hash: String) {
        let filename_parts = filename_str.split("/");
        let filename = filename_parts.last().unwrap().to_string();
        self.files.insert(filename, hash);
    }

}

#[derive(serde::Serialize, serde::Deserialize,Debug, PartialEq, Clone)]
pub enum DirectoryListingItemStatus {
    Found,
    NotFound,
    HashChanged
}

#[derive(serde::Serialize, serde::Deserialize,Debug, PartialEq, Clone)]
pub struct DirectoryListingItem {
    pub week:u32,
    pub filename:String,
    pub hash:String
}

impl DirectoryListingItem {

    // build a file request object that's inserted into the archival queue
    pub fn get_file_request(&self, request_queue:&CDDISFileQueueData, process_files:bool) -> CDDISFileRequest {

        CDDISFileRequest {
            request_queue: request_queue.clone(),
            week: self.week,
            path:get_cddis_file_path(self.week, &self.filename),
            hash: self.hash.clone(),
            archive_path: get_cddis_archive_file_path(self.week, &self.filename),
            process_files
        }
    }

}

// Configuration objects for initiating an archiver workflow run

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Clone)]
pub enum CDDISArchiveRequestWeekRange {
    AllWeeks,
    RecentWeeks(u32),
    WeeksList(Vec<u32>)
}


#[derive(serde::Serialize, serde::Deserialize,Debug, PartialEq, Clone)]
pub struct CDDISArchiveRequest {
    pub request_id:String,
    pub weeks:Option<CDDISArchiveRequestWeekRange>,
    pub parallelism:Option<u32>,
    pub process_files:Option<bool>,
    pub recurring:Option<u64>, // seconds
}

impl CDDISArchiveRequest {

    fn get_key(&self) -> String {
        self.request_id.to_string()
    }

    fn get_next_request(&self, request_id:String) -> Self {
        let mut next_request = self.clone();
        next_request.request_id = request_id; // builds the next workflow request with same settings + new id
        next_request
    }

    fn parallelism_with_default(&self) -> u32 {
        self.parallelism.unwrap_or(1)
    }

    fn process_files_with_default(&self) -> bool {
        self.process_files.unwrap_or(false)
    }

    // builds an iterable liset of weeks to archive baed on selected CDDISArchiveRequestWeekRange
    fn week_requests(&self, current_week:u32) -> impl Iterator<Item = CDDISArchiveWeekRequest>  {

        let archive_weeks: Vec<u32> = match self.weeks.as_ref().unwrap() {
            CDDISArchiveRequestWeekRange::AllWeeks => (MIN_GPST_WEEKS..=current_week).collect(),
            CDDISArchiveRequestWeekRange::RecentWeeks(look_back_period) => ((current_week-look_back_period)..=current_week).collect(),
            CDDISArchiveRequestWeekRange::WeeksList(weeks) => weeks.clone(),
        };

        let week_requests:Vec<CDDISArchiveWeekRequest> = archive_weeks.iter().rev().map(|week| CDDISArchiveWeekRequest {
            request_id: self.request_id.clone(),
            week: *week,
            parallelism: self.parallelism_with_default(),
            process_files: self.process_files_with_default()
        }).collect();

        week_requests.into_iter()
    }
}

// Week archival request, inserted into processing queue
#[derive(serde::Serialize, serde::Deserialize,Debug, PartialEq, Clone)]
pub struct CDDISArchiveWeekRequest {
    request_id:String,
    week:u32,
    parallelism:u32,
    process_files:bool,
}

impl CDDISArchiveWeekRequest {
    fn get_key(&self) -> String {
        format!("{}_{}", self.request_id, self.week)
    }
}


#[derive(serde::Serialize, serde::Deserialize,Debug, PartialEq, Clone)]
pub struct CDDISArchiveWorkflowStatus {
    request:CDDISArchiveRequest,
    weeks_completed:u32,
    weeks_failed:Vec<u32>,
    time_started:f64,
    time_completed:Option<f64>,
    last_udpate:Option<f64>
}

#[derive(serde::Serialize, serde::Deserialize,Debug, PartialEq, Clone)]
pub struct CDDISWeekWorkflowStatus {
    week_request:CDDISArchiveWeekRequest,
    original_files:u32,
    archived_files:u32,
    new_files:u32,
    changed_files:u32,
    queues:Vec<CDDISFileQueueData>,
    time_started:f64,
    time_completed:Option<f64>,
    last_udpate:Option<f64>
}

/// Restate handlers

#[restate_sdk::workflow]
pub trait CDDISWeekWorkflow {
    async fn run(archive_week_request:Json<CDDISArchiveWeekRequest>) -> Result<(), HandlerError>;

    #[shared]
    async fn get_status() -> Result<Json<CDDISWeekWorkflowStatus>, HandlerError>;
}

pub struct CDDISWeekWorkflowImpl;

impl CDDISWeekWorkflow for CDDISWeekWorkflowImpl {

    async fn run(&self, ctx:WorkflowContext<'_>, archive_week_request:Json<CDDISArchiveWeekRequest>) -> Result<(), HandlerError> {

        let archive_week_request = archive_week_request.into_inner();
        let week = archive_week_request.week;
        info!("starting week: {}", week);

        let last_update_started = ctx.run(current_gpst_seconds).await?;

        let process_files = archive_week_request.process_files;
        let cdds_directory_listing = ctx.run(||get_cddis_directory_listing(week)).await?.into_inner();
        let r2_directory_listing = ctx.run(||r2_get_archived_directory_listing(week)).await?.into_inner();
        let original_files = cdds_directory_listing.file_count();
        let archived_files = r2_directory_listing.file_count();

        let mut new_files = 0;
        let mut changed_files = 0;

        let mut pending_files:Vec<DirectoryListingItem> = Vec::new();

        for original in cdds_directory_listing.directory_items() {
            match r2_directory_listing.contains(&original) {
                DirectoryListingItemStatus::Found => {
                    // skip existing file
                }
                DirectoryListingItemStatus::NotFound => {
                    pending_files.push(original);
                    new_files += 1;
                }
                DirectoryListingItemStatus::HashChanged => {
                    pending_files.push(original);
                    changed_files += 1;
                }
            }
        }

        let mut status = CDDISWeekWorkflowStatus {
            week_request:archive_week_request.clone(),
            original_files,
            archived_files,
            new_files,
            changed_files,
            queues:Vec::new(),
            time_started:last_update_started,
            time_completed:None,
            last_udpate:None
        };

        ctx.set("status", Json(status.clone()));

        if !pending_files.is_empty() {

            let chunk_size = if pending_files.len() > archive_week_request.parallelism as usize {
                pending_files.len() / (archive_week_request.parallelism as usize)
            } else {
                1
            };

            let queue_chunks:Vec<Vec<DirectoryListingItem>> = pending_files
                .chunks(chunk_size)
                .map(|chunk|chunk.to_vec())
                .collect();

            let mut queue_num = 1;

            for queue_chunk in queue_chunks {

                let request_queue = CDDISFileQueueData {
                    request_id:archive_week_request.request_id.clone(),
                    queue_num,
                };

                status.queues.push(request_queue.clone());
                ctx.set("status", Json(status.clone()));

                for file in queue_chunk {
                    let file_request = file.get_file_request(&request_queue, process_files);
                    ctx.object_client::<CDDISFileQueueClient>(request_queue.get_key()).archive_file(Json(file_request)).send();
                }

                queue_num += 1;
            }
        }

        Ok(())
    }

    async fn get_status(&self, ctx:SharedWorkflowContext<'_>) -> Result<Json<CDDISWeekWorkflowStatus>, HandlerError> {

        let status = ctx.get::<Json<CDDISWeekWorkflowStatus>>("status").await?;
        if let Some(status) = status {
            return Ok(status);
        }

        Err(TerminalError::new(format!("Status not found for request_id: {}", ctx.key())).into())
    }

}


#[restate_sdk::workflow]
pub trait CDDISArchiverWorkflow {
    async fn run(archive_request:Json<CDDISArchiveRequest>) -> Result<(), HandlerError>;

    #[shared]
    async fn get_status() -> Result<Json<CDDISArchiveWorkflowStatus>, HandlerError>;
}

pub struct CDDISArchiverWorkflowImpl;

impl CDDISArchiverWorkflow for CDDISArchiverWorkflowImpl {

    async fn run(&self, mut ctx: WorkflowContext<'_>, archive_request:Json<CDDISArchiveRequest>) -> Result<(), HandlerError> {

        let archive_request:CDDISArchiveRequest = archive_request.into_inner();

        // moved scheduling next action to top in case of failure during archival
        // waiting for durable scheduling/crontab in Restate
        if archive_request.recurring.is_some() {

            let recurring_delay = archive_request.recurring.unwrap();
            info!("queuing next archival workflow in {} seconds...", recurring_delay);
            let next_request_id = ctx.rand_uuid();
            let next_archive_request = archive_request.get_next_request(next_request_id.to_string());
            ctx.workflow_client::<CDDISArchiverWorkflowClient>(next_archive_request.get_key())
                .run(Json(next_archive_request))
                .send_after(Duration::from_secs(recurring_delay));

        }

        // using workflow for archiver intialization to prevent task duplication
        // each workflow init requires a unique archive task id, used as part of downstream keys
        // currently forcing ArchiveRequest id to match ctx key
        if archive_request.request_id != ctx.key() {
            return Err(TerminalError::new("ArchiveRequest id and ArchiverWorkflow id mismatch.").into());
        }

        let last_update_started = ctx.run(current_gpst_seconds).await?;
        let current_week = ctx.run(current_gpst_week).await?;

        let status = CDDISArchiveWorkflowStatus {request:archive_request.clone(),
            weeks_completed: 0,
            weeks_failed: Vec::new(),
            time_started: last_update_started,
            time_completed: None,
            last_udpate: None
        };

        ctx.set("status", Json(status));

        for week_request in archive_request.week_requests(current_week) {

            // tood handle failures in workflow status
            let result = ctx.workflow_client::<CDDISWeekWorkflowClient>(week_request.get_key())
                .run(Json(week_request.clone())).call().await;

            let mut status = ctx.get::<Json<CDDISArchiveWorkflowStatus>>("status").await?.unwrap().into_inner();

            if result.is_ok() {
                status.weeks_completed += 1;
            }
            else {
                status.weeks_failed.push(week_request.week);
            }

            ctx.set("status", Json(status));

        }

        Ok(())
    }

    async fn get_status(&self, ctx:SharedWorkflowContext<'_>) -> Result<Json<CDDISArchiveWorkflowStatus>, HandlerError> {

        let status = ctx.get::<Json<CDDISArchiveWorkflowStatus>>("status").await?;
        if let Some(status) = status {
            return Ok(status);
        }

        Err(TerminalError::new(format!("Status not found for request_id: {}", ctx.key())).into())
    }

}
