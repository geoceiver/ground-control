use std::{collections::BTreeMap, fmt::Debug};
use ground_control::gpst::{current_gpst_seconds, current_gpst_week};
use restate_sdk::{prelude::*,serde::Json,errors::{HandlerError, TerminalError}};
use serde_with::serde_as;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::{cddis::{get_archive_file_path, get_cddis_directory_listing, get_cddis_file_path}, queue::{ArchiverFileQueue, ArchiverFileQueueClient, FileQueueData, FileRequest}, r2::r2_get_archived_directory_listing};

const MIN_GPST_WEEKS: u32 = 2238; // CDDIS format changed for products prior to week 2238
const DEFAULT_WEEK_LOOKBACK_PERIOD: u32 = 12; // defualt


#[serde_as]
#[derive(Default, Serialize, Deserialize, Debug)]
pub struct DirectoryListing {
    #[serde_as(as = "Vec<(_, _)>")]
    files: BTreeMap<String, String>,
}

impl DirectoryListing {
    pub fn new() -> Self {
        DirectoryListing {
            files: BTreeMap::new(),
        }
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

    pub fn into_iter(&self) -> impl Iterator<Item = DirectoryListingItem> {
        self.files.iter().map(|f|
            DirectoryListingItem {filename: f.0.to_string(), hash:f.1.to_string()})
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
    pub filename:String,
    pub hash:String
}

impl DirectoryListingItem {

    pub fn get_file_request(&self, request_queue:&FileQueueData, process_files:bool) -> FileRequest {

        FileRequest {
            request_queue: request_queue.clone(),
            path:get_cddis_file_path(request_queue.week, &self.filename),
            hash: self.hash.clone(),
            archive_path: get_archive_file_path(request_queue.week, &self.filename),
            process_files
        }
    }

}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Clone)]
pub enum ArchiveWeeks {
    AllWeeks,
    RecentWeeks(u32),
    WeeksList(Vec<u32>)
}

#[derive(serde::Serialize, serde::Deserialize,Debug, PartialEq, Clone)]
pub struct ArchiveRequest {
    pub request_id:String,
    pub weeks:Option<ArchiveWeeks>,
    pub parallelism:Option<u32>,
    pub process_files:Option<bool>,
    pub recurring:Option<bool>,
}

impl ArchiveRequest {

    fn get_key(&self) -> String {
        format!("{}", self.request_id)
    }

    fn weeks_with_default(&self) -> ArchiveWeeks {
        let weeks = ArchiveWeeks::RecentWeeks(DEFAULT_WEEK_LOOKBACK_PERIOD);
        self.weeks.as_ref().unwrap_or(&weeks).clone()
    }

    fn parallelism_with_default(&self) -> u32 {
        self.parallelism.unwrap_or(1)
    }

    fn process_files_with_default(&self) -> bool {
        self.process_files.unwrap_or(false)
    }

    fn recurring_with_default(&self) -> bool {
        self.recurring.unwrap_or(false)
    }

    fn num_weeks(&self, current_week:u32) -> u32 {
        self.week_requests(current_week).count() as u32
    }

    fn week_requests(&self, current_week:u32) -> impl Iterator<Item = ArchiveWeekRequest>  {

        let archive_weeks:Vec<u32>;
        match self.weeks.as_ref().unwrap() {
            ArchiveWeeks::AllWeeks => {
                archive_weeks = (MIN_GPST_WEEKS..current_week).collect()
            },
            ArchiveWeeks::RecentWeeks(look_back_period) => {
                archive_weeks = ((current_week-look_back_period)..current_week).collect()
            },
            ArchiveWeeks::WeeksList(weeks) => {
                archive_weeks = weeks.clone();
            },
        }

        let week_requests:Vec<ArchiveWeekRequest> = archive_weeks.iter().rev().map(|week| ArchiveWeekRequest {
            request_id: self.request_id.clone(),
            week: week.clone(),
            parallelism: self.parallelism_with_default().clone(),
            process_files: self.process_files_with_default().clone()
        }).collect();

        return week_requests.into_iter();
    }
}

#[derive(serde::Serialize, serde::Deserialize,Debug, PartialEq, Clone)]
pub struct ArchiveWeekRequest {
    request_id:String,
    week:u32,
    parallelism:u32,
    process_files:bool,
}

impl ArchiveWeekRequest {

    fn get_key(&self) -> String {
        format!("{}_{}", self.request_id, self.week)
    }

}

// Status objects

#[derive(serde::Serialize, serde::Deserialize,Debug, PartialEq, Clone)]
pub struct ArchiveWorkflowStatus {
    request:ArchiveRequest,
    weeks_completed:u32,
    weeks_failed:Vec<u32>,
    time_started:f64,
    time_completed:Option<f64>,
    last_udpate:Option<f64>
}

#[derive(serde::Serialize, serde::Deserialize,Debug, PartialEq, Clone)]
pub struct ArchiveWeekWorkflowStatus {
    week_request:ArchiveWeekRequest,
    original_files:u32,
    archived_files:u32,
    new_files:u32,
    changed_files:u32,
    queues:Vec<FileQueueData>,
    time_started:f64,
    time_completed:Option<f64>,
    last_udpate:Option<f64>
}

/// Restate handlers

#[restate_sdk::workflow]
pub trait ArchiveWeekWorkflow {
    async fn run(archive_week_request:Json<ArchiveWeekRequest>) -> Result<(), HandlerError>;

    #[shared]
    async fn get_status() -> Result<Json<ArchiveWeekWorkflowStatus>, HandlerError>;
}

pub struct ArchiveWeekWorkflowImpl;

impl ArchiveWeekWorkflow for ArchiveWeekWorkflowImpl {

    async fn run(&self, ctx:WorkflowContext<'_>, archive_week_request:Json<ArchiveWeekRequest>) -> Result<(), HandlerError> {

        let archive_week_request = archive_week_request.into_inner();
        let last_update_started = ctx.run(||current_gpst_seconds()).await?;

        let request_id = archive_week_request.request_id.clone();
        let week = archive_week_request.week;
        let process_files = archive_week_request.process_files;

        let cdds_directory_listing = ctx.run(||get_cddis_directory_listing(week)).await?.into_inner();
        let r2_directory_listing = ctx.run(||r2_get_archived_directory_listing(week)).await?.into_inner();

        let original_files = cdds_directory_listing.file_count();
        let archived_files = r2_directory_listing.file_count();

        let mut new_files = 0;
        let mut changed_files = 0;

        let mut pending_files:Vec<DirectoryListingItem> = Vec::new();

        for original in cdds_directory_listing.into_iter() {
            match r2_directory_listing.contains(&original) {
                DirectoryListingItemStatus::Found => {
                    // skip file
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

        let mut status = ArchiveWeekWorkflowStatus {
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

        let queue_chunks:Vec<Vec<DirectoryListingItem>> = pending_files
            .chunks(pending_files.len() / (archive_week_request.parallelism as usize))
            .map(|chunk|chunk.to_vec())
            .collect();

        let mut queue_num = 1;
        let mut queue_promises:Vec<_> = Vec::new();

        for queue_chunk in queue_chunks {

            let (awakeable_id, promise) = ctx.awakeable::<String>();
            queue_promises.push(promise);

            let request_queue = FileQueueData {
                request_id:request_id.clone(),
                week,
                queue_num,
                enqueued_files:queue_chunk.len() as u32,
                awakeable_id
            };

            status.queues.push(request_queue.clone());
            ctx.set("status", Json(status.clone()));

            info!("queue {}: {}", request_queue.get_key(), queue_chunk.len());

            for file in queue_chunk {
                let file_request = file.get_file_request(&request_queue, process_files);
                ctx.object_client::<ArchiverFileQueueClient>(request_queue.get_key()).archive_file(Json(file_request)).send();
            }

            queue_num += 1;
        }

        // need to sequentially await queues because Rust SDK doesn't implement join_all
        for queue_promise in queue_promises {
            queue_promise.await?;
        }

        Ok(())
    }

    async fn get_status(&self, ctx:SharedWorkflowContext<'_>) -> Result<Json<ArchiveWeekWorkflowStatus>, HandlerError> {

        let status = ctx.get::<Json<ArchiveWeekWorkflowStatus>>("status").await?;
        if status.is_some() {
            return Ok(status.unwrap());
        }

        Err(TerminalError::new(format!("Status not found for request_id: {}", ctx.key())).into())
    }

}


#[restate_sdk::workflow]
pub trait ArchiverWorkflow {
    async fn run(archive_request:Json<ArchiveRequest>) -> Result<(), HandlerError>;

    #[shared]
    async fn get_status() -> Result<Json<ArchiveWorkflowStatus>, HandlerError>;
}

pub struct ArchiverWorkflowImpl;

impl ArchiverWorkflow for ArchiverWorkflowImpl {

    async fn run(&self, ctx: WorkflowContext<'_>, archive_request:Json<ArchiveRequest>) -> Result<(), HandlerError> {

        let archive_request:ArchiveRequest = archive_request.into_inner();

        // using workflow for archiver intialization to prevent task duplication
        // each workflow init requires a unique archive task id, used as part of downstream keys
        // currently forcing ArchiveRequest id to match ctx key
        if archive_request.request_id != ctx.key() {
            return Err(TerminalError::new("ArchiveRequest id and ArchiverWorkflow id mismatch.").into());
        }

        let last_update_started = ctx.run(||current_gpst_seconds()).await?;
        let current_week = ctx.run(||current_gpst_week()).await?;

        let status = ArchiveWorkflowStatus {request:archive_request.clone(),
            //weeks_total: archive_request.num_weeks(current_week),
            weeks_completed: 0,
            weeks_failed: Vec::new(),
            time_started: last_update_started,
            time_completed: None,
            last_udpate: None
        };

        ctx.set("status", Json(status));

        for week_request in archive_request.week_requests(current_week) {

            //info!("week: {:?}", week_request);

            // tood handle failures in workflow status
            let result = ctx.workflow_client::<ArchiveWeekWorkflowClient>(week_request.get_key())
                .run(Json(week_request.clone())).call().await;

            let mut status = ctx.get::<Json<ArchiveWorkflowStatus>>("status").await?.unwrap().into_inner();

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

    async fn get_status(&self, ctx:SharedWorkflowContext<'_>) -> Result<Json<ArchiveWorkflowStatus>, HandlerError> {

        let status = ctx.get::<Json<ArchiveWorkflowStatus>>("status").await?;
        if status.is_some() {
            return Ok(status.unwrap());
        }

        Err(TerminalError::new(format!("Status not found for request_id: {}", ctx.key())).into())
    }

}
