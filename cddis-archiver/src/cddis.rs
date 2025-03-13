use hifitime::Epoch;
use restate_sdk::{errors::HandlerError, prelude::{ContextSideEffects, WorkflowContext}, serde::Json};

const MIN_GPST_WEEKS: u32 = 2238; // CDDIS format changed for products prior to week 2238
const DEFAULT_WEEK_LOOKBACK_PERIOD: u32 = 12; // defualt

#[derive(serde::Serialize, serde::Deserialize,Debug, PartialEq, Clone)]
pub enum ArchiveWeeks {
    AllWeeks,
    RecentWeeks(u32),
    WeeksList(Vec<u32>)
}

#[derive(serde::Serialize, serde::Deserialize,Debug, PartialEq, Clone)]
pub struct CDDISArchiveRequest {
    request_id:String,
    weeks:Option<ArchiveWeeks>,
    parallelism:Option<u32>,
    process_files:Option<bool>,
    recurring:Option<bool>,
}

impl CDDISArchiveRequest {

    fn with_defaults(self) -> Self {
        let request_id = self.request_id;
        let weeks = Some(self.weeks.unwrap_or(ArchiveWeeks::RecentWeeks(DEFAULT_WEEK_LOOKBACK_PERIOD)));
        let parallelism = Some(self.parallelism.unwrap_or(1));
        let process_files = Some(self.process_files.unwrap_or(false));
        let recurring = Some(self.recurring.unwrap_or(false));

        Self {request_id, weeks, parallelism, process_files, recurring}
    }
}


pub fn gpst_week(epoch:&Epoch) -> u32 {
    let gpst_week = (epoch.to_gpst_days() / 7.0).floor() as u32;
    gpst_week
}

// async so it can be wraped in a restate run closure
async fn current_gpst_seconds() -> Result<f64, HandlerError> {
    Ok(Epoch::now()?.to_gpst_seconds())
}

// async so it can be wraped in a restate run closure
async fn current_gpst_week() -> Result<u32, HandlerError> {
    Ok(gpst_week(&Epoch::now()?))
}


#[derive(serde::Serialize, serde::Deserialize,Debug, PartialEq, Clone)]
pub struct CDDISFileRequest {
    request_id:String,
    cddis_path:String,
    archive_path:String,
    process_files:bool
}

#[restate_sdk::object]
pub trait CDDISFileQueue {
    async fn archive_file(file_request:Json<CDDISFileRequest>) -> Result<(), HandlerError>;
}

#[restate_sdk::object]
pub trait CDDISArchiver {
    async fn archive_week(weeks_request:Json<CDDISArchiveRequest>) -> Result<(), HandlerError>;
}

#[restate_sdk::workflow]
pub trait CDDISArchiverWorkflow {
    async fn run(weeks_request:Json<CDDISArchiveRequest>) -> Result<(), HandlerError>;
}

struct CDDISArchiverWorkflowImpl;

impl CDDISArchiverWorkflow for CDDISArchiverWorkflowImpl {

    async fn run(&self, ctx: WorkflowContext<'_>, weeks_request:Json<CDDISArchiveRequest>) -> Result<(), HandlerError> {

        let weeks_request = weeks_request.into_inner().with_defaults();

        let last_update_started = ctx.run(||current_gpst_seconds()).await.unwrap();
        let current_week = ctx.run(||current_gpst_week()).await.unwrap();

        let weeks;
        match weeks_request.weeks.unwrap() {
            ArchiveWeeks::AllWeeks => {
                weeks = MIN_GPST_WEEKS..current_week;
            },
            ArchiveWeeks::RecentWeeks(look_back_period) => {
                weeks = (current_week-look_back_period)..current_week;
            },
            ArchiveWeeks::WeeksList(weeks) => {

            },
        }

        Ok(())
    }

}
