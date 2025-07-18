#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;


use archiver::{CDDISArchiveRequestWeekRange, CDDISArchiveRequest, CDDISWeekWorkflow, CDDISWeekWorkflowImpl, CDDISArchiverWorkflow, CDDISArchiverWorkflowClient, CDDISArchiverWorkflowImpl};
use queue::{CDDISFileQueue, CDDISFileQueueImpl};
use restate_sdk::prelude::*;
use tracing::info;

mod archiver;
mod queue;
mod r2;
mod cddis;
mod utils;

#[restate_sdk::workflow]
trait CDDISWorkflow {
    async fn run() -> Result<(), HandlerError>;

    #[shared]
    async fn get_status() -> Result<bool, HandlerError>;
}

struct CDDISWorkflowImpl;

impl CDDISWorkflow for CDDISWorkflowImpl {

    async fn run(&self, mut ctx: WorkflowContext<'_>) ->  Result<(),HandlerError>  {

        let task_id = ctx.rand_uuid();
        let workflow_id = ctx.key();

        let request_id = format!("{}_{}", workflow_id, task_id);

        let archive_request = CDDISArchiveRequest {
            request_id: request_id.clone(),
            parallelism: Some(25),
            weeks: Some(CDDISArchiveRequestWeekRange::AllWeeks),
            process_files: Some(true),
            recurring: Some(60*5)
        };

        info!("starting job: {:?}", archive_request);

        ctx.workflow_client::<CDDISArchiverWorkflowClient>(request_id).run(Json(archive_request)).call().await?;

        Ok(())
    }

    async  fn get_status(&self, _ctx: SharedWorkflowContext<'_>) -> Result<bool,HandlerError> {

        Ok(true)
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::INFO) // Set the maximum log level
        .init();

    let loaded_env = dotenvy::dotenv();
    if loaded_env.is_err() || std::env::var("EARTHDATA_TOKEN").is_err() {
        info!("Failed to load keys from .env file: {}", loaded_env.err().unwrap());
        std::process::exit(1);
    }

    let endpoint = Endpoint::builder()
        .bind(CDDISFileQueueImpl.serve())
        .bind(CDDISWeekWorkflowImpl.serve())
        .bind(CDDISArchiverWorkflowImpl.serve())
        .bind(CDDISWorkflowImpl.serve())
        .build();

    HttpServer::new(endpoint)
        .listen_and_serve("0.0.0.0:9080".parse().unwrap())
        .await;

}
