#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;


use archiver::{ArchiveRequest, ArchiveWeekWorkflow, ArchiveWeekWorkflowImpl, ArchiveWeeks, ArchiverWorkflow, ArchiverWorkflowClient, ArchiverWorkflowImpl};
use queue::{ArchiverFileQueue, ArchiverFileQueueImpl};
use restate_sdk::prelude::*;
use tracing::info;

mod archiver;
mod queue;
mod r2;
mod cddis;
mod utils;

#[restate_sdk::workflow]
trait ArchiverFullTestWorkflow {
    async fn run() -> Result<(), HandlerError>;

    #[shared]
    async fn get_status() -> Result<bool, HandlerError>;
}

struct ArchiverFullTestWorkflowImpl;

impl ArchiverFullTestWorkflow for ArchiverFullTestWorkflowImpl {

    async fn run(&self, mut ctx: WorkflowContext<'_>) ->  Result<(),HandlerError>  {

        let request_id = format!("test_{}", ctx.rand_uuid());

        let archive_request = ArchiveRequest {
            request_id: request_id.clone(),
            parallelism: Some(25),
            weeks: Some(ArchiveWeeks::RecentWeeks(3)),
            process_files: Some(false),
            recurring: Some(false)
        };

        info!("starting job: {:?}", archive_request);

        ctx.workflow_client::<ArchiverWorkflowClient>(request_id).run(Json(archive_request)).call().await?;

        Ok(())
    }

    async  fn get_status(&self, ctx: SharedWorkflowContext<'_>) -> Result<bool,HandlerError> {

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
        .bind(ArchiverFileQueueImpl.serve())
        .bind(ArchiverWorkflowImpl.serve())
        .bind(ArchiveWeekWorkflowImpl.serve())
        .bind(ArchiverFullTestWorkflowImpl.serve())
        .build();

    HttpServer::new(endpoint)
        .listen_and_serve("0.0.0.0:9080".parse().unwrap())
        .await;

}
