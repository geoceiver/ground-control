use cddis_archiver::{archiver::{CDDISArchiveRequest, CDDISWeekWorkflow, CDDISWeekWorkflowImpl, CDDISArchiveRequestWeekRange, CDDISArchiverWorkflow, CDDISArchiverWorkflowClient, CDDISArchiverWorkflowImpl}, queue::{CDDISFileQueue, CDDISFileQueueImpl}};
use restate_sdk::prelude::*;
use restate_sdk_test_env::TestContainer;


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

        let archive_request = CDDISArchiveRequest {
            request_id: request_id.clone(),
            parallelism: Some(5),
            weeks: Some(CDDISArchiveRequestWeekRange::RecentWeeks(3)),
            process_files: Some(false),
            recurring: Some(60)
        };

        ctx.workflow_client::<CDDISArchiverWorkflowClient>(request_id).run(Json(archive_request)).call().await?;

        Ok(())
    }

    async  fn get_status(&self, ctx: SharedWorkflowContext<'_>) -> Result<bool,HandlerError> {

        Ok(true)
    }
}

#[tokio::test]
async fn cddis_workflow_test() {
    tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::INFO) // Set the maximum log level
        .init();

    let loaded_env = dotenvy::dotenv();
    if loaded_env.is_err() || std::env::var("EARTHDATA_TOKEN").is_err() {
        tracing::info!("Failed to load keys from .env file: {}", loaded_env.err().unwrap());
        std::process::exit(1);
    }

    let endpoint = Endpoint::builder()
        .bind(CDDISFileQueueImpl.serve())
        .bind(CDDISArchiverWorkflowImpl.serve())
        .bind(CDDISWeekWorkflowImpl.serve())
        .bind(ArchiverFullTestWorkflowImpl.serve())
        .build();

    let test_container = TestContainer::builder()
        // optional passthrough logging from the resstate server testcontainer
        // prints container logs to tracing::info level
        .with_container_logging()
        .with_container(
            "docker.io/restatedev/restate".to_string(),
            "latest".to_string(),
        )
        .build()
        .start(endpoint)
        .await
        .unwrap();

    let ingress_url = test_container.ingress_url();

    let response = reqwest::Client::new()
        .get(format!("{}/ArchiverFullTestWorkflow/test/run", ingress_url))
        .header("Accept", "application/json")
        .header("Content-Type", "*/*")
        .send()
        .await;

    assert!(response.is_ok());
}
