use cddis_archiver::queue::{CDDISArchiverFileQueue, CDDISArchiverFileQueueClient, CDDISArchiverFileQueueImpl, CDDISFileQueueData, CDDISFileRequest};
use restate_sdk::prelude::*;
use restate_sdk_test_env::TestContainer;
use tracing::info;


#[restate_sdk::workflow]
trait ArchiverTestWorkflow {
    async fn run() -> Result<(), HandlerError>;

    #[shared]
    async fn get_status() -> Result<bool, HandlerError>;
}

struct ArchiverTestWorkflowImpl;

impl ArchiverTestWorkflow for ArchiverTestWorkflowImpl {

    async fn run(&self, mut ctx: WorkflowContext<'_>) ->  Result<(),HandlerError>  {

        let request_id = format!("test_{}", ctx.rand_uuid());

        let queue_data = CDDISFileQueueData {
            request_id: request_id.clone(),
            queue_num: 1
        };

        let archive_path = format!("/test_data/{}_COD0OPSULT_20250640000_02D_05M_ORB.SP3.gz", request_id);

        let file_request = CDDISFileRequest {
            request_queue: queue_data.clone(),
            week: 2356,
            path: "https://cddis.nasa.gov/archive/gnss/products/2356/COD0OPSULT_20250640000_02D_05M_ORB.SP3.gz".to_string(),
            hash: "13d957c5404d8868f9da1b545d406909b2ab0b5869aea29a866b47eb1ea0e1fc016ac7939d438f8256cdae84173775b2d67f87a35cb771331bde9c0d940974e6".to_string(),
            archive_path,
            process_files:false
        };

        info!("sending test file request...");

        let response = ctx.object_client::<CDDISArchiverFileQueueClient>(request_id).archive_file(Json(file_request)).call();

        info!("awaiting test file request...");

        response.await?;

        info!("test complete...");

        Ok(())
    }

    async  fn get_status(&self, ctx: SharedWorkflowContext<'_>) -> Result<bool,HandlerError> {

        Ok(true)
    }
}

#[tokio::test]
async fn cddis_r2_transfer_test() {
    tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::INFO) // Set the maximum log level
        .init();

    let loaded_env = dotenvy::dotenv();
    if loaded_env.is_err() || std::env::var("EARTHDATA_TOKEN").is_err() {
        tracing::info!("Failed to load keys from .env file: {}", loaded_env.err().unwrap());
        std::process::exit(1);
    }

    let endpoint = Endpoint::builder()
        .bind(CDDISArchiverFileQueueImpl.serve())
        .bind(ArchiverTestWorkflowImpl.serve())
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
        .get(format!("{}/ArchiverTestWorkflow/test/run", ingress_url))
        .header("Accept", "application/json")
        .header("Content-Type", "*/*")
        .send()
        .await;
}
