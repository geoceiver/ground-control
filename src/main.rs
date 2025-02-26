mod objects;
mod crawl;
use restate_sdk::prelude::*;
use objects::sv::{SVImpl, SV};
use crawl::cddis::{CDDISArchiveFile, CDDISArchiveFileImpl, CDDISArchiveWeek, CDDISArchiveWeekImpl, CDDISArchiveWorkflow, CDDISArchiveWorkflowImpl};
//use tracing::info;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // TODO move key management from env file
    // e.g. https://crates.io/crates/secret-vault
    let loaded_env = dotenvy::dotenv();
    if loaded_env.is_err() || std::env::var("EARTHDATA_TOKEN").is_err() {
        tracing::info!("Failed to load keys from .env file: {}", loaded_env.err().unwrap());
        std::process::exit(1);
    }

    //info!("EARTHDATA_TOKEN {}", std::env::var("EARTHDATA_TOKEN").unwrap_or_else(|_| "not set".to_string()));

    HttpServer::new(
        Endpoint::builder()
            .bind(CDDISArchiveWorkflowImpl.serve())
            .bind(CDDISArchiveFileImpl.serve())
            .bind(CDDISArchiveWeekImpl.serve())
            .bind(SVImpl.serve())
            .build(),
    )
    .listen_and_serve("0.0.0.0:9080".parse().unwrap())
    .await;
}
