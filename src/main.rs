#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

mod objects;
mod data;
use restate_sdk::prelude::*;
use objects::sv::{SVImpl, SV};
use data::{cddis::{CDDISArchiveWeek, CDDISArchiveWeekImpl, CDDISArchiveWorkflow, CDDISArchiveWorkflowImpl}, sources::{OrbitSource, OrbitSourceImpl}};

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

    HttpServer::new(
        Endpoint::builder()
            .bind(CDDISArchiveWorkflowImpl.serve())
            .bind(CDDISArchiveWeekImpl.serve())
            .bind(OrbitSourceImpl.serve())
            .bind(SVImpl.serve())
            .build(),
    )
    .listen_and_serve("0.0.0.0:9080".parse().unwrap())
    .await;
}
