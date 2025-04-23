use std::time::Duration;

use ground_control::{data::sp3::{Sp3Data, Sp3DataImpl}, product::sv::{Orbit, SVOrbits, SVOrbitsImpl}};
use restate_sdk::prelude::*;
use restate_sdk_test_env::TestContainer;
use ground_control::data::sp3::Sp3File;
use tracing::info;

#[tokio::test]
async fn sp3_loader_workflow_test() {
    tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::INFO) // Set the maximum log level
        .init();

    let loaded_env = dotenvy::dotenv();

    let endpoint = Endpoint::builder()
        .bind(SVOrbitsImpl.serve())
        .bind(Sp3DataImpl.serve())
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

    let sp3_file = Sp3File {source: "cddis".to_string(), archive_path:"/cddis/2361/COD0OPSULT_20250960000_02D_05M_ORB.SP3.gz".to_string()};

    let response = reqwest::Client::new()
        .get(format!("{}/Sp3Data/cddis/processSp3", ingress_url))
        .header("Accept", "application/json")
        .header("Content-Type", "*/*")
        .json(&sp3_file)
        .send()
        .await;

    assert!(response.unwrap().status().is_success());

    tokio::time::sleep(Duration::from_secs(2)).await;

    let epoch = 1427935500.0;

    let response = reqwest::Client::new()
        .get(format!("{}/SVOrbits/G01/getOrbitPosition", ingress_url))
        .header("Accept", "application/json")
        .header("Content-Type", "*/*")
        .body(epoch.to_string())
        .send()
        .await;

    assert!(response.as_ref().unwrap().status().is_success());

    let result = response.unwrap().bytes().await.unwrap();

    info!("orbit pos1 response: {:#?}", result);



    let epoch = 1427935515.2;

    let response = reqwest::Client::new()
        .get(format!("{}/SVOrbits/G01/getOrbitPosition", ingress_url))
        .header("Accept", "application/json")
        .header("Content-Type", "*/*")
        .body(epoch.to_string())
        .send()
        .await;

    assert!(response.as_ref().unwrap().status().is_success());

    let result = response.unwrap().bytes().await.unwrap();

    info!("orbit pos2 response: {:#?}", result);



    let epoch = 1427945315.2;

    let response = reqwest::Client::new()
        .get(format!("{}/SVOrbits/G01/getOrbitPosition", ingress_url))
        .header("Accept", "application/json")
        .header("Content-Type", "*/*")
        .body(epoch.to_string())
        .send()
        .await;

    assert!(response.as_ref().unwrap().status().is_success());

    let result = response.unwrap().bytes().await.unwrap();

    info!("orbit pos3 response: {:#?}", result);

}
