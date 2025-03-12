use std::time::Duration;

use ground_control::{data::{build_reqwest_client, cddis::get_cddis_file_path, s3_get_gz_object_buffer, s3_object_head, s3_put_object_url, sources::{OrbitSource, OrbitSourceImpl, RinexSource, Sources}}, objects::sv::{Orbit, SVImpl, DEFAULT_INTERPOLATION_ORDER, SV as _}};
use hifitime::Epoch;
use restate_sdk::{prelude::Endpoint, serde::{Json, Serialize}};
use restate_sdk_test_env::TestContainer;
use sp3::{prelude::SV, SP3};
use tracing::{error, info};

fn test_km_mm( a:(f64, f64, f64), b:(f64, f64, f64) ) -> bool {

    let e = 1_000_000.0;

    (a.0 * e).round() as i64 == (b.0 * e).round() as i64 &&
    (a.1 * e).round() as i64 == (b.1 * e).round() as i64 &&
    (a.2 * e).round() as i64 == (b.2 * e).round() as i64
}

#[tokio::test]
async fn r2_transfer_test() {
    tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::INFO) // Set the maximum log level
        .init();

    let loaded_env = dotenvy::dotenv();
    if loaded_env.is_err() || std::env::var("EARTHDATA_TOKEN").is_err() {
        tracing::info!("Failed to load keys from .env file: {}", loaded_env.err().unwrap());
        std::process::exit(1);
    }

    let file_path = "2357/COD0OPSULT_20250680000_02D_05M_ORB.SP3.gz";

    info!("starting download: {}", file_path);

    let client = build_reqwest_client().unwrap();
    let result = client.get(get_cddis_file_path(file_path))
         .bearer_auth(std::env::var("EARTHDATA_TOKEN").unwrap()).send().await; //unwrap().bytes().await.unwrap();

    if result.is_err() {
        error!("{}", result.err().unwrap());
        panic!();
    }

    let result = result.unwrap().bytes().await;

    if result.is_err() {
        error!("{}", result.err().unwrap());
        panic!();
    }

    let bytes = result.unwrap();

    info!("bytes downloaded: {}",  bytes.len());

    let upload_url = s3_put_object_url(file_path);
    let client = build_reqwest_client().unwrap();
    let result = client.put(upload_url).body(bytes).send().await.unwrap();

    if ! result.status().is_success() {
        error!("failed to upload file: {:?}", result.error_for_status_ref())
    }

    info!("uploaded file: {}", file_path);


    let head_url = s3_object_head(file_path);
    let client = build_reqwest_client().unwrap();
    let result = client.head(head_url).send().await.unwrap();

    if ! result.status().is_success() {
        error!("failed to get object head: {:?}", result.error_for_status_ref())
    }

    info!("object head: {:?}", result.headers())

    // let mut archived_listing = get_archived_directory_listing(file_request.week).await?;
    // archived_listing.files.insert(file_request.file_path.to_string(), file_request.hash);
    // put_archived_directory_listing(file_request.week, &archived_listing).await?;

    // let path_parts = cddis_path_parser(file_request.file_path.as_str());
    // if path_parts.is_some() {
    //     let path_parts  = path_parts.unwrap();
    //     let rinex_file = RinexSource {
    //         path:file_request.file_path.clone(),
    //         ac:path_parts["AC"].to_string(),
    //         solution:path_parts["TYP"].to_string(),
    //         solution_time:path_parts["TIME"].to_string(),
    //         content_type:path_parts["CNT"].to_string(),
    //         source:"CDDIS".to_string(),
    //         collected_at: Epoch::now()?.to_gpst_seconds(),
    //         sv_coverage: None
    //     };
    //     if path_parts["TYP"].eq("ULT") && path_parts["CNT"].eq("ORB") && path_parts["FMT"].eq("SP3") {
    //         ctx.object_client::<OrbitSourceClient>(rinex_file.get_source_key())
    //             .process_sp3(Json(rinex_file)).send();
    //     }
    // }

}


#[tokio::test]
async fn test_container() {
    tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::INFO) // Set the maximum log level
        .init();

    let loaded_env = dotenvy::dotenv();
    if loaded_env.is_err() || std::env::var("EARTHDATA_TOKEN").is_err() {
        tracing::info!("Failed to load keys from .env file: {}", loaded_env.err().unwrap());
        std::process::exit(1);
    }

    let endpoint = Endpoint::builder()
        .bind(OrbitSourceImpl.serve())
        .bind(SVImpl.serve())
        .build();

    // simple test container intialization with default configuration
    //let test_container = TestContainer::default().start(endpoint).await.unwrap();

    // custom test container initialization with builder
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

    // pub struct OrbitSourceSP3File {
    //     pub path:String,
    //     pub ac:String,
    //     pub solution: String,
    //     pub solution_time: String,
    //     pub source: String,
    //     pub collected_at:f64,
    // }

    let path = "2356/COD0OPSULT_20250640000_02D_05M_ORB.SP3.gz";

    let source = RinexSource {
        path:path.to_string(),
        ac:"COD".to_string(),
        solution:"ULT".to_string(),
        solution_time:"20250640000".to_string(),
        content_type: "ORB".to_string(),
        source:"CDDIS".to_string(),
        collected_at: 10.0,
        sv_coverage: None
    };

    // call container ingress url for /MyService/my_handler
    let response = reqwest::Client::new()
        .post(format!("{}/OrbitSource/COD_ULT/processSp3", ingress_url))
        .header("Accept", "application/json")
        .header("Content-Type", "*/*")
        .json(&source)
        .send()
        .await;

    assert!(response.is_ok());

    tokio::time::sleep(Duration::from_secs(5)).await;

    let epoch = 1425214207.5;

    info!("get orbit for E34 at: {}", epoch);
    let response = reqwest::Client::new()
        .get(format!("{}/SV/E34/getOrbit", ingress_url))
        .header("Accept", "application/json")
        .header("Content-Type", "*/*")
        .json(&epoch)
        .send()
        .await;

    let orbit_e34 = response.unwrap().json::<Orbit>().await.unwrap();

    info!("got orbit {:?}", orbit_e34);

    let response = reqwest::Client::new()
        .get(format!("{}/SV/G13/getOrbit", ingress_url))
        .header("Accept", "application/json")
        .header("Content-Type", "*/*")
        .json(&epoch)
        .send()
        .await;

    let orbit_g13 = response.unwrap().json::<Orbit>().await.unwrap();

    info!("got orbit {:?}", orbit_g13);

    let mut sp3_buf_reader = s3_get_gz_object_buffer(path).await.unwrap();
    let sp3 = SP3::from_reader(&mut sp3_buf_reader).unwrap();

    info!("has_satellite_clock_drift: {}", sp3.has_satellite_clock_drift());
    info!("has_satellite_clock_event: {}", sp3.has_satellite_clock_event());
    info!("has_satellite_clock_offset: {}", sp3.has_satellite_clock_offset());


    info!("calculating e34 from sp3...");
    let sv_e34 = SV {prn:34, constellation:sp3::prelude::Constellation::Galileo};

    let sp3_orbit_e34 =
        sp3.satellite_position_lagrangian_interpolation(sv_e34,
            Epoch::from_gpst_seconds(epoch),
            DEFAULT_INTERPOLATION_ORDER as usize).unwrap();

    info!("{:?}", sp3_orbit_e34);

    // assert_eq!((orbit_e34.x, orbit_e34.y, orbit_e34.z),
    //     (sp3_orbit_e34.0, sp3_orbit_e34.1, sp3_orbit_e34.2));

    assert!(test_km_mm((orbit_e34.x, orbit_e34.y, orbit_e34.z),
       (sp3_orbit_e34.0, sp3_orbit_e34.1, sp3_orbit_e34.2)));

    info!("calculating g13 from sp3...");
    let sv_g13 = SV {prn:13, constellation:sp3::prelude::Constellation::GPS};

    let sp3_orbit_g13 =
        sp3.satellite_position_lagrangian_interpolation(sv_g13,
            Epoch::from_gpst_seconds(epoch),
            DEFAULT_INTERPOLATION_ORDER as usize).unwrap();

    info!("{:?}", sp3_orbit_g13);

    // assert_eq!((orbit_g13.x, orbit_g13.y, orbit_g13.z),
    //     (sp3_orbit_g13.0, sp3_orbit_g13.1, sp3_orbit_g13.2));

    assert!(test_km_mm((orbit_g13.x, orbit_g13.y, orbit_g13.z),
       (sp3_orbit_g13.0, sp3_orbit_g13.1, sp3_orbit_g13.2)));


    tokio::time::sleep(Duration::from_secs(1)).await;

    let response = reqwest::Client::new()
        .get(format!("{}/OrbitSource/current/sources", ingress_url))
        .header("Accept", "application/json")
        .header("Content-Type", "*/*")
        .json(&epoch)
        .send()
        .await;

    let orbit_sources = response.unwrap().json::<Sources>().await.unwrap();
    info!("orbit sources: {:?}", orbit_sources);

}
