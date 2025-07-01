
use ground_control::data::{build_reqwest_client, cddis::get_cddis_file_path, s3_object_head, s3_put_object_url};
use tracing::{info, error};

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

    tracing::info!("starting download: {}", file_path);

    let client = build_reqwest_client().unwrap();
    let result = client.get(get_cddis_file_path(file_path))
         .bearer_auth(std::env::var("EARTHDATA_TOKEN").unwrap()).send().await; //unwrap().bytes().await.unwrap();

    if result.is_err() {
        tracing::error!("{}", result.err().unwrap());
        panic!();
    }

    let result = result.unwrap().bytes().await;

    if result.is_err() {
        tracing::error!("{}", result.err().unwrap());
        panic!();
    }

    let bytes = result.unwrap();

    tracing::info!("bytes downloaded: {}",  bytes.len());

    let upload_url = s3_put_object_url(file_path);
    let client = build_reqwest_client().unwrap();
    let result = client.put(upload_url).body(bytes).send().await.unwrap();

    if ! result.status().is_success() {
        tracing::error!("failed to upload file: {:?}", result.error_for_status_ref())
    }

    tracing::info!("uploaded file: {}", file_path);


    let head_url = s3_object_head(file_path);
    let client = build_reqwest_client().unwrap();
    let result = client.head(head_url).send().await.unwrap();

    if ! result.status().is_success() {
        tracing::error!("failed to get object head: {:?}", result.error_for_status_ref())
    }

    tracing::info!("object head: {:?}", result.headers())

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
