mod data;
mod product;
mod algo;

use axum::{extract::Path, response::IntoResponse, routing::{get, post}, Json, Router};
use axum::http::{HeaderValue, Method};
use data::sp3::{Sp3Data, Sp3DataImpl, Sp3File};
use hifitime::Epoch;
use product::sv::{DataSource, DataSources, DataSourcesImpl, Orbit, SVOrbits, SVOrbitsImpl, SVSource};
use reqwest::StatusCode;
use restate_sdk::prelude::{Endpoint, HttpServer};
use tracing::info;

const INGRESS_URL:&str = "http://127.0.0.1:8080/";

async fn add_cors_headers() -> impl IntoResponse {
    (
        [
            ("Access-Control-Allow-Origin", "*"),
            ("Access-Control-Allow-Methods", "GET, POST, OPTIONS"),
            ("Access-Control-Allow-Headers", "Content-Type, Authorization"),
        ],
        "",
    )
}

async fn process_sp3(Json(payload): Json<Sp3File>) -> impl IntoResponse {

    let response = reqwest::Client::new()
        .get(format!("{}/Sp3Data/processSp3", INGRESS_URL))
        .header("Accept", "application/json")
        .header("Content-Type", "*/*")
        .json(&payload)
        .send()
        .await;

    info!("{:?}", response);

    (StatusCode::OK, ()).into_response()
}

async fn get_sources() -> impl IntoResponse {

    let response = reqwest::Client::new()
        .post(format!("{}/DataSources/orbits/getSources", INGRESS_URL))
        .header("Accept", "application/json")
        .header("Content-Type", "*/*")
        .send()
        .await;

    (
        [
            ("Access-Control-Allow-Origin", "*"),
            ("Access-Control-Allow-Methods", "GET, POST, OPTIONS"),
            ("Access-Control-Allow-Headers", "Content-Type, Authorization"),
        ],
        Json(response.unwrap().text().await.unwrap())
    ).into_response()
}

async fn get_orbit(Path((source, sv, epoch_input)): Path<(String, String, Option<f64>)>) -> impl IntoResponse  {

    let epoch:f64;
    if epoch_input.is_none() {
        epoch = Epoch::now().unwrap().to_gpst_seconds();
    }
    else {
        epoch = epoch_input.unwrap();
    }

    let sv_source = SVSource {satellite:sv.to_uppercase(), data_source:DataSource::from_key(source).unwrap()};

    let response = reqwest::Client::new()
        .post(format!("{}/SVOrbits/{}/getOrbitPosition", INGRESS_URL, sv_source.get_key()))
        .header("Accept", "application/json")
        .header("Content-Type", "*/*")
        .body(epoch.to_string())
        .send()
        .await;

    if response.is_ok() && response.as_ref().unwrap().status().is_success() {
        let orbit:Orbit = serde_json::from_str(response.unwrap().text().await.unwrap().as_str()).unwrap();

        return (
            [
                ("Access-Control-Allow-Origin", "*"),
                ("Access-Control-Allow-Methods", "GET, POST, OPTIONS"),
                ("Access-Control-Allow-Headers", "Content-Type, Authorization"),
            ],
            Json(orbit)
        ).into_response();
    }

    (
        StatusCode::NOT_FOUND,
        [
            ("Access-Control-Allow-Origin", "*"),
            ("Access-Control-Allow-Methods", "GET, POST, OPTIONS"),
            ("Access-Control-Allow-Headers", "Content-Type, Authorization"),
        ],
        format!("SV {} not found", sv)
    ).into_response()

}

async fn get_orbits(Path((source, epoch)): Path<(String, f64)>) -> impl IntoResponse  {

    let response = reqwest::Client::new()
        .post(format!("{}/SVOrbits/{}/getSatellites", INGRESS_URL, source))
        .header("Accept", "application/json")
        .header("Content-Type", "*/*")
        .send()
        .await.unwrap();

    let satellites:Vec<String> = response.json().await.unwrap();

    let mut orbits:Vec<Orbit> = Vec::new();

    let data_source:DataSource = DataSource::from_key(source).unwrap();

    info!("getting orbits for: {:?}", satellites);

    for satellite in satellites {

        let sv_source = SVSource {satellite, data_source: data_source.clone()};

        let response = reqwest::Client::new()
            .post(format!("{}/SVOrbits/{}/getOrbitPosition", INGRESS_URL, sv_source.get_key()))
            .header("Accept", "application/json")
            .header("Content-Type", "*/*")
            .body(epoch.to_string())
            .send()
            .await;

        if response.is_ok() && response.as_ref().unwrap().status().is_success() {
            let orbit:Orbit = serde_json::from_str(response.unwrap().text().await.unwrap().as_str()).unwrap();

            orbits.push(orbit);
        }
    }

    (
        [
            ("Access-Control-Allow-Origin", "*"),
            ("Access-Control-Allow-Methods", "GET, POST, OPTIONS"),
            ("Access-Control-Allow-Headers", "Content-Type, Authorization"),
        ],
        Json(orbits)
    ).into_response()
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {

    tracing_subscriber::fmt::fmt()

        .with_max_level(tracing::Level::INFO) // Set the maximum log level
        .init();

    let loaded_env = dotenvy::dotenv();
    if loaded_env.is_err()
        || std::env::var("EARTHDATA_TOKEN").is_err()
        || std::env::var("AWS_ACCESS_KEY_ID").is_err() {
        tracing::info!("Failed to load keys from .env file: {}", loaded_env.err().unwrap());
        std::process::exit(1);
    }

    // build our application with a route
    let app = Router::new()
        .route("/orbit/{source}/{sv}/{epoch}", get(get_orbit).options(add_cors_headers))
        .route("/orbits/{source}/{epoch}", get(get_orbits).options(add_cors_headers))
        .route("/orbit/sources", get(get_sources).options(add_cors_headers))
        .route("/orbit/source", post(process_sp3).options(add_cors_headers));

    // run our app with hyper, listening globally on port 3000
    let _api_task = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind("0.0.0.0:3010").await.unwrap();
        axum::serve(listener, app).await.unwrap();
    });

    HttpServer::new(Endpoint::builder()
    .bind(SVOrbitsImpl.serve())
    .bind(Sp3DataImpl.serve())
    .bind(DataSourcesImpl.serve())
    .build())
        .listen_and_serve("0.0.0.0:9080".parse().unwrap())
        .await;


    Ok(())
}
