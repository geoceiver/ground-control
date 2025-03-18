use regex::{Captures, Regex};
use anyhow::{anyhow, Error};
use restate_sdk::{errors::{HandlerError, TerminalError}, serde::Json};

use crate::{archiver::DirectoryListing, utils::build_reqwest_client};

const CDDIS_PATH:&str = "https://cddis.nasa.gov/archive/gnss/products";

fn get_cddis_week_path(week:u32) -> String {
    format!("{}/{}", CDDIS_PATH, week)
}

pub fn get_cddis_file_path(week:u32, file_path:&str) -> String {
    format!("{}/{}/{}", CDDIS_PATH, week, file_path)
}

pub fn get_archive_file_path(week:u32, file_path:&str) -> String {
    format!("cddis/{}/{}", week, file_path)
}

pub fn cddis_path_parser(path:&str) -> Option<Captures<'_>> {
    let re = Regex::new(r"^(?<WEEK>\d{4})\/(?<AC>.{3})0(?<PROJ>.{3})(?<TYP>.{3})_(?<TIME>[0-9]{11})_(?<PER>.*)_(?<SMP>.*)_(?<CNT>.*)\.(?<FMT>.*)\.gz$").unwrap();
    return re.captures(path);
}

pub async fn get_cddis_directory_listing(week:u32) -> Result<Json<DirectoryListing>, HandlerError> {

    let client = build_reqwest_client()?;
    let response = client.get(format!("{}/SHA512SUMS", get_cddis_week_path(week)))
        .bearer_auth(std::env::var("EARTHDATA_TOKEN").unwrap())
        .send()
        .await?;

    if response.status().is_success() {
        let directory_listing_text = response.text().await?;

        let mut directory_listing = DirectoryListing::default();

        for line in directory_listing_text.lines() {
            let line_parts:Vec<&str> = line.split_whitespace().collect();

            if line_parts.len() == 2 {
                // store week/name.ext format
                let file = line_parts.get(1).unwrap().to_string();
                let hash = line_parts.get(0).unwrap().to_string();
                directory_listing.add_file(file, hash);
            }
        }

        return Ok(Json(directory_listing));
    }

    Err(TerminalError::new("Unable to get current directory listing.").into())

}
