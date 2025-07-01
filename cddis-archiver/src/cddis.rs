use restate_sdk::{errors::{HandlerError, TerminalError}, serde::Json};

use crate::{archiver::DirectoryListing, utils::build_reqwest_client};

const CDDIS_PATH:&str = "https://cddis.nasa.gov/archive/gnss/products";

// Builds CDDIS weekly directory path with GPS week
fn get_cddis_week_path(gps_week:u32) -> String {
    format!("{}/{}", CDDIS_PATH, gps_week)
}

// Builds CDDIS file path with GPS week and file name
pub fn get_cddis_file_path(gps_week:u32, file_name:&str) -> String {
    let week_path = get_cddis_week_path(gps_week);
    format!("{}/{}", week_path, file_name)
}

// Builds archival path for storing CDDIS data with GPS week and file name
pub fn get_cddis_archive_file_path(gps_week:u32, file_name:&str) -> String {
    format!("/cddis/{}/{}", gps_week, file_name)
}

pub async fn get_cddis_directory_listing(week:u32) -> Result<Json<DirectoryListing>, HandlerError> {

    let client = build_reqwest_client()?;
    let response = client.get(format!("{}/SHA512SUMS", get_cddis_week_path(week)))
        .bearer_auth(std::env::var("EARTHDATA_TOKEN").unwrap())
        .send()
        .await?;

    if response.status().is_success() {
        let directory_listing_text = response.text().await?;

        let mut directory_listing = DirectoryListing::new(week);

        for line in directory_listing_text.lines() {
            let line_parts:Vec<&str> = line.split_whitespace().collect();

            if line_parts.len() == 2 {
                // store week/name.ext format
                let file = line_parts.get(1).unwrap().to_string();
                let hash = line_parts.first().unwrap().to_string();
                directory_listing.add_file(file, hash);
            }
        }

        return Ok(Json(directory_listing));
    }

    Err(TerminalError::new("Unable to get current directory listing.").into())

}
