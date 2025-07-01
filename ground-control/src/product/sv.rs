use std::collections::HashMap;

use anyhow::anyhow;
use regex::Regex;
use restate_sdk::prelude::*;
use tracing::info;
use anise::{constants::frames::EARTH_J2000, math::{cartesian::CartesianState, Vector3}, prelude::Almanac};

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Clone)]
pub struct DataSource {
    pub source:String,
    pub analysis_center:String,
    pub product_type:String,
}

impl DataSource {
    pub fn get_key(&self) -> String {
        format!("{}_{}_{}", self.source,
            self.analysis_center,
            self.product_type)
    }

    pub fn from_key(key:String) -> Result<Self, anyhow::Error> {

        let key_parts:Vec<&str> = key.splitn(4, "_").collect();

        if key_parts.len() != 3 {
            return Err(anyhow!("Invalid data source key"));
        }

        Ok(DataSource {source:key_parts[0].to_string(), analysis_center:key_parts[1].to_string(), product_type:key_parts[2].to_string() })
    }

    pub fn defaults() -> Self {
        DataSource {source:"cddis".to_string(), analysis_center:"cod".to_string(), product_type:"ult".to_string() }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Clone)]
pub struct SVSource {
    pub satellite:String,
    pub data_source:DataSource,
}

impl SVSource{

    pub fn get_key(&self) -> String {
        format!("{}_{}_{}_{}", self.data_source.source,
            self.data_source.analysis_center,
            self.data_source.product_type,
            self.satellite.to_lowercase())
    }

    pub fn from_key(key:&str) -> Result<Self, anyhow::Error> {

        let key_parts:Vec<&str> = key.splitn(4, "_").collect();
        let satellite_regex = Regex::new(r"[a-zA-Z]\d\d")?;
        if key_parts.len() == 1 &&
            satellite_regex.is_match(key_parts[0]) {
                let sv_source = SVSource {satellite:key_parts[0].to_string(), data_source:DataSource::defaults()};
                return Ok(sv_source);
        }
        else if key_parts.len() == 4 {

        }

        Err(anyhow!("invalid key format"))
    }
}


#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Clone)]
pub struct Orbit {

    pub sv:SVSource,
    pub product_run_id:u64,
    pub sampling_resolution:f64,
    pub valid_from:f64,
    pub valid_to:f64,
    pub epochs:Vec<f64>,
    pub pos_ecef_km:Vec<Vector3>,
    pub pos_latlonalt:Option<Vec<(f64, f64, f64)>>,
    pub clock_usec:Option<Vec<f64>>

}

impl Orbit {

    pub fn get_position_at(&self, epoch:f64) -> Result<Orbit, anyhow::Error> {

        let orbit = self.lagrange_orbit_interpolation(epoch, 17)?;

        Ok(orbit)
    }

    pub fn lagrange_orbit_interpolation(&self, epoch:f64, order:i64) -> Result<Orbit, anyhow::Error> {

        info!("caclulating orbit for {} at {}", self.sv.satellite, epoch);

        let odd_order = order % 2 > 0;
        let (min_before, min_after): (usize, usize) = match odd_order {
            true => (((order + 1) / 2) as usize, ((order + 1) / 2) as usize),
            false => ((order / 2) as usize, (order / 2 + 1) as usize),
        };

        let epoch_index_fractional = (epoch - self.valid_from) / self.sampling_resolution;
        let epoch_index = epoch_index_fractional.round() as usize;

        if epoch_index < min_before || epoch_index + min_after > self.epochs.len() - 1 {
            return Err(anyhow!("Interpolation window outside epoch data range."));
        }

        let interpolation_range = epoch_index - min_before..epoch_index + min_after;
        let interpolation_epochs = &self.epochs[interpolation_range.clone()];
        let interpolation_positions = &self.pos_ecef_km[interpolation_range.clone()];

        if epoch_index_fractional.fract() == 0.0 {

            let mut orbit = self.clone();

            orbit.epochs = interpolation_epochs.to_vec();
            orbit.pos_ecef_km = interpolation_positions.to_vec();
            orbit.clock_usec = None;

            info!("Done! orbit {} at {}", self.sv.satellite, epoch);
            return Ok(orbit);

        }

        let mut result_position = Vector3::new(0.0, 0.0, 0.0);

        // Lagrange interpolation for SP3 orbit data
        let n = interpolation_epochs.len();
        for i in 0..n {
            let orbit_i_epoch = &interpolation_epochs[i];
            let mut li = 1.0_f64;

            // Compute the lagrange basis polynomial l_i(t)
            for j in 0..n {
                if i != j {
                    let orbit_j_epoch = &interpolation_epochs[j];
                    let delta_t = orbit_i_epoch - orbit_j_epoch;
                    li *= (epoch - orbit_j_epoch) / delta_t;
                }
            }

            let orbit_i_position = interpolation_positions[i];

            result_position += li * orbit_i_position;
        }


        let mut orbit = self.clone();

        orbit.epochs = interpolation_epochs.to_vec();
        orbit.epochs .insert(min_before+1, epoch);
        orbit.pos_ecef_km = interpolation_positions.to_vec();
        orbit.pos_ecef_km.insert(min_before+1, result_position);


        orbit.clock_usec = None;

        info!("Done! orbit {} at {}", self.sv.satellite, epoch);
        Ok(orbit)
    }

}

#[restate_sdk::object]
pub trait DataSources {
    #[name = "updateSource"]
    async fn update_source(data_source:Json<DataSource>) -> Result<(), HandlerError>;

    #[shared]
    #[name = "getSources"]
    async fn get_sources() -> Result<Json<HashMap<String, DataSource>>, HandlerError>;
}

pub struct DataSourcesImpl;

impl DataSources for DataSourcesImpl {

    async fn update_source(&self, ctx:ObjectContext<'_>, data_source:Json<DataSource>) -> Result<(), HandlerError> {
        let data_source = data_source.into_inner();
        info!("insert source: {}", data_source.get_key());
        ctx.set(&data_source.get_key(), Json(data_source));

        Ok(())
    }

    async fn get_sources(&self, ctx:SharedObjectContext<'_>) -> Result<Json<HashMap<String, DataSource>>, HandlerError> {
        let keys = ctx.get_keys().await.unwrap();

        let mut data_sources:HashMap<String, DataSource> = HashMap::new();

        for key in keys {
            data_sources.insert(key.clone(), ctx.get::<Json<DataSource>>(&key).await.unwrap().unwrap().into_inner());
        }

        Ok(Json(data_sources))
    }

}

#[restate_sdk::object]
pub trait SVOrbits {

    #[name = "updateSatellites"]
    async fn update_satellites(satellites:Json<Vec<String>>) -> Result<(), HandlerError>;

    #[name = "getSatellites"]
    async fn get_satellites() -> Result<Json<Vec<String>>, HandlerError>;

    #[name = "updateOrbit"]
    async fn update_orbit(orbit:Json<Orbit>) -> Result<(), HandlerError>;

    #[shared]
    #[name = "getOrbitPosition"]
    async fn get_position(epoch:f64) -> Result<Json<Orbit>, HandlerError>;
}

pub struct SVOrbitsImpl;

impl SVOrbits for SVOrbitsImpl {

    async fn update_satellites(&self, ctx: ObjectContext<'_>, satellites:Json<Vec<String>>) -> Result<(), HandlerError> {
        ctx.set("satellites", satellites);
        Ok(())
    }

    async fn get_satellites(&self, ctx: ObjectContext<'_>) -> Result<Json<Vec<String>>, HandlerError> {
        let satellites:Json<Vec<String>> = ctx.get("satellites").await.unwrap().unwrap();
        Ok(satellites)
    }

    async fn update_orbit(&self, ctx: ObjectContext<'_>, orbit:Json<Orbit>) -> Result<(), HandlerError> {
        let orbit = orbit.into_inner();
        info!("set orbit for {}", ctx.key());
        ctx.set("orbit", Json(orbit.clone()));
        ctx.object_client::<DataSourcesClient>("orbits").update_source(Json(orbit.sv.data_source)).send();
        Ok(())
    }

    async fn get_position(&self, ctx: SharedObjectContext<'_>, epoch:f64) -> Result<Json<Orbit>, HandlerError> {

        let orbit = ctx.get::<Json<Orbit>>("orbit").await?;

        if orbit.is_some() {

            let almanac = Almanac::new("pck08.pca");
            if almanac.is_err() {
                info!("alamanc error: {}", almanac.as_ref().err().unwrap().to_string())
            }
            let frame = almanac.as_ref().unwrap().frame_from_uid(EARTH_J2000);

            if frame.is_err() {
                info!("frame error: {}", frame.as_ref().err().unwrap().to_string())
            }

            let frame = frame.unwrap();

            let position = orbit.unwrap().into_inner().get_position_at(epoch);

            if position.is_err() {
                return Err(TerminalError::new(position.err().unwrap().to_string()).into());
            }

            let mut position = position.unwrap();

            let mut pos_latlonalt:Vec<(f64, f64, f64)> = Vec::new();

            for i in 0..position.epochs.len() {

                let epoch = position.epochs[i];
                let pos = position.pos_ecef_km[i];

                let epoch = hifitime::Epoch::from_gpst_seconds(epoch);
                let cartian_state = CartesianState::from_position(pos.x, pos.y, pos.z, epoch, frame);
                let latlonalt = cartian_state.latlongalt();

                if latlonalt.is_err() {
                    info!("latlonalt error: {}", latlonalt.err().unwrap().to_string());
                }
                else {
                    pos_latlonalt.push(latlonalt.unwrap());
                }

            }

            position.pos_latlonalt = Some(pos_latlonalt);

            return Ok(Json(position));

        }

        Err(TerminalError::new("Missing orbit data.").into())
    }
}
