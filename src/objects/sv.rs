use anise::{frames::Frame, math::Vector3};
use anyhow::anyhow;

use restate_sdk::prelude::*;
use tracing::info;

//use rkyv::{Archive, Deserialize, Serialize};

// #[derive(serde::Serialize, serde::Deserialize,Debug, PartialEq, Clone)]
// pub enum Source {
//     IGSUltraRapid(String),
//     IGSRapid(String),
//     IGSFinal(String),
//     Geoceiver(String)
// }

// #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Clone)]
// pub enum Interpolation {
//     None,
//     Geoceiver(String)
// }

pub const DEFAULT_INTERPOLATION_ORDER:i64 = 17; // todo find default in RTKLIB

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Clone)]
pub struct Orbit {
    pub epoch:f64,
    pub x:f64,
    pub y:f64,
    pub z:f64,
    pub clock_offset:Option<f64>,
    pub sv:Option<String>,
    pub ac:Option<String>,
    pub solution: Option<String>,
    pub solution_time: Option<String>,
    pub source:Option<String>,
    pub frame:Option<Frame>
}

impl Orbit {

    pub fn new(epoch:f64, x:f64, y:f64, z:f64, clock_offset:Option<f64>) -> Self {
        Orbit {epoch, x, y, z, clock_offset, sv: None, ac: None, solution: None, solution_time: None, source: None, frame: None}
    }

    pub fn with_sv(mut self, sv:String) -> Self {
        self.sv = Some(sv);
        self
    }

    pub fn with_solution(mut self, solution:String) -> Self {
        self.solution = Some(solution);
        self
    }

    pub fn with_solution_time(mut self, solution_time:String) -> Self {
        self.solution_time = Some(solution_time);
        self
    }

    pub fn with_source(mut self, source:String) -> Self {
        self.source = Some(source);
        self
    }

    pub fn with_frame(mut self, frame:Frame) -> Self {
        self.frame = Some(frame);
        self
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Clone)]
pub struct Orbits {
    pub sv:String,
    pub ac:String,
    pub solution:String,
    pub solution_time:String,
    pub source: String,
    pub valid_from:f64,
    pub valid_to:f64,
    pub period:f64,
    pub collected_at:f64,
    pub epochs:Vec<Orbit>,
    pub frame:Option<Frame>
}

impl  Orbits
{

    // pub fn serialize(&self) -> Result<bytes::Bytes, rkyv::rancor::Error> {
    //     let _bytes = rkyv::to_bytes::<rkyv::rancor::Error>(self).unwrap();
    //     let bytes = Bytes::from(_bytes.to_vec());
    //     return Ok(bytes)
    // }

    // pub fn from_bytes(bytes: &bytes::Bytes) -> Result<Orbits, rkyv::rancor::Error> {
    //     let archived = ArchivedOrbits::from_bytes(bytes);
    //     let deserialized = rkyv::deserialize::<Orbits, rkyv::rancor::Error>(archived.unwrap()).unwrap();
    //     return Ok(deserialized);
    // }

    pub fn get_key(&self) -> String {
        format!("ORBITS_{}_{}", self.ac, self.solution )
    }

    pub fn get_clock_at(&self, epoch:f64) -> Option<f64> {

        info!("getting clock offset for {} at {}", self.sv, epoch);

        if epoch < self.valid_from || epoch > self.valid_to {
            return None;
        }
        let epoch_index = (epoch - self.valid_from) / self.period;

        let i = epoch_index.round() as usize;
        if epoch_index.fract() == 0.0 {

            let orbit = self.epochs.get(i).unwrap();

            if orbit.clock_offset.is_some() {
                info!("clock offset for {} found at at index {}: {}", self.sv, i, orbit.clock_offset.unwrap());
                return Some(orbit.clock_offset.unwrap());
            }
            else {
                return None;
            }
        }

        info!("interpoloating clock offset for {} at index {} ({})", self.sv, epoch_index, i);

        // TODO switch to weighted average?
        let orbit = self.epochs.get(i).unwrap();
        if orbit.clock_offset.is_some() {
            info!("interpolating clock offset at {}: {}", i, orbit.clock_offset.unwrap());
            return Some(orbit.clock_offset.unwrap());
        }

        None
    }

    pub fn get_orbit_at(&self, epoch:f64) -> Result<Orbit, anyhow::Error> {

        if epoch < self.valid_from || epoch > self.valid_to {
            return Err(anyhow!("Epoch outside orbit data range."));
        }

        let epoch_index = (epoch - self.valid_from) / self.period;
        if epoch_index.fract() == 0.0 {
            let i = epoch_index as usize;
            let orbit = self.epochs.get(i).unwrap().clone();
            return Ok(orbit);
        }
        else {
            self.lagrange_orbit_interpolation(epoch, DEFAULT_INTERPOLATION_ORDER)
        }

    }

    pub fn lagrange_orbit_interpolation(&self, epoch:f64, order:i64) -> Result<Orbit, anyhow::Error> {

        info!("caclulating orbit for {} at {} using {}", self.sv, epoch, self.source);

        let odd_order = order % 2 > 0;
        let (min_before, min_after): (usize, usize) = match odd_order {
            true => (((order + 1) / 2) as usize, ((order + 1) / 2) as usize),
            false => ((order / 2) as usize, (order / 2 + 1) as usize),
        };

        let epoch_index_fractional = (epoch - self.valid_from) / self.period;
        let epoch_index = epoch_index_fractional.round() as usize;

        if epoch_index < min_before || epoch_index + min_after > self.epochs.len() - 1 {
            return Err(anyhow!("Interpolation window outside epoch data range."))
        }

        if epoch.fract() == 0.0 {
            return Ok(self.epochs.get(epoch_index).unwrap().clone());
        }

        let interpolation_range = epoch_index - min_before..epoch_index + min_after;
        let interpolation_data = &self.epochs[interpolation_range];

        let mut result_position = Vector3::new(0.0, 0.0, 0.0);

        // Lagrange interpolation for SP3 orbit data
        let n = interpolation_data.len();
        for i in 0..n {
            let orbit_i = &interpolation_data[i];
            let mut li = 1.0_f64;

            // Compute the lagrange basis polynomial l_i(t)
            for j in 0..n {
                if i != j {
                    let orbit_j = &interpolation_data[j];
                    let delta_t = orbit_i.epoch - orbit_j.epoch;
                    li *= (epoch - orbit_j.epoch) / delta_t;
                }
            }

            result_position[0] += li * orbit_i.x;
            result_position[1] += li * orbit_i.y;
            result_position[2] += li * orbit_i.z;
        }

        info!("{} at {} ({}): {}", self.sv, epoch, result_position, self.source);


        Ok(Orbit {
            epoch,
            x: result_position[0],
            y: result_position[1],
            z: result_position[2],
            clock_offset: self.get_clock_at(epoch),
            sv:Some(self.sv.clone()),
            source:Some(self.source.clone()),
            ac:Some(self.ac.clone()),
            solution:Some(self.solution.clone()),
            solution_time:Some(self.solution_time.clone()),
            frame:self.frame.clone()
        })
    }
}

// impl  ArchivedOrbits {

//     fn get_orbit(&self, epoch:f64) -> Option<Orbit> {
//         let a = self.epochs.get(0);

//         if a.is_some() {
//             let a = a.unwrap();
//             let a = rkyv::deserialize::<Orbit, rkyv::rancor::Error>(a);
//             if a.is_ok() {
//                 return Some(a.unwrap());
//             }
//         }

//         return None;
//     }

//     pub fn from_bytes(bytes: &bytes::Bytes) -> Result<&ArchivedOrbits, rkyv::rancor::Error> {
//         let archived = rkyv::access::<ArchivedOrbits, rkyv::rancor::Error>(&bytes[..]).unwrap();
//         return Ok(archived);
//     }
// }

// #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
// pub struct Clock {
//     epoch:f64,
//     clock:f64,
// }

// #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
// pub struct Clocks {
//     sv:String,
//     source:String,
//     epochs:Vec<Clock>
// }

// impl  Clocks
// {
//     // fn serialize(&self) -> Result<bytes::Bytes, rkyv::rancor::Error> {
//     //     let _bytes = rkyv::to_bytes::<rkyv::rancor::Error>(self).unwrap();
//     //     let bytes = Bytes::from(_bytes.to_vec());
//     //     return Ok(bytes)
//     // }

//     // fn archive_from_bytes(bytes: &bytes::Bytes) -> Result<&ArchivedClocks, rkyv::rancor::Error> {
//     //     let archived = rkyv::access::<ArchivedClocks, rkyv::rancor::Error>(&bytes[..]).unwrap();
//     //     return Ok(archived);
//     // }
// }

#[restate_sdk::object]
pub trait SV {
    #[name = "updateOrbits"]
    async fn update_orbits(orbits:Json<Orbits>) -> Result<(), HandlerError>;

    // #[name = "updateClocks"]
    // async fn update_clocks(clocks:Json<Clocks>) -> Result<(), HandlerError>;

    #[shared]
    #[name = "getOrbit"]
    async fn get_orbit(epoch:f64) -> Result<Json<Option<Orbit>>, HandlerError>;
    //#[shared]
    //async fn clock(request:Json<ClockRequest>) -> Result<Json<Clock>, HandlerError>;
}

pub struct SVImpl;


impl SV for SVImpl {
    async fn update_orbits(&self, ctx: ObjectContext<'_>, orbits:Json<Orbits>) -> Result<(), HandlerError> {

        let sv = ctx.key();
        let new_orbits = orbits.into_inner();
        let orbits_key = new_orbits.get_key();

        let current_orbits = ctx.get::<Json<Orbits>>(&orbits_key).await?;
        if current_orbits.is_some() {
            let current_orbits = current_orbits.unwrap().into_inner();
            if current_orbits.solution_time >= new_orbits.solution_time {
                info!("new {} ({}) orbits older than existing orbits: {} >= {}", sv, orbits_key, current_orbits.solution_time, new_orbits.solution_time);
                return Ok(());
            }
        }

        ctx.set(&orbits_key, Json(new_orbits));

        Ok(())
    }

    // async fn update_clocks(&self, _ctx: ObjectContext<'_>, clock:Json<Clocks>) -> Result<(), HandlerError> {
    //     let clock = clock.into_inner();
    //     println!("{:?}", clock);
    //     Ok(())
    // }

    async fn get_orbit(&self, ctx: SharedObjectContext<'_>, epoch:f64) -> Result<Json<Option<Orbit>>, HandlerError> {
        let orbits = ctx.get::<Json<Orbits>>("ORBITS_COD_ULT").await?.unwrap().into_inner();
        //let orbits = ArchivedOrbits::from_bytes(&bytes)?;
        let orbit = orbits.get_orbit_at(epoch)?;
        return Ok(Json(Some(orbit)));
    }

    // async fn clock(&self, _ctx: SharedObjectContext<'_>, request:Json<ClockRequest>) -> Result<Json<Clock>, HandlerError> {

    //     let cr = &request.into_inner();
    //     let epoch = cr.epoch;

    //     if cr.clock.is_none() {
    //         return  Err(HandlerError::from(TerminalError::new("unable to read data")));
    //     }


    //     let clock = cr.clock.unwrap();

    //     Ok(Json::from(Clock {sv:_ctx.key().to_string(), epoch, clock}))
    // }
}
