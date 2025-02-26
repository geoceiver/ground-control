use restate_sdk::prelude::*;
//use rkyv::{Archive, Deserialize, Serialize};

#[derive(serde::Serialize, serde::Deserialize,Debug, PartialEq, Clone)]
pub enum Source {
    IGSUltraRapid(String),
    IGSRapid(String),
    IGSFinal(String),
    Geoceiver(String)
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Clone)]
pub enum Interpolation {
    None,
    Geoceiver(String)
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Clone)]
pub struct Orbit {
    epoch:f64,
    x:f64,
    y:f64,
    z:f64,
    source:Source,
    interpolation:Interpolation
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
pub struct Orbits {
    sv:String,
    epochs:Vec<Orbit>
}

impl  Orbits
{
    pub fn new(sv:String) -> Self {
        Orbits {sv, epochs:Vec::new()}
    }

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

    pub fn update_orbit(&mut self, orbit:Orbit) {
        self.epochs.push(orbit);
    }

    pub fn get_orbit(&self, epoch:f64) -> Orbit {
        let o = self.epochs.get(0).unwrap().clone();
        o
    }

    // clean expired items from cache
    fn deorbit() {

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

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
pub struct Clock {
    epoch:f64,
    clock:f64,
    source:Source,
    interpolation:Interpolation
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
pub struct Clocks {
    sv:String,
    epochs:Vec<Clock>
}

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
    #[name = "updateOrbit"]
    async fn update_orbit(orbit:Json<Orbit>) -> Result<(), HandlerError>;

    #[name = "updateClock"]
    async fn update_clock(orbit:Json<Clock>) -> Result<(), HandlerError>;

    #[shared]
    async fn orbit(epoch:f64) -> Result<Json<Option<Orbit>>, HandlerError>;
    //#[shared]
    //async fn clock(request:Json<ClockRequest>) -> Result<Json<Clock>, HandlerError>;
}

pub struct SVImpl;


impl SV for SVImpl {
    async fn update_orbit(&self, _ctx: ObjectContext<'_>, orbit:Json<Orbit>) -> Result<(), HandlerError> {
        let mut orbits_json =_ctx.get::<Json<Orbits>>("orbits").await?;
        let mut orbits;
        if orbits_json.is_none() {
            orbits = Orbits::new("test".to_string());
        }
        else {
            orbits = orbits_json.unwrap().into_inner();
        }

        orbits.update_orbit(orbit.into_inner());
        _ctx.set("orbit", Json(orbits));

        //println!("{}", orbit);
        Ok(())
    }

    async fn update_clock(&self, _ctx: ObjectContext<'_>, clock:Json<Clock>) -> Result<(), HandlerError> {
        let clock = clock.into_inner();
        println!("{:?}", clock);
        Ok(())
    }

    async fn orbit(&self, _ctx: SharedObjectContext<'_>, epoch:f64) -> Result<Json<Option<Orbit>>, HandlerError> {

        let orbits = _ctx.get::<Json<Orbits>>("orbits").await?.unwrap().into_inner();
        //let orbits = ArchivedOrbits::from_bytes(&bytes)?;
        let orbit = orbits.get_orbit(epoch);
        return Ok(Json::from(Some(orbit)))
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
