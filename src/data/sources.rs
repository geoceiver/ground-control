use std::collections::{HashMap, HashSet};
use hifitime::Epoch;
use restate_sdk::{errors::HandlerError, prelude::{ContextClient, ContextReadState, ContextWriteState, ObjectContext, SharedObjectContext}, serde::Json};
use serde_with::serde_as;
use sp3::{prelude::SV, SP3};
use tracing::info;
use crate::{data::cddis::s3_get_gz_object_buffer, objects::sv::{Orbit, Orbits, SVClient}};

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Clone)]
pub struct OrbitSourceSP3 {
    pub path:String,
    pub ac:String,
    pub solution: String,
    pub solution_time: String,
    pub source: String,
    pub collected_at:f64,
    pub sv_coverage:Option<HashSet<String>>
}

impl OrbitSourceSP3 {
    pub fn get_source_key(&self) -> String {
        format!("{}_{}_{}", self.source, self.ac, self.solution)
    }
}

#[serde_as]
#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Clone)]
pub struct Sources {
    #[serde_as(as = "Vec<(_, _)>")]
    pub sp3_sources:HashMap<String, OrbitSourceSP3>
}

#[restate_sdk::object]
pub trait OrbitSource {
    #[name = "processSp3"]
    async fn process_sp3(source_file:Json<OrbitSourceSP3>) -> Result<(), HandlerError>;

    #[name = "updateSp3Sources"]
    async fn update_sp3_sources(source_file:Json<OrbitSourceSP3>) -> Result<(), HandlerError>;

    #[shared]
    async fn sources() -> Result<Json<Sources>, HandlerError>;
}

pub struct OrbitSourceImpl;

impl OrbitSource  for OrbitSourceImpl {

    async fn sources(&self, ctx: SharedObjectContext<'_>) -> Result<Json<Sources>, HandlerError> {
        let sources = ctx.get::<Json<Sources>>("sources").await?.unwrap_or(Json(Sources {sp3_sources:HashMap::new()}));
        Ok(sources)
    }

    async fn update_sp3_sources(&self, ctx: ObjectContext<'_>, source_file:Json<OrbitSourceSP3>) -> Result<(),HandlerError> {

        let source_file = source_file.into_inner();
        let mut sources = ctx.get::<Json<Sources>>("sources").await?.unwrap_or(Json(Sources {sp3_sources:HashMap::new()})).into_inner();

        sources.sp3_sources.insert(source_file.get_source_key(), source_file);
        ctx.set("sources", Json(sources));

        Ok(())
    }

    async fn process_sp3(&self, ctx: ObjectContext<'_>, source_file:Json<OrbitSourceSP3>) -> Result<(), HandlerError> {

        let mut source_file = source_file.into_inner();

        info!("processing: {:?}", source_file);

        let mut sp3_buf_reader = s3_get_gz_object_buffer(source_file.path.as_str()).await?;
        let sp3 = SP3::from_reader(&mut sp3_buf_reader)?;

        if !sp3.has_steady_sampling() {
            // TODO handle invalid SP3 sampling
        }

        let valid_from = sp3.first_epoch().to_gpst_seconds();
        let valid_to = sp3.last_epoch().unwrap().to_gpst_seconds();
        let period = (valid_to - valid_from) / (sp3.total_epochs() - 1) as f64;

        info!("{}: {} - {} ({})", source_file.source, valid_from, valid_to, period);
        //let frame = IAU_EARTH_FRAME;

        let mut sv_clocks:HashMap<(String, Epoch), f64> = HashMap::new();
        if sp3.has_satellite_clock_offset() {
            for (epoch, sv, offset) in sp3.satellites_clock_offset_sec_iter() {
                let sv_id = sv.to_string();
                sv_clocks.insert((sv_id, epoch), offset);
            }
        }


        let mut sv_orbits:HashMap<String, Orbits> = HashMap::new();
        for (epoch, sv, (x, y, z)) in sp3.satellites_position_km_iter() {
            let sv_id = sv.to_string();

            if !sv_orbits.contains_key(&sv_id) {
                let orbit_data = Orbits { sv:sv_id.clone(),
                    ac:source_file.ac.clone(),
                    solution:source_file.solution.clone(),
                    solution_time:source_file.solution_time.clone(),
                    source:source_file.source.clone(),
                    valid_from,
                    valid_to,
                    period,
                    epochs:Vec::new(),
                    collected_at: Epoch::now()?.to_gpst_seconds(),
                    frame:None
                };
                sv_orbits.insert(sv_id.clone(), orbit_data);
            }

            let orbit_data = sv_orbits.get_mut(&sv_id).unwrap();
            let epoch_seconds = epoch.to_gpst_seconds();
            let clock_offset = sv_clocks.remove(&(sv_id, epoch));
            orbit_data.epochs.push(Orbit::new(epoch_seconds, x, y, z, clock_offset));
        }

        let mut sv_coverage:HashSet<String> = HashSet::new();
        for sv in sv_orbits.keys() {
            let orbits = sv_orbits.get(sv).unwrap();
            sv_coverage.insert(sv.clone());
            ctx.object_client::<SVClient>(sv).update_orbits(Json(orbits.clone())).send();
        }

        source_file.sv_coverage = Some(sv_coverage);
        info!("sv coverage: {:?}", &source_file);
        ctx.set("current_coverage", Json(source_file.clone()));

        ctx.object_client::<OrbitSourceClient>("current").update_sp3_sources(Json(source_file.clone())).send();

        Ok(())
    }

}
