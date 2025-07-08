use std::{env, io::BufReader, sync::Arc};
use anise::math::Vector3;
use anyhow::anyhow;
use arrow::compute::kernels::filter;
use arrow_array::{Array, BooleanArray, FixedSizeListArray, Float64Array, RecordBatch, StringArray, UInt64Array};
use arrow_buffer::Buffer;
use arrow_data::ArrayData;
use bytes::Buf;
use flate2::bufread::GzDecoder;
use regex::{Captures, Regex};
use restate_sdk::prelude::*;
use arrow_schema::{DataType, Field, Schema};
use object_store::{aws::{AmazonS3, AmazonS3Builder}, path::Path, ObjectStore};
use sp3::SP3;
use tracing::info;

use crate::product::sv::{DataSource, Orbit, SVOrbitsClient, SVSource};

pub fn r2_cddis_bucket() -> Result<AmazonS3, object_store::Error> {

    let r2_path = env::var("R2_PATH").expect("R2 path is set and a vaild String");
    let r2_region = env::var("R2_REGION").expect("R2 region is set and a vaild String");
    let key = env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID is set and a valid String");
    let secret = env::var("AWS_SECRET_ACCESS_KEY").expect("AWS_ACCESS_KEY_ID is set and a valid String");

    AmazonS3Builder::new()
        .with_url(r2_path)
        .with_region(r2_region)
        .with_access_key_id(key)
        .with_secret_access_key(secret)
        .build()
}

pub fn cddis_filename_parser(path:&str) -> Option<Captures<'_>> {
    let re = Regex::new(r"^(?<AC>.{3})0(?<PROJ>.{3})(?<TYP>.{3})_(?<TIME>[0-9]{11})_(?<PER>.*)_(?<SMP>.*)_(?<CNT>.*)\.(?<FMT>.*)\.gz$").unwrap();
    return re.captures(path);
}

pub fn cddis_path_parser(path:&str) -> Option<Captures<'_>> {
    let re = Regex::new(r"^\/cddis\/(?<WEEK>.{4})\/(?<FILENAME>.*)$").unwrap();
    return re.captures(path);
}

pub struct Sp3Table {
    table_name:String
}

impl Sp3Table {

    pub fn new(table_name:String) -> Sp3Table {
        Sp3Table {table_name: table_name}
    }

    pub async fn load_sp3_file(&self, sp3_file:&Sp3File) -> Result<RecordBatch, HandlerError> {

        let r2_bucket = r2_cddis_bucket()?;
        let response = r2_bucket.get(&Path::from_absolute_path(&sp3_file.archive_path)?).await?;

        let sp3_bytes = response.bytes().await?;
        let sp3_reader = GzDecoder::new(sp3_bytes.reader());
        let mut buffered_sp3_reader = BufReader::new(sp3_reader);

        let sp3_data_result = SP3::from_reader(&mut buffered_sp3_reader);

        if sp3_data_result.is_err() {
            return Err(TerminalError::new(format!("Unable to parse {}", sp3_file.archive_path)).into());
        }

        let sp3_data = sp3_data_result.unwrap();

        let product_run_id = sp3_file.get_product_run_id()?;

        let mut gpst_seconds_vec = vec![];
        let mut product_run_id_vec = vec![];
        let mut satellite_vec = vec![];
        let mut constellation_vec = vec![];
        let mut pos_km_vec = vec![];

        let vec_3d_type = Self::vec_3d_type();

        for (e, sv, _, _, vec_3d) in sp3_data.satellites_position_km_iter() {

            gpst_seconds_vec.push(e.to_gpst_seconds());
            product_run_id_vec.push(product_run_id);
            satellite_vec.push(sv.to_string());
            let constellation = sv.to_string().chars().nth(0).unwrap().to_string();
            constellation_vec.push(constellation);

            // build vec within sequential
            pos_km_vec.push(vec_3d.0);
            pos_km_vec.push(vec_3d.1);
            pos_km_vec.push(vec_3d.2);

            //pos_km_data_vec.push(pos_km_vec_data);
        }

        let item_count = gpst_seconds_vec.len();

        let gpst_seconds_array = Arc::new(Float64Array::from(gpst_seconds_vec));
        let product_run_id_array = Arc::new(UInt64Array::from(product_run_id_vec));
        let satellite_array = Arc::new(StringArray::from(satellite_vec));
        let constellation_array = Arc::new(StringArray::from(constellation_vec));

        let pos_km_vec_data_values = ArrayData::builder(DataType::Float64)
            .len(pos_km_vec.len())
            .add_buffer(Buffer::from_slice_ref(&pos_km_vec))
            .build()?;

        let pos_km_vec_data_array = Arc::new(FixedSizeListArray::from(ArrayData::builder(vec_3d_type.clone())
            .len(item_count)
            .add_child_data(pos_km_vec_data_values)
            .build()?));



        let schema = Self::arrow_schema();

        let record_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                gpst_seconds_array,
                product_run_id_array,
                satellite_array,
                constellation_array,
                pos_km_vec_data_array,

            ]
            ).unwrap();

        Ok(record_batch)
    }

    pub fn vec_3d_type() -> DataType {
        DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Float64, false)), 3)
    }

    pub fn arrow_schema() -> Arc<Schema> {

        let vec_3d_type = Self::vec_3d_type();

        let schema = Schema::new(vec![
            Field::new("gpst_seconds", DataType::Float64, false),
            Field::new("product_run_id", DataType::UInt64, false), // unique id for each product run -- IGS uses YYYYDDDHHMM from filename
            Field::new("satellite", DataType::Utf8, false),
            Field::new("constellation", DataType::Utf8, false),
            Field::new("pos_km_vec", vec_3d_type.clone(), false),
            // Field::new("clock_usec", DataType::Float64, true),
            // Field::new("vel_dms_vec", vec_3d_type.clone(), true), // (x,y,z) dm/sec
            // Field::new("clock_roc", DataType::Float64, true), // 10^-4 microseconds/second
        ]);

        Arc::new(schema)
    }
}

#[derive(serde::Serialize, serde::Deserialize,Debug, PartialEq, Clone)]
pub struct Sp3File {
    pub source:String,
    pub archive_path:String
}

impl Sp3File {

    pub fn is_sp3(&self) -> bool {
        let resposne = self.get_product_type();
        resposne.is_ok()
    }

    pub fn get_sampling_resolution(&self) ->  Result<f64, anyhow::Error> {
        match self.source.to_lowercase().as_str() {
            "cddis" => {
                let path_parts = cddis_path_parser(&self.archive_path);
                if let Some(path_parts) = path_parts {
                    let filename = path_parts.name("FILENAME");
                    if let Some(filename) = filename {
                        let filename_parts = cddis_filename_parser(filename.as_str());
                        if let Some (filename_parts) = filename_parts {
                            let sampling_resolution_string = filename_parts.name("SMP").unwrap().as_str().to_lowercase();

                            let (digits, unit) = sampling_resolution_string.split_at_checked(2).unwrap();
                            let sampleing_resolution_digits = digits.parse::<f64>().unwrap();
                            let sampling_resolution_secs;
                            info!("sampleing_resolution_digits {}, sampleing_resolution_unit {}", digits, unit);
                            match unit.chars().last().unwrap() {
                                'd' => {
                                    sampling_resolution_secs = sampleing_resolution_digits * 60.0 * 60.0 * 24.0;
                                }
                                'h' => {
                                    sampling_resolution_secs = sampleing_resolution_digits * 60.0 * 60.0;
                                }
                                'm' => {
                                    sampling_resolution_secs = sampleing_resolution_digits * 60.0;
                                }
                                's' => {
                                    sampling_resolution_secs = sampleing_resolution_digits;
                                }
                                _ => {
                                    sampling_resolution_secs = 0.0;
                                }
                            }

                            return Ok(sampling_resolution_secs);
                        }
                    }
                }
            }
            _ => {

            }
        }

        Err(anyhow!("Unsupported file type: {}", self.archive_path))

    }

    pub fn get_product_type(&self) ->  Result<String, anyhow::Error> {

        match self.source.to_lowercase().as_str() {
            "cddis" => {
                let path_parts = cddis_path_parser(&self.archive_path);
                if let Some(path_parts) = path_parts {
                    let filename = path_parts.name("FILENAME");
                    if let Some(filename) = filename {
                        let filename_parts = cddis_filename_parser(filename.as_str());
                        if let Some (filename_parts) = filename_parts {
                            let product_type_str = filename_parts.name("TYP").unwrap().as_str().to_lowercase();
                            return Ok(product_type_str);
                        }
                    }
                }
            }
            _ => {
                // TODO add global distribution centers
            }
        }

        Err(anyhow!("Unsupported file type: {}", self.archive_path))

    }

    pub fn get_analysis_center(&self) ->  Result<String, anyhow::Error> {

        match self.source.to_lowercase().as_str() {
            "cddis" => {
                let path_parts = cddis_path_parser(&self.archive_path);
                if let Some(path_parts) = path_parts {
                    let filename = path_parts.name("FILENAME");
                    if let Some(filename) = filename {
                        let filename_parts = cddis_filename_parser(filename.as_str());
                        if let Some (filename_parts) = filename_parts {
                            let analysis_center_str = filename_parts.name("AC").unwrap().as_str().to_lowercase();
                            return Ok(analysis_center_str);
                        }
                    }
                }
            }
            _ => {
                // TODO add global distribution centers
            }
        }

        Err(anyhow!("Unsupported file type: {}", self.archive_path))

    }

    pub fn get_product_run_id(&self) -> Result<u64, anyhow::Error> {
        match self.source.to_lowercase().as_str() {
            "cddis" => {
                let path_parts = cddis_path_parser(&self.archive_path);
                if let Some(path_parts) = path_parts {
                    let filename = path_parts.name("FILENAME");
                    if let Some(filename) = filename {
                        let filename_parts = cddis_filename_parser(filename.as_str());
                        if let Some (filename_parts) = filename_parts {
                            let time_str = filename_parts.name("TIME").unwrap().as_str().to_lowercase();
                            let product_run_id = time_str.parse::<u64>();
                            if let Ok(product_run_id) = product_run_id {
                                return Ok(product_run_id);
                            }
                        }
                    }
                }
            }
            _ => {
                // TODO add global distribution centers
            }
        }

        Err(anyhow!("Unsupported file type: {}", self.archive_path))
    }

    pub fn get_table_name(&self) -> Result<String, anyhow::Error> {
        match self.source.to_lowercase().as_str() {
            "cddis" => {
                let path_parts = cddis_path_parser(&self.archive_path);
                if let Some(path_parts) = path_parts {
                    info!("path_parts: {:?}", path_parts);
                    let filename = path_parts.name("FILENAME");
                    if let Some(filename) = filename {
                        let filename_parts = cddis_filename_parser(filename.as_str());
                        if let Some (filename_parts) = filename_parts {
                             info!("filename_parts: {:?}", filename_parts);
                            let proj = filename_parts.name("PROJ").unwrap().as_str().to_lowercase();
                            if proj == "ops" {

                                let ac = filename_parts.name("AC").unwrap().as_str().to_lowercase();
                                let typ = filename_parts.name("TYP").unwrap().as_str().to_lowercase();
                                let per = filename_parts.name("PER").unwrap().as_str().to_lowercase();
                                let smp = filename_parts.name("SMP").unwrap().as_str().to_lowercase();
                                let table_name = format!("cddis_sp3_{}_{}_{}_{}", ac, typ, per, smp);

                                return Ok(table_name);
                            }
                        }
                    }
                }
            }
            _ => {
                // TODO add global distribution centers
            }
        }

        Err(anyhow!("Unsupported file type: {}", self.archive_path))
    }
}

#[restate_sdk::object]
pub trait Sp3Data {
    #[name = "processSp3"]
    async fn process_sp3_file(sp_file:Json<Sp3File>) -> Result<(), HandlerError>;
}

pub struct Sp3DataImpl;

impl Sp3Data for Sp3DataImpl {

    async fn process_sp3_file(&self, ctx:ObjectContext<'_>,  sp3_file:Json<Sp3File>) -> Result<(), HandlerError> {

        let sp3_file = sp3_file.into_inner();

        info!("sp3_file {:?}", sp3_file);

        let table_name = sp3_file.get_table_name()?;

        let sp3_table = Sp3Table::new(table_name);
        let record_data = sp3_table.load_sp3_file(&sp3_file).await?;
        //let record_data = ctx.run(||).await?;

        let satellite_array = record_data.column_by_name("satellite")
                .ok_or_else(|| arrow::error::ArrowError::SchemaError("satellite column not found".into()))?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| arrow::error::ArrowError::CastError("Could not cast to StringArray".into()))?;

        // Find all unique satellite values
        let mut unique_satellites:Vec<String> = Vec::new();
        for i in 0..satellite_array.len() {
            let sat = satellite_array.value(i);
            if !unique_satellites.contains(&sat.to_string()) {
                unique_satellites.push(sat.to_string());
            }
        }

        let data_source = DataSource {
            source: sp3_file.source.clone(),
            analysis_center: sp3_file.get_analysis_center().unwrap(),
            product_type: sp3_file.get_product_type().unwrap()
        };

        let current_data_source = data_source.get_key();

        let current_product_run_id:Option<u64> = ctx.get(&current_data_source).await?;
        let product_run_id = sp3_file.get_product_run_id().unwrap();

        // check if current product run id is newer
        if current_product_run_id.is_some() &&
            current_product_run_id.unwrap() > product_run_id {

            info!("skipping sp3 load for {} current product_run_id = {}", sp3_file.archive_path, current_product_run_id.unwrap());
            return Ok(());
        }
        else {
            ctx.set(&current_data_source, product_run_id);
        }

        // Create a batch for each satellite
        //let mut batches_by_satellite = HashMap::new();s
        for satellite in &unique_satellites {

            // Create filter mask for this satellite
            let filter_values: Vec<bool> = (0..satellite_array.len())
                .map(|i| satellite_array.value(i) == satellite)
                .collect();
            let filter_array = BooleanArray::from(filter_values);

            // Apply filter
            let filtered_batch = filter::filter_record_batch(&record_data, &filter_array)?;

            let epoch_array = filtered_batch.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
            let valid_from = epoch_array.value(0);
            let valid_to = epoch_array.value(epoch_array.len() - 1);
            let epoch_vec = epoch_array.values().to_vec();

            let pos_vec3d_km_array = filtered_batch.column(4).as_any().downcast_ref::<FixedSizeListArray>().unwrap();

            let pos_vec3d_km_vec:Vec<Vector3> = pos_vec3d_km_array.iter()
                .map(|a|
                    Vector3::new(a.as_ref().unwrap().as_any().downcast_ref::<Float64Array>().unwrap().value(0),
                                    a.as_ref().unwrap().as_any().downcast_ref::<Float64Array>().unwrap().value(1),
                                    a.as_ref().unwrap().as_any().downcast_ref::<Float64Array>().unwrap().value(2))
                ).collect();

            let sv = SVSource {satellite:satellite.clone(), data_source: data_source.clone()};

            let orbit = Orbit { sv,
                product_run_id,
                sampling_resolution: sp3_file.get_sampling_resolution().unwrap(),
                valid_from,
                valid_to,
                epochs:epoch_vec,
                pos_ecef_km:pos_vec3d_km_vec,
                pos_latlonalt:None,
                clock_usec:None };

            ctx.object_client::<SVOrbitsClient>(orbit.sv.get_key()).update_orbit(Json(orbit)).send();

        }

        ctx.object_client::<SVOrbitsClient>(data_source.get_key()).update_satellites(Json(unique_satellites)).send();

        Ok(())
    }
}
