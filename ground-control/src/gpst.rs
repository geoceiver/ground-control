use hifitime::Epoch;
use restate_sdk::errors::HandlerError;


pub fn gpst_week(epoch:&Epoch) -> u32 {
    let gpst_week = (epoch.to_gpst_days() / 7.0).floor() as u32;
    gpst_week
}

// async so it can be wraped in a restate run closure
pub async fn current_gpst_seconds() -> Result<f64, HandlerError> {
    Ok(Epoch::now()?.to_gpst_seconds())
}

// async so it can be wraped in a restate run closure
pub async fn current_gpst_week() -> Result<u32, HandlerError> {
    Ok(gpst_week(&Epoch::now()?))
}
