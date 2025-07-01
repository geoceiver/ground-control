use hifitime::Epoch;

pub fn _gpst_week(epoch:&Epoch) -> u64 {
    (epoch.to_gpst_days() / 7.0).floor() as u64
}

pub fn build_reqwest_client() -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder().use_rustls_tls().pool_max_idle_per_host(0).build()
}
