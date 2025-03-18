mod archiver;
mod queue;
mod r2;
mod cddis;
mod utils;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
}
