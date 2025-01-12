#![forbid(unsafe_code)]
#![deny(
    clippy::all,
    clippy::as_conversions,
    clippy::float_arithmetic,
    clippy::arithmetic_side_effects,
    clippy::must_use_candidate
)]
#![warn(clippy::todo, clippy::dbg_macro)]

use monitor::config::Config;
use monitor::Monitor;
use utils::config::read_config_file;
use utils::tracing::setup_tracing;

use anyhow::Result;
use camino::Utf8PathBuf;
use clap::Parser;
use tracing::debug;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Debug, clap::Parser)]
struct Opt {
    #[clap(long)]
    config: Utf8PathBuf,
}

// fn main() -> Result<()> {
//     let opt = Opt::parse();

//     setup_tracing();

//     let config: Config = read_config_file(&opt.config)?;

//     debug!(?config);

//     run(config)
// }

// #[allow(clippy::redundant_async_block)] // FIXME
// #[tokio::main]
// async fn run(config: Config) -> Result<()> {
//     Monitor::run(config).await
// }
