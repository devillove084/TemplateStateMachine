// #![forbid(unsafe_code)]
// #![deny(
//     clippy::all,
//     clippy::as_conversions,
//     clippy::float_arithmetic,
//     clippy::arithmetic_side_effects,
//     clippy::must_use_candidate
// )]
// #![warn(clippy::todo, clippy::dbg_macro)]

// include!("client.rs");
// include!("bench.rs");
// include!("cluster.rs");

// use anyhow::Result;
// use clap::Parser;

// #[global_allocator]
// static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// #[derive(Debug, clap::Parser)]
// struct Opt {
//     #[clap(subcommand)]
//     cmd: Command,
// }

// #[derive(Debug, clap::Subcommand)]
// enum Command {
//     Cluster(cluster::Opt),
//     Client(client::Opt),
//     Bench(bench::Opt),
// }

// fn main() -> Result<()> {
//     let opt = Opt::parse();
//     run(opt)
// }

// #[tokio::main]
// async fn run(opt: Opt) -> Result<()> {
//     match opt.cmd {
//         Command::Cluster(cluster_opt) => cluster::run(cluster_opt)?,
//         Command::Client(client_opt) => client::run(client_opt).await?,
//         Command::Bench(bench_opt) => bench::run(bench_opt).await?,
//     }
//     Ok(())
// }
