// use std::net::SocketAddr;
// use std::time::Instant;

// use anyhow::Result;
// use bytes::Bytes;
// use consensus::display::display_bytes;
// use consensus::{RpcClientConfig, utf8};
// use rand::RngCore;
// use serde::Serialize;

// pub fn default_rpc_client_config() -> RpcClientConfig {
//     RpcClientConfig {
//         max_frame_length: 16777216, // 16 MiB
//         op_chan_size: 65536,
//         forward_chan_size: 65536,
//     }
// }

// pub fn random_bytes(size: usize) -> Bytes {
//     let mut buf: Vec<u8> = vec![0; size];
//     rand::thread_rng().fill_bytes(&mut buf);
//     Bytes::from(buf)
// }

// pub fn pretty_json<T: Serialize>(value: &T) -> Result<String> {
//     let mut buf = Vec::new();
//     let formatter = serde_json::ser::PrettyFormatter::with_indent("    ".as_ref());
//     let mut serializer = serde_json::Serializer::with_formatter(&mut buf, formatter.clone());
//     value.serialize(&mut serializer)?;
//     let ans = utf8::vec_to_string(buf)?;
//     Ok(ans)
// }

// #[derive(Debug, clap::Args)]
// pub struct Opt {
//     #[clap(long)]
//     pub server: SocketAddr,

//     #[clap(long)]
//     pub debug_time: bool,

//     #[clap(subcommand)]
//     cmd: Command,
// }

// #[derive(Debug, clap::Subcommand)]
// pub enum Command {
//     GetMetrics {},
//     Get { key: String },
//     Set { key: String, value: String },
//     Del { key: String },
// }

// pub async fn run(opt: Opt) -> Result<()> {
//     let server = {
//         let remote_addr = opt.server;
//         let rpc_client_config = crate::default_rpc_client_config();
//         consensus::ServerForClient::connect(remote_addr, &rpc_client_config).await?
//     };

//     let t0 = opt.debug_time.then(Instant::now);

//     match opt.cmd {
//         Command::GetMetrics { .. } => {
//             let args = consensus::GetMetricsArgs {};
//             let output = server.get_metrics(args).await?;
//             println!("{}", crate::pretty_json(&output)?);
//         }
//         Command::Get { key, .. } => {
//             let args = consensus::GetArgs { key: key.into() };
//             let output = server.get(args).await?;
//             if let Some(val) = output.value {
//                 println!("{}", display_bytes(&val))
//             }
//         }
//         Command::Set { key, value, .. } => {
//             let args = consensus::SetArgs {
//                 key: key.into(),
//                 value: value.into(),
//             };
//             let output = server.set(args).await?;
//             let consensus::SetOutput {} = output;
//         }
//         Command::Del { key, .. } => {
//             let args = consensus::DelArgs { key: key.into() };
//             let output = server.del(args).await?;
//             let consensus::DelOutput {} = output;
//         }
//     }

//     let t1 = Instant::now();

//     if let Some(t0) = t0 {
//         #[allow(clippy::arithmetic_side_effects)]
//         let duration = t1 - t0;
//         eprintln!("time: {:?}", duration);
//     }

//     Ok(())
// }
