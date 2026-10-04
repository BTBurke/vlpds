//! The `vlpds` binary's subcommands: clients of a running node over HTTP
//! (`admin`), and offline helpers (`dashboards`, whose JSON the console
//! also serves).

pub mod admin;
pub mod dashboards;
