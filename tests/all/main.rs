//! vlpds conformance suite, built as ONE test binary.
//!
//! Every file here used to be its own `tests/*.rs` integration test, which
//! linked the whole crate once per file. They are modules of this binary now,
//! so a full run is a single link. Filter by module:
//! `cargo test --test all crud::` (see tests/STATUS.md).

mod common;

mod account_deactivation;
mod account_races;
mod admin_cluster;
mod account_status;
mod account;
mod app_passwords;
mod auth;
mod auth_caches;
mod blob_deletes;
mod blob_gc_race;
mod blobs;
mod cbor_transcode;
mod create_post;
mod crud;
mod differential_shrike;
mod e2e_regressions;
mod email_flows;
mod file_uploads;
mod firehose_backfill;
mod firehose_fanout;
mod firehose_shards;
mod firehose_startup;
mod get_blocks_index;
mod go_checker;
mod ha_auth;
mod ha_liveness;
mod handle_validation;
mod handles;
mod harness;
mod internal_auth;
mod interop_crypto;
mod interop_data_model;
mod interop_mst;
mod interop_syntax;
mod invertible_ops;
mod invite_codes;
mod invites_optional;
mod lexicons;
mod list_repos_scale;
mod log_pipeline;
mod log_retention;
mod migration;
mod moderation;
mod oauth;
mod oauth_replay_durable;
mod preferences;
mod proxy;
mod proxy_fast_path;
mod races;
mod rebalance_handback;
mod rate_limits;
mod record_encode;
mod revocation_gc;
mod segment_bytes;
mod sequencer;
mod server_basics;
mod service_auth;
mod shard_ingest;
mod subscribe_repos;
mod sync_list;
mod sync;
mod sync11_property;
mod takedown_routes;
mod totp;
mod untrusted_repo_data;
