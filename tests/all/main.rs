//! vlpds conformance suite, built as ONE test binary.
//!
//! Every file here used to be its own `tests/*.rs` integration test, which
//! linked the whole crate once per file. They are modules of this binary now,
//! so a full run is a single link. Filter by module:
//! `cargo test --test all crud::` (see tests/STATUS.md).

mod common;

/// `--features bench-jemalloc`: the server's allocator, for benches
/// (CPU per op); the suite otherwise runs on the system allocator.
#[cfg(feature = "bench-jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod account_deactivation;
mod account_races;
mod admin_cli;
mod admin_cluster;
mod account_status;
mod account;
mod app_passwords;
mod auth;
mod auth_caches;
mod backlinks;
mod blob_deletes;
mod blob_gc_race;
mod blobs;
mod bulk_create;
mod cache_caps;
mod cbor_transcode;
mod checkpoint_stall;
mod commit_cpu;
mod cold_start;
mod compaction_polling;
mod cost_defaults;
mod create_post;
mod crud;
mod differential_shrike;
mod e2e_regressions;
mod fast_failover;
mod email_flows;
mod email_2fa;
mod export_limits;
mod smtp_mail;
mod file_uploads;
mod firehose_backfill;
mod feature_levels;
mod firehose_fanout;
mod formats;
mod firehose_shards;
mod firehose_startup;
mod get_blocks_index;
mod go_checker;
mod ha_auth;
mod ha_liveness;
mod handle_validation;
mod handles;
mod identity_races;
mod harness;
mod internal_auth;
mod interop_crypto;
mod interop_data_model;
mod interop_mst;
mod interop_syntax;
mod invertible_ops;
mod invite_codes;
mod invites_optional;
mod join_follow;
mod key_rotation;
mod lexicons;
mod list_repos_scale;
mod log_pipeline;
mod log_retention;
mod migration;
mod moderation;
mod mst_lazy;
mod oauth;
mod ops_metrics;
mod oauth_replay_durable;
mod plc;
mod preferences;
mod proxy;
mod push;
mod proxy_fast_path;
mod races;
mod read_after_write;
mod ref_proxy;
mod rebalance_handback;
mod rate_limit_config;
mod rate_limits;
mod rate_limits_cluster;
mod record_encode;
mod reshard;
mod revocation_gc;
mod revocation_races;
mod secrets_at_rest;
mod segment_bytes;
mod segment_compression;
mod sequencer;
mod shrike_adopt;
mod server_basics;
mod service_auth;
mod signature_faults;
mod shard_ingest;
mod subscribe_repos;
mod sync_list;
mod sync;
mod sync11_property;
mod takedown_routes;
mod totp;
mod untrusted_repo_data;
mod import_limits;
mod user_service_auth;
mod ref_auth;
mod ref_ssrf;
mod ref_invites;
mod ref_moderation;
mod ref_moderator_auth;
mod ref_plc;
mod ref_sync;
mod ref_handles;
mod ref_repo;
mod ref_account;
