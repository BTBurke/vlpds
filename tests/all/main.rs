//! vlpds conformance suite, built as ONE test binary.
//!
//! Every file here used to be its own `tests/*.rs` integration test, which
//! linked the whole crate once per file. They are modules of this binary now,
//! so a full run is a single link. Filter by module:
//! `cargo test --test all crud::` (see tests/STATUS.md).

mod common;

mod account_deactivation;
mod admin_cluster;
mod account_status;
mod account;
mod app_passwords;
mod auth;
mod blob_deletes;
mod blobs;
mod cbor_transcode;
mod create_post;
mod crud;
mod email_flows;
mod file_uploads;
mod firehose_backfill;
mod go_checker;
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
mod lexicons;
mod moderation;
mod oauth;
mod preferences;
mod proxy;
mod races;
mod rate_limits;
mod sequencer;
mod server_basics;
mod service_auth;
mod subscribe_repos;
mod sync_list;
mod sync;
mod sync11_property;
mod totp;
mod untrusted_repo_data;
