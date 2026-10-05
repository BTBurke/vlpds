//! Spaces phase 1 side lane: leak, durability and fuzz harnesses and the
//! sync micro-bench, which drive the space surface over XRPC (see
//! tests/all/common/spaces.rs), and the ported reference space suites
//! (`ref_*`, on the `ref_net` harness). Phase 2's cluster checks are the
//! `cluster_*` modules: split and merge with space rows, revocation across
//! nodes, the leak test across nodes and their peer streams, and takeover
//! mid-burst.

mod accept;
mod bench;
mod check;
mod cluster;
mod cluster_leak;
mod cluster_reshard;
mod durability;
mod fuzz;
mod hooks;
mod leak;
mod ref_client_attestation;
mod ref_net;
mod ref_simplespace;
mod ref_space_auth;
mod ref_space_records;
mod ref_space_scope;
mod ref_space_sync;
