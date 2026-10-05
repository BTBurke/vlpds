//! Spaces phase 1 side lane: leak, durability and fuzz harnesses and the
//! sync micro-bench, which drive the space surface over XRPC (see
//! tests/all/common/spaces.rs).

mod bench;
mod check;
mod durability;
mod fuzz;
mod hooks;
mod leak;
