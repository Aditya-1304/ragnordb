//! Benchmark-only models for comparing experimental MVCC physical layouts.
//!
//! These types deliberately live in the benchmark crate. Their encodings are
//! not production storage formats and must not be treated as durable V1 bytes.

pub mod lsm_layout;
