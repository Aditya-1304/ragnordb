//! durable storage model for Raft replicas hosted in the shared WAL
//!
//! the codec layer defines stable bytes and identity validation. Later slices
//! build replay views, persistence, and public Raft storage adapters on top of
//! these records without creating a second write-through persistence path

pub mod adapter;
pub mod codec;
pub mod frontier;
pub mod persistence;
pub mod recovery;
pub mod view;
