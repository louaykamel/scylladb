#![warn(missing_docs)]
//! Scylla
/// Scylla application module
mod application;

/// Access traits and helpers for constructing and executing queries
pub mod access;
/// Cluster application
pub mod cluster;
/// Node application which manages scylla nodes
pub mod node;
/// The ring, which manages scylla access
pub mod ring;
/// The stage application, which handles sending and receiving scylla requests
pub mod stage;
/// Workers which can be used when sending requests to handle the responses
pub mod worker;

pub use application::*;
use log::*;

pub use worker::{
    Worker,
    WorkerError,
};
