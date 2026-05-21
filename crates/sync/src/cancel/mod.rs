//! Cancellation taxonomy.
//!
//! Four exits: Drop, `Control::pause`, `Control::checkpoint_now`,
//! `Fatal`. The boundary primitive (see `boundary.rs`) carries the
//! `Run`/`Pause`/`CheckpointNow`/`Stop` request that each worker
//! peeks at the top of every batch iteration.

pub mod boundary;

pub use boundary::{Boundary, BoundaryRequest, BoundaryView};
