use crate::error::Error;

/// Error returned by [`Pipeline::execute`](super::Pipeline::execute) and
/// [`Pipeline::execute_dynamic`](super::Pipeline::execute_dynamic).
///
/// Distinguished from [`Error`] because pipeline execution has three
/// failure classes: per-command errors (embedded in the result tuple as
/// `Result<T, Error>`), pipeline-level driver errors (encoding failure
/// that aborts the whole batch), and type-mismatch bugs (internal only).
#[derive(Debug)]
pub enum PipelineError {
    /// The driver task has exited  -  the command channel is closed. Carries
    /// the observed `DriverGone` / `DriverPanicked`, stamped `Unsent` when
    /// the batch never reached the driver and `InFlight` when the driver
    /// owned it and died before answering, so a batch that may have been
    /// written is not reported as one that certainly was not.
    Disconnected(Error),
    /// A pipeline-level error: a local refusal of the whole batch before
    /// anything was submitted or written (a command the session state does
    /// not permit, an encoding failure), or a driver failure that ended the
    /// batch.
    Driver(Error),
    /// Internal error: a consumer returned a type that does not match
    /// the expected downcast target. This indicates a bug in the
    /// pipeline builder's command-to-consumer mapping.
    TypeMismatch {
        /// Zero-based index of the command whose output could not be
        /// downcast.
        index: usize,
    },
}

impl std::fmt::Display for PipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Disconnected(e) => write!(f, "pipeline: driver task disconnected: {e}"),
            Self::Driver(e) => write!(f, "pipeline: driver error: {e}"),
            Self::TypeMismatch { index } => {
                write!(f, "pipeline: type mismatch at command index {index}")
            }
        }
    }
}

impl std::error::Error for PipelineError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Driver(e) | Self::Disconnected(e) => Some(e),
            Self::TypeMismatch { .. } => None,
        }
    }
}
