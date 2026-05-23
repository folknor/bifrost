use std::any::Any;

use crate::error::Error;

use super::PipelineError;

/// Trait for converting a `Vec<Result<Box<dyn Any + Send>, Error>>`
/// (the driver's per-command results) into a flat, typed tuple.
///
/// Each impl maps a nested type-level list `(C, (B, (A, ())))` to its
/// corresponding flat tuple `(Result<A, Error>, Result<B, Error>, Result<C, Error>)`.
/// The nesting order matches the push order: the first command pushed
/// is the innermost type, and results are extracted in push order from
/// index 0 upward.
///
/// Impls are provided for 0 through 8 commands.
pub(crate) trait UnfoldTuple {
    /// The flat tuple type produced by unfolding.
    type Output;

    /// Consume the results vec and produce the typed tuple.
    ///
    /// Returns `Err(PipelineError::TypeMismatch)` if any `Ok` result
    /// fails to downcast to the expected type. Per-command `Err` values
    /// are preserved as-is in the tuple elements.
    fn unfold(
        results: Vec<Result<Box<dyn Any + Send>, Error>>,
    ) -> Result<Self::Output, PipelineError>;
}

/// Downcast a single result entry, preserving per-command errors.
fn downcast_result<T: Any + Send>(
    result: Result<Box<dyn Any + Send>, Error>,
    index: usize,
) -> Result<Result<T, Error>, PipelineError> {
    match result {
        Ok(boxed) => match boxed.downcast::<T>() {
            Ok(val) => Ok(Ok(*val)),
            Err(_) => Err(PipelineError::TypeMismatch { index }),
        },
        Err(e) => Ok(Err(e)),
    }
}

/// Empty pipeline  -  no commands.
impl UnfoldTuple for () {
    type Output = ();
    fn unfold(results: Vec<Result<Box<dyn Any + Send>, Error>>) -> Result<(), PipelineError> {
        if !results.is_empty() {
            return Err(PipelineError::TypeMismatch { index: 0 });
        }
        Ok(())
    }
}

/// Generate `UnfoldTuple` impls for nested type-level lists up to 8 elements.
///
/// Each invocation maps a nested type `(TN, (... (T0, ())))` to a flat
/// tuple `(Result<T0, Error>, ..., Result<TN, Error>)`.
macro_rules! impl_unfold_tuple {
    ($nested:ty, $count:literal, [$($idx:literal : $T:ident),+ $(,)?]) => {
        impl<$($T: Any + Send),+> UnfoldTuple for $nested {
            type Output = ($(Result<$T, Error>,)+);

            fn unfold(
                results: Vec<Result<Box<dyn Any + Send>, Error>>,
            ) -> Result<Self::Output, PipelineError> {
                if results.len() != $count {
                    return Err(PipelineError::TypeMismatch { index: 0 });
                }
                let mut iter = results.into_iter();
                Ok(($(
                    downcast_result::<$T>(
                        iter.next().ok_or(PipelineError::TypeMismatch { index: $idx })?,
                        $idx,
                    )?,
                )+))
            }
        }
    };
}

// 1 command: Accumulated = (A, ())
impl_unfold_tuple!((A, ()), 1, [0: A]);
// 2 commands: Accumulated = (B, (A, ()))
impl_unfold_tuple!((B, (A, ())), 2, [0: A, 1: B]);
// 3 commands: Accumulated = (C, (B, (A, ())))
impl_unfold_tuple!((C, (B, (A, ()))), 3, [0: A, 1: B, 2: C]);
// 4 commands
impl_unfold_tuple!((D, (C, (B, (A, ())))), 4, [0: A, 1: B, 2: C, 3: D]);
// 5 commands
impl_unfold_tuple!((E, (D, (C, (B, (A, ()))))), 5, [0: A, 1: B, 2: C, 3: D, 4: E]);
// 6 commands
impl_unfold_tuple!((F, (E, (D, (C, (B, (A, ())))))), 6, [0: A, 1: B, 2: C, 3: D, 4: E, 5: F]);
// 7 commands
impl_unfold_tuple!(
    (G, (F, (E, (D, (C, (B, (A, ()))))))),
    7,
    [0: A, 1: B, 2: C, 3: D, 4: E, 5: F, 6: G]
);
// 8 commands
impl_unfold_tuple!(
    (H, (G, (F, (E, (D, (C, (B, (A, ())))))))),
    8,
    [0: A, 1: B, 2: C, 3: D, 4: E, 5: F, 6: G, 7: H]
);
