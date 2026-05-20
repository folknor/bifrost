/// A UID range (e.g. `1:100`, or a single UID `42`)
/// (RFC 3501 Section 9 / RFC 4315 Section 2.1).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct UidRange {
    /// First UID in this range (RFC 3501 Section 9 / RFC 4315 Section 2.1).
    pub start: u32,
    /// `None` means a single UID (not a range) (RFC 3501 Section 9 / RFC 4315 Section 2.1).
    pub end: Option<u32>,
}

impl UidRange {
    /// Create a single-UID range.
    ///
    /// # Panics (debug builds only)
    /// Panics if `uid` is 0  -  UIDs are `nz-number` per RFC 3501 Section 9.
    pub const fn single(uid: u32) -> Self {
        debug_assert!(
            uid != 0,
            "UID must be non-zero (RFC 3501 Section 9: uniqueid = nz-number)"
        );
        Self {
            start: uid,
            end: None,
        }
    }

    /// Create an inclusive UID range.
    ///
    /// # Panics (debug builds only)
    /// Panics if `start` or `end` is 0  -  UIDs are `nz-number` per RFC 3501 Section 9.
    pub const fn range(start: u32, end: u32) -> Self {
        debug_assert!(
            start != 0,
            "UID start must be non-zero (RFC 3501 Section 9: uniqueid = nz-number)"
        );
        debug_assert!(
            end != 0,
            "UID end must be non-zero (RFC 3501 Section 9: uniqueid = nz-number)"
        );
        Self {
            start,
            end: Some(end),
        }
    }

    /// Try to create a single-UID range, returning `None` if `uid` is 0
    /// (RFC 3501 Section 9: uniqueid = nz-number).
    pub const fn try_single(uid: u32) -> Option<Self> {
        if uid == 0 {
            None
        } else {
            Some(Self {
                start: uid,
                end: None,
            })
        }
    }

    /// Try to create an inclusive UID range, returning `None` if `start` or `end` is 0
    /// (RFC 3501 Section 9: uniqueid = nz-number).
    pub const fn try_range(start: u32, end: u32) -> Option<Self> {
        if start == 0 || end == 0 {
            None
        } else {
            Some(Self {
                start,
                end: Some(end),
            })
        }
    }
}
