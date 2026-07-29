/// A UID range (e.g. `1:100`, or a single UID `42`)
/// (RFC 3501 Section 9 / RFC 4315 Section 2.1).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
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

#[cfg(test)]
mod tests {
    use super::UidRange;

    #[test]
    fn single_and_range_carry_the_expected_shape() {
        let single = UidRange::single(7);
        assert_eq!(single.start, 7);
        assert_eq!(single.end, None, "a single UID is not a range");

        let range = UidRange::range(3, 9);
        assert_eq!(range.start, 3);
        assert_eq!(range.end, Some(9));

        // A one-element range is representable both ways, and the two
        // spellings are NOT equal: `end` distinguishes them.
        assert_ne!(UidRange::range(3, 3), UidRange::single(3));
    }

    // RFC 3501 Section 9: uniqueid = nz-number. The checked constructors
    // are the ones decode paths must use, because the unchecked pair only
    // asserts in debug builds - a release build would silently mint a
    // zero-UID range from corrupt input.
    #[test]
    fn checked_constructors_reject_zero_uids() {
        assert_eq!(UidRange::try_single(0), None);
        assert_eq!(UidRange::try_range(0, 5), None);
        assert_eq!(UidRange::try_range(5, 0), None);
        assert_eq!(UidRange::try_range(0, 0), None);

        assert_eq!(UidRange::try_single(1), Some(UidRange::single(1)));
        assert_eq!(UidRange::try_range(1, 2), Some(UidRange::range(1, 2)));
    }

    #[test]
    fn checked_range_does_not_police_direction() {
        // `try_range` only enforces nz-number, not ordering: a backwards
        // range is the caller's problem (cursor decode rejects it
        // explicitly in `account::envelope`).
        assert_eq!(UidRange::try_range(9, 3), Some(UidRange::range(9, 3)));
    }

    #[test]
    fn default_is_the_degenerate_zero_single() {
        // `Default` exists for struct-update syntax; it is deliberately
        // NOT a valid UID, so nothing may treat it as one.
        let default = UidRange::default();
        assert_eq!(default.start, 0);
        assert_eq!(default.end, None);
        assert_eq!(UidRange::try_single(default.start), None);
    }
}
