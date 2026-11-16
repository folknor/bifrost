//! Per-id wire accounting, independent of the consumer's absent-target policy.

/// Request-owned facts. Never infer a conditional write from its response.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct AnswerContext {
    pub(crate) method: &'static str,
    pub(crate) guarded: bool,
    pub(crate) write: bool,
}

/// Every submitted id has one of these five observations. In particular an
/// omission is not a server-declared miss: an Outlook user expects an uncertain
/// write to reconcile rather than disappear as a terminal failure.
///
/// This algebra covers readers that extract one id through `from_parts`. Raw
/// collection iterators on the response types (`created_ids` and kin) bypass
/// it and see the provider's maps as sent.
#[derive(Debug)]
pub(crate) enum IdAnswer<T> {
    Ok(T),
    Refused(crate::Error),
    /// A declared miss, optionally retaining the provider's typed set refusal.
    NotFound(Option<crate::Error>),
    Unanswered,
    /// The id was answered in more than one outcome lane (a success entry and
    /// a refusal, a success and `notFound`, or a refusal and `notFound`). A
    /// provider that contradicts itself proves neither outcome, so this is
    /// uncertain evidence, never success and never the retained refusal: an
    /// Outlook user whose save the server both accepted and refused expects
    /// it to reconcile, not a false "saved" or a failure that invites a
    /// duplicating retry.
    Contradicted(Lanes),
}

/// Which outcome lanes answered a contradicted id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Lanes {
    pub(crate) success: bool,
    pub(crate) refusal: bool,
    pub(crate) not_found: bool,
}

impl std::fmt::Display for Lanes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names = [
            (self.success, "success"),
            (self.refusal, "refusal"),
            (self.not_found, "notFound"),
        ];
        let mut first = true;
        for (_, name) in names.iter().filter(|(present, _)| *present) {
            if !first {
                f.write_str(" and ")?;
            }
            f.write_str(name)?;
            first = false;
        }
        Ok(())
    }
}

impl<T> IdAnswer<T> {
    pub(crate) fn from_parts(
        success: Option<T>,
        refusal: Option<crate::Error>,
        not_found: bool,
    ) -> Self {
        let lanes = Lanes {
            success: success.is_some(),
            refusal: refusal.is_some(),
            not_found,
        };
        if u8::from(lanes.success) + u8::from(lanes.refusal) + u8::from(lanes.not_found) > 1 {
            return Self::Contradicted(lanes);
        }
        if let Some(value) = success {
            Self::Ok(value)
        } else if let Some(error) = refusal {
            if matches!(&error, crate::Error::Set(error) if matches!(error.error_type(), super::set::SetErrorType::NotFound))
            {
                Self::NotFound(Some(error))
            } else {
                Self::Refused(error)
            }
        } else if not_found {
            Self::NotFound(None)
        } else {
            Self::Unanswered
        }
    }

    pub(crate) fn into_result(self, id: &str, context: AnswerContext) -> crate::Result<T> {
        match self {
            Self::Ok(value) => Ok(value),
            Self::Refused(error) => Err(error),
            Self::NotFound(None) => Err(crate::Error::IdNotFound(id.to_owned())),
            Self::NotFound(Some(error)) => Err(error),
            Self::Unanswered => Err(crate::Error::IdUnanswered {
                id: id.to_owned(),
                context,
            }),
            Self::Contradicted(lanes) => Err(crate::Error::IdContradicted {
                id: id.to_owned(),
                context,
                lanes,
            }),
        }
    }
}

/// Correlate a bulk read against submitted identities. Unrelated and id-less
/// rows cannot answer a submitted id. First answer wins; repeated submissions
/// receive the same observation rather than consuming each other's result.
pub(crate) fn read_answers<I, T>(
    requested: &[I],
    list: Vec<T>,
    not_found: &[I],
    identity: impl Fn(&T) -> Option<I>,
) -> Vec<(I, IdAnswer<T>)>
where
    I: Clone + Eq + std::hash::Hash,
    T: Clone,
{
    let mut objects = std::collections::HashMap::new();
    for object in list {
        if let Some(id) = identity(&object) {
            objects.entry(id).or_insert(object);
        }
    }
    let missing: std::collections::HashSet<_> = not_found.iter().collect();
    requested
        .iter()
        .map(|id| {
            (
                id.clone(),
                IdAnswer::from_parts(objects.get(id).cloned(), None, missing.contains(id)),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bulk_reads_correlate_submissions_without_consuming_repeated_ids() {
        let answers = read_answers(
            &["wanted", "gone", "silent", "wanted"],
            vec![
                (Some("unrelated"), 0),
                (None, 1),
                (Some("wanted"), 2),
                (Some("wanted"), 3),
            ],
            &["gone"],
            |row| row.0,
        );
        assert_eq!(answers.len(), 4);
        assert!(matches!(answers[0].1, IdAnswer::Ok((Some("wanted"), 2))));
        assert!(matches!(answers[1].1, IdAnswer::NotFound(_)));
        assert!(matches!(answers[2].1, IdAnswer::Unanswered));
        assert!(matches!(answers[3].1, IdAnswer::Ok((Some("wanted"), 2))));
    }

    #[test]
    fn an_id_answered_in_more_than_one_lane_is_contradicted() {
        let refusal = || Some(crate::Error::NotParsable("x".into()));
        let cases = [
            (
                IdAnswer::from_parts(Some(()), refusal(), false),
                true,
                true,
                false,
            ),
            (
                IdAnswer::from_parts(Some(()), None, true),
                true,
                false,
                true,
            ),
            (
                IdAnswer::from_parts(None, refusal(), true),
                false,
                true,
                true,
            ),
            (
                IdAnswer::from_parts(Some(()), refusal(), true),
                true,
                true,
                true,
            ),
        ];
        for (answer, success, refusal, not_found) in cases {
            let IdAnswer::Contradicted(lanes) = answer else {
                panic!("contradiction expected: {answer:?}");
            };
            assert_eq!(
                lanes,
                Lanes {
                    success,
                    refusal,
                    not_found
                }
            );
        }
        let error = IdAnswer::from_parts(Some(()), None, true)
            .into_result("x", AnswerContext::default())
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "Id x answered as success and notFound in one  response"
        );
    }

    #[cfg(feature = "mail")]
    #[test]
    fn a_set_create_in_both_created_and_not_created_is_not_success() {
        let mut response: crate::core::set::SetResponse<crate::sieve::SieveScript> =
            serde_json::from_value(serde_json::json!({
                "accountId": "a", "newState": "s1",
                "created": {"c0": {"id": "S1"}},
                "notCreated": {"c0": {"type": "forbidden"}}
            }))
            .unwrap();
        assert!(matches!(
            response.created("c0"),
            Err(crate::Error::IdContradicted { .. })
        ));
    }

    #[test]
    fn parse_requires_explicit_absence_instead_of_turning_omission_into_not_found() {
        let mut response: crate::core::parse::ParseResponse<String> =
            serde_json::from_value(serde_json::json!({
                "accountId": "a", "parsed": {"ok": "parsed"},
                "notParsable": ["invalid"], "notFound": ["gone"]
            }))
            .unwrap();
        assert_eq!(
            response
                .parsed(&super::super::id::BlobId::new("ok"))
                .unwrap(),
            "parsed"
        );
        assert!(matches!(
            response.parsed(&super::super::id::BlobId::new("invalid")),
            Err(crate::Error::NotParsable(_))
        ));
        assert!(matches!(
            response.parsed(&super::super::id::BlobId::new("gone")),
            Err(crate::Error::IdNotFound(_))
        ));
        assert!(matches!(
            response.parsed(&super::super::id::BlobId::new("silent")),
            Err(crate::Error::IdUnanswered { .. })
        ));
    }
}
