//! Shared Gmail query encoding for search and stored filter criteria.

use std::time::SystemTime;

use bifrost_types::{
    AccountError, AccountOperation, AttachmentFilter, FilterCondition, Provider, SearchFilter,
    balanced_raw_query,
};

use super::error::{GmailErrorContext, into_account_error};

fn invalid(op: AccountOperation, detail: impl Into<String>) -> AccountError {
    into_account_error(
        crate::Error::invalid_request(op, detail),
        GmailErrorContext::base(op),
    )
}

// Keep the public predicate algebras at the boundary: search has filename and
// label predicates while stored rules also have sizes and arbitrary headers.
// Unifying those public contracts belongs in their owning crate. Here both
// surfaces use one encoder so escaping, grouping and containers cannot drift.
//
// `op` stamps every refusal, so a `SearchMessages` request refused here reports
// `SearchMessages`, not `Search`.
pub(super) fn filter_query(
    filter: &SearchFilter,
    op: AccountOperation,
) -> Result<String, AccountError> {
    let condition = search_condition(filter, op)?;
    condition_query(&condition, op)
}

fn search_condition(
    filter: &SearchFilter,
    op: AccountOperation,
) -> Result<FilterCondition, AccountError> {
    Ok(match filter {
        SearchFilter::From(value) => FilterCondition::From(value.clone()),
        SearchFilter::To(value) => FilterCondition::To(value.clone()),
        SearchFilter::Subject(value) => FilterCondition::Subject(value.clone()),
        SearchFilter::Body(value) => FilterCondition::Body(value.clone()),
        SearchFilter::Attachment(AttachmentFilter::Any) => FilterCondition::HasAttachment(true),
        // Gmail's own attachment-name search (name or extension, word
        // matching), which is what the shared Name form promises.
        SearchFilter::Attachment(AttachmentFilter::Name(name)) => {
            if name.trim().is_empty() {
                return Err(invalid(op, "attachment name search needs a name"));
            }
            FilterCondition::ProviderExpression {
                provider: Provider::Gmail,
                expression: format!("filename:{}", query_term(name)),
            }
        }
        // Gmail has no MIME-type operator; `filename:application/pdf` would
        // be a filename search, so the type form is refused, not faked.
        SearchFilter::Attachment(_) => {
            return Err(into_account_error(
                crate::Error::unsupported(op),
                GmailErrorContext::base(op),
            ));
        }
        // Raw Gmail query, as `provider_query` takes it, parenthesised so an
        // inner OR or a leading `-` stays inside this leaf. The balance check
        // stops an unmatched `)` or quote from escaping the group.
        SearchFilter::ProviderQuery(raw) => {
            let raw = sanitize_term(raw);
            if raw.is_empty() {
                return Err(invalid(op, "provider query leaf is empty"));
            }
            if !balanced_raw_query(&raw) {
                return Err(invalid(
                    op,
                    "provider query leaf has unbalanced parentheses or quotes",
                ));
            }
            FilterCondition::ProviderExpression {
                provider: Provider::Gmail,
                expression: format!("({raw})"),
            }
        }
        SearchFilter::In(container) => FilterCondition::InContainer(container.clone()),
        SearchFilter::Labeled(label) => FilterCondition::ProviderExpression {
            provider: Provider::Gmail,
            expression: format!("label:{}", query_term(&label.0)),
        },
        SearchFilter::DateRange { after, before } => FilterCondition::DateRange {
            after: *after,
            before: *before,
        },
        SearchFilter::And(items) => FilterCondition::And(
            items
                .iter()
                .map(|item| search_condition(item, op))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        SearchFilter::Or(items) => FilterCondition::Or(
            items
                .iter()
                .map(|item| search_condition(item, op))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        SearchFilter::Not(item) => FilterCondition::Not(Box::new(search_condition(item, op)?)),
        _ => {
            return Err(into_account_error(
                crate::Error::unsupported(op),
                GmailErrorContext::base(op),
            ));
        }
    })
}

enum Reduced {
    Constant(bool),
    Condition(FilterCondition),
}

fn reduce(condition: &FilterCondition) -> Reduced {
    match condition {
        FilterCondition::DateRange {
            after: None,
            before: None,
        } => Reduced::Constant(true),
        FilterCondition::And(items) | FilterCondition::Or(items) => {
            let and = matches!(condition, FilterCondition::And(_));
            let mut kept = Vec::new();
            for item in items {
                match reduce(item) {
                    Reduced::Constant(value) if value != and => return Reduced::Constant(value),
                    Reduced::Constant(_) => {}
                    Reduced::Condition(item) => kept.push(item),
                }
            }
            match kept.len() {
                0 => Reduced::Constant(and),
                1 => Reduced::Condition(kept.remove(0)),
                _ => Reduced::Condition(if and {
                    FilterCondition::And(kept)
                } else {
                    FilterCondition::Or(kept)
                }),
            }
        }
        FilterCondition::Not(item) => match reduce(item) {
            Reduced::Constant(value) => Reduced::Constant(!value),
            Reduced::Condition(item) => Reduced::Condition(FilterCondition::Not(Box::new(item))),
        },
        other => Reduced::Condition(other.clone()),
    }
}

pub(super) fn condition_query(
    condition: &FilterCondition,
    op: AccountOperation,
) -> Result<String, AccountError> {
    match reduce(condition) {
        // Gmail has no Boolean literals. A predicate and its complement keep
        // constants explicit without widening the mailbox scope with in:anywhere.
        // Users expect an all/none query to compose with provider terms too.
        Reduced::Constant(true) => Ok("(has:attachment OR -has:attachment)".into()),
        Reduced::Constant(false) => Ok("has:attachment -has:attachment".into()),
        Reduced::Condition(condition) => compile_condition(&condition, op, true),
    }
}

fn nonempty(fragment: String, op: AccountOperation) -> Result<String, AccountError> {
    if fragment.is_empty() {
        Err(invalid(
            op,
            "empty Gmail query fragment or unfolded Boolean constant",
        ))
    } else {
        Ok(fragment)
    }
}

fn compile_condition(
    condition: &FilterCondition,
    op: AccountOperation,
    positive: bool,
) -> Result<String, AccountError> {
    let fragment = match condition {
        FilterCondition::From(value) => Ok(format!("from:{}", query_term(value))),
        FilterCondition::To(value)
        | FilterCondition::Cc(value)
        | FilterCondition::Recipient(value) => Ok(format!("to:{}", query_term(value))),
        FilterCondition::Subject(value) => Ok(format!("subject:{}", query_term(value))),
        FilterCondition::Body(value) => Ok(query_term(value)),
        FilterCondition::HasAttachment(true) => Ok("has:attachment".to_string()),
        FilterCondition::HasAttachment(false) => Ok("-has:attachment".to_string()),
        FilterCondition::InContainer(container) => Ok(container_query(&container.0)),
        FilterCondition::SizeGreaterThan(size) => Ok(format!("larger:{size}")),
        FilterCondition::SizeLessThan(size) => Ok(format!("smaller:{size}")),
        FilterCondition::DateRange { after, before } => {
            let mut parts = Vec::new();
            if let Some(after) = after {
                parts.push(format!(
                    "after:{}",
                    gmail_date(
                        *after,
                        if positive {
                            BoundSide::After
                        } else {
                            BoundSide::Before
                        },
                        op
                    )?
                ));
            }
            if let Some(before) = before {
                parts.push(format!(
                    "before:{}",
                    gmail_date(
                        *before,
                        if positive {
                            BoundSide::Before
                        } else {
                            BoundSide::After
                        },
                        op
                    )?
                ));
            }
            Ok(parts.join(" "))
        }
        FilterCondition::ProviderExpression {
            provider,
            expression,
        } => {
            if *provider == Provider::Gmail {
                nonempty(sanitize_term(expression), op)
            } else {
                Err(invalid(
                    op,
                    "Gmail filters accept only Provider::Gmail expressions; \
                     Provider::Google names the non-mail Google APIs",
                ))
            }
        }
        FilterCondition::And(items) => items
            .iter()
            .map(|item| compile_condition(item, op, positive))
            .collect::<Result<Vec<_>, _>>()
            .map(|items| items.join(" ")),
        FilterCondition::Or(items) => items
            .iter()
            .map(|item| compile_condition(item, op, positive))
            .collect::<Result<Vec<_>, _>>()
            .and_then(|items| nonempty(items.join(" OR "), op))
            .map(|items| format!("({items})")),
        FilterCondition::Not(item) => Ok(format!("-({})", compile_condition(item, op, !positive)?)),
        FilterCondition::HeaderContains { .. } => Err(invalid(
            op,
            "Gmail filters do not support arbitrary header conditions",
        )),
        _ => Err(invalid(op, "unsupported Gmail filter condition")),
    }?;
    nonempty(fragment, op)
}

pub(super) fn sanitize_term(value: &str) -> String {
    value
        .chars()
        .map(|ch| if matches!(ch, '\r' | '\n') { ' ' } else { ch })
        .collect::<String>()
        .trim()
        .to_string()
}

fn query_term(value: &str) -> String {
    let sanitized = sanitize_term(value);
    if sanitized
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '@' | '/' | ':'))
    {
        sanitized
    } else {
        format!(
            "\"{}\"",
            sanitized.replace('\\', "\\\\").replace('"', "\\\"")
        )
    }
}

pub(super) fn container_query(id: &str) -> String {
    super::flags::container_search_operator(id)
        .map(str::to_owned)
        .unwrap_or_else(|| format!("label:{}", query_term(id)))
}

/// Positive range sides: `After` floors and `Before` ceils to whole seconds.
/// The compiler swaps them under odd negation to round inward.
#[derive(Clone, Copy)]
pub(super) enum BoundSide {
    After,
    Before,
}

pub(super) fn gmail_date(
    time: SystemTime,
    side: BoundSide,
    op: AccountOperation,
) -> Result<String, AccountError> {
    // Round outward when asserted, inward beneath odd negation: an enterprise
    // user expects rounding to preserve every real match of the whole query.
    let elapsed = time.duration_since(SystemTime::UNIX_EPOCH).map_err(|_| {
        invalid(
            op,
            "Gmail query date bounds must be on or after the Unix epoch",
        )
    })?;
    let mut secs = elapsed.as_secs();
    if matches!(side, BoundSide::Before) && elapsed.subsec_nanos() != 0 {
        secs = secs
            .checked_add(1)
            .ok_or_else(|| invalid(op, "Gmail query date bound exceeds signed Unix seconds"))?;
    }
    let seconds = i64::try_from(secs)
        .map_err(|_| invalid(op, "Gmail query date bound exceeds signed Unix seconds"))?;
    Ok(seconds.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::ContainerId;

    fn filter_query(filter: &SearchFilter) -> Result<String, AccountError> {
        super::filter_query(filter, AccountOperation::Search)
    }

    #[test]
    fn refusals_name_the_receiving_search_operation() {
        let op = AccountOperation::SearchMessages;
        let pdf = SearchFilter::Attachment(AttachmentFilter::MediaType(
            bifrost_types::MediaTypePattern::parse("application/pdf").unwrap(),
        ));
        // Unsupported carries the operation in its kind and recovery as well
        // as in `operation()`; nesting proves the recursion keeps it.
        let nested = SearchFilter::Or(vec![
            SearchFilter::Subject("a".into()),
            SearchFilter::Not(Box::new(pdf)),
        ]);
        let err = super::filter_query(&nested, op).expect_err("no type operator");
        assert_eq!(err.operation(), Some(op));
        assert_eq!(
            err.kind(),
            &bifrost_types::AccountErrorKind::Unsupported(op)
        );
        assert_eq!(
            err.recovery(),
            &bifrost_types::RecoveryClass::Unsupported(op)
        );
        let malformed = SearchFilter::And(vec![
            SearchFilter::From("x@example.com".into()),
            SearchFilter::Not(Box::new(SearchFilter::ProviderQuery("a) OR (b".into()))),
        ]);
        let err = super::filter_query(&malformed, op).expect_err("unbalanced");
        assert_eq!(err.operation(), Some(op));
        assert!(matches!(
            err.kind(),
            bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
    }

    #[test]
    fn compiler_refuses_unfolded_constants() {
        for condition in [
            FilterCondition::DateRange {
                after: None,
                before: None,
            },
            FilterCondition::And(vec![]),
            FilterCondition::Or(vec![]),
        ] {
            let err = compile_condition(&condition, AccountOperation::Search, true)
                .expect_err("constant must have been folded");
            assert!(matches!(
                err.kind(),
                bifrost_types::AccountErrorKind::Request(
                    bifrost_types::RequestErrorKind::Malformed
                )
            ));
        }
    }

    #[test]
    fn constants_fold_before_gmail_rendering() {
        let all = SearchFilter::DateRange {
            after: None,
            before: None,
        };
        let leaf = SearchFilter::Subject("x".into());
        for constant in [all, SearchFilter::And(vec![])] {
            assert_eq!(
                filter_query(&SearchFilter::Or(vec![constant.clone(), leaf.clone()])).unwrap(),
                "(has:attachment OR -has:attachment)"
            );
            assert_eq!(
                filter_query(&SearchFilter::Not(Box::new(constant))).unwrap(),
                "has:attachment -has:attachment"
            );
        }
        assert_eq!(
            filter_query(&SearchFilter::Or(vec![])).unwrap(),
            "has:attachment -has:attachment"
        );
        assert_eq!(
            filter_query(&SearchFilter::Or(vec![SearchFilter::Or(vec![]), leaf])).unwrap(),
            "subject:x"
        );
    }

    #[test]
    fn negated_dates_round_inward_and_double_negation_outward() {
        let t = SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(131_696_250);
        let range = SearchFilter::DateRange {
            after: Some(t),
            before: Some(t),
        };
        let negated = SearchFilter::Not(Box::new(range));
        assert_eq!(
            filter_query(&negated).unwrap(),
            "-(after:131697 before:131696)"
        );
        assert_eq!(
            filter_query(&SearchFilter::Not(Box::new(negated))).unwrap(),
            "-(-(after:131696 before:131697))"
        );
    }

    #[test]
    fn empty_provider_fragment_is_malformed() {
        let err = condition_query(
            &FilterCondition::ProviderExpression {
                provider: Provider::Gmail,
                expression: " \r\n ".into(),
            },
            AccountOperation::FilterCreate,
        )
        .expect_err("empty fragment");
        assert!(matches!(
            err.kind(),
            bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
    }

    #[test]
    fn date_bounds_keep_time_of_day_on_both_surfaces() {
        let after = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(131_696);
        let before = after + std::time::Duration::from_secs(123);
        let search = SearchFilter::DateRange {
            after: Some(after),
            before: Some(before),
        };
        let rule = FilterCondition::DateRange {
            after: Some(after),
            before: Some(before),
        };
        assert_eq!(filter_query(&search).unwrap(), "after:131696 before:131819");
        assert_eq!(
            condition_query(&rule, AccountOperation::FilterCreate).unwrap(),
            "after:131696 before:131819"
        );
    }

    #[test]
    fn unrepresentable_date_bounds_are_refused() {
        let op = AccountOperation::Search;
        let before_epoch = SystemTime::UNIX_EPOCH - std::time::Duration::from_secs(1);
        assert!(gmail_date(before_epoch, BoundSide::After, op).is_err());
        assert!(gmail_date(before_epoch, BoundSide::Before, op).is_err());
    }

    #[test]
    fn fractional_bounds_round_outward() {
        let op = AccountOperation::Search;
        let t = SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(131_696_250);
        assert_eq!(gmail_date(t, BoundSide::After, op).unwrap(), "131696");
        assert_eq!(gmail_date(t, BoundSide::Before, op).unwrap(), "131697");
        let whole = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(131_696);
        assert_eq!(gmail_date(whole, BoundSide::Before, op).unwrap(), "131696");
        let search = SearchFilter::DateRange {
            after: Some(t),
            before: Some(t),
        };
        assert_eq!(filter_query(&search).unwrap(), "after:131696 before:131697");
    }

    #[test]
    fn search_and_rule_queries_share_nested_encoding() {
        let search = SearchFilter::And(vec![
            SearchFilter::From("Ada Example".into()),
            SearchFilter::Or(vec![
                SearchFilter::In(ContainerId("Archive".into())),
                SearchFilter::Not(Box::new(SearchFilter::Attachment(AttachmentFilter::Any))),
            ]),
        ]);
        let rule = FilterCondition::And(vec![
            FilterCondition::From("Ada Example".into()),
            FilterCondition::Or(vec![
                FilterCondition::InContainer(ContainerId("Archive".into())),
                FilterCondition::Not(Box::new(FilterCondition::HasAttachment(true))),
            ]),
        ]);
        let encoded = filter_query(&search).unwrap();
        assert_eq!(
            encoded,
            condition_query(&rule, AccountOperation::FilterCreate).unwrap(),
        );
        assert_eq!(
            encoded,
            format!(
                "from:\"Ada Example\" ({} OR -(has:attachment))",
                container_query("Archive"),
            ),
        );
    }

    #[test]
    fn attachment_forms_map_to_gmail_operators_or_are_refused() {
        let name = |value: &str| SearchFilter::Attachment(AttachmentFilter::Name(value.into()));
        assert_eq!(
            filter_query(&SearchFilter::Attachment(AttachmentFilter::Any)).unwrap(),
            "has:attachment"
        );
        // No magic word: a file called "attachment" is a name search.
        assert_eq!(
            filter_query(&name("attachment")).unwrap(),
            "filename:attachment"
        );
        assert_eq!(
            filter_query(&SearchFilter::Not(Box::new(name("q3 report")))).unwrap(),
            "-(filename:\"q3 report\")"
        );
        for empty in ["", "  "] {
            let err = filter_query(&name(empty)).expect_err("empty name");
            assert!(matches!(
                err.kind(),
                bifrost_types::AccountErrorKind::Request(
                    bifrost_types::RequestErrorKind::Malformed
                )
            ));
        }
        let pdf = SearchFilter::Attachment(AttachmentFilter::MediaType(
            bifrost_types::MediaTypePattern::parse("application/pdf").unwrap(),
        ));
        assert!(matches!(
            filter_query(&pdf).expect_err("no type operator").kind(),
            bifrost_types::AccountErrorKind::Unsupported(_)
        ));
    }

    #[test]
    fn a_raw_query_leaf_stays_inside_its_group() {
        let search = SearchFilter::And(vec![
            SearchFilter::ProviderQuery("from:a OR from:b".into()),
            SearchFilter::Subject("budget".into()),
        ]);
        assert_eq!(
            filter_query(&search).unwrap(),
            "(from:a OR from:b) subject:budget"
        );
        assert_eq!(
            filter_query(&SearchFilter::Not(Box::new(SearchFilter::ProviderQuery(
                "larger:5M".into()
            ))))
            .unwrap(),
            "-((larger:5M))"
        );
        for bad in ["", " ", "a) OR (b", "\"open"] {
            let err = filter_query(&SearchFilter::ProviderQuery(bad.into())).expect_err(bad);
            assert!(matches!(
                err.kind(),
                bifrost_types::AccountErrorKind::Request(
                    bifrost_types::RequestErrorKind::Malformed
                )
            ));
        }
    }
}
