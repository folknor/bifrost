use serde::Serialize;

use super::headers::{find_header_values_case_insensitive, unfold_header_value};
use super::types::GmailHeader;

const DEFAULT_AUTHSERV_IDS: &[&str] = &["mx.google.com", "google.com"];

/// Individual authentication mechanism result.
#[derive(Debug, Clone, Serialize)]
pub struct AuthVerdict {
    pub result: String,
    pub detail: Option<String>,
}

/// Aggregate authentication result (SPF + DKIM + DMARC).
#[derive(Debug, Clone, Serialize)]
pub struct AuthResult {
    pub spf: AuthVerdict,
    pub dkim: AuthVerdict,
    pub dmarc: AuthVerdict,
    pub aggregate: String,
}

/// Parse email authentication results from message headers.
///
/// Tries these headers in order:
/// 1. `Authentication-Results`
/// 2. `ARC-Authentication-Results`
/// 3. `Received-SPF` (SPF-only fallback)
///
/// Returns `None` if no authentication headers are found.
pub fn parse_authentication_results(headers: &[GmailHeader]) -> Option<AuthResult> {
    parse_authentication_results_for_authserv(headers, None)
}

/// Parse authentication results, preferring the supplied `authserv-id`.
///
/// When no `authserv-id` is supplied this prefers Gmail's own result when
/// present, then falls back to the first header in top-down order.
pub fn parse_authentication_results_for_authserv(
    headers: &[GmailHeader],
    authserv_id: Option<&str>,
) -> Option<AuthResult> {
    let auth_header = select_authentication_header(headers, "authentication-results", authserv_id);
    let arc_header = auth_header.or_else(|| {
        select_authentication_header(headers, "arc-authentication-results", authserv_id)
    });
    let received_spf = find_header(headers, "received-spf");

    if arc_header.is_none() && received_spf.is_none() {
        return None;
    }

    let mut spf = unknown_verdict();
    let mut dkim = unknown_verdict();
    let mut dmarc = unknown_verdict();

    if let Some(header_value) = arc_header {
        let normalized = normalize_header(header_value);

        if let Some(v) = parse_verdict(&normalized, "spf") {
            spf = v;
        }

        dkim = parse_dkim_verdicts(&normalized);

        if let Some(v) = parse_verdict(&normalized, "dmarc") {
            dmarc = v;
        }
    } else if let Some(header_value) = received_spf
        && let Some(v) = parse_received_spf(header_value)
    {
        spf = v;
    }

    let aggregate = compute_aggregate(&spf, &dkim, &dmarc);

    Some(AuthResult {
        spf,
        dkim,
        dmarc,
        aggregate,
    })
}

fn select_authentication_header<'a>(
    headers: &'a [GmailHeader],
    name: &str,
    authserv_id: Option<&str>,
) -> Option<&'a str> {
    let values = find_header_values_case_insensitive(
        headers,
        name,
        |h| h.name.as_str(),
        |h| h.value.as_str(),
    );
    if values.is_empty() {
        return None;
    }

    if let Some(authserv_id) = authserv_id
        && let Some(value) = values
            .iter()
            .copied()
            .find(|value| header_authserv_id_matches(value, authserv_id))
    {
        return Some(value);
    }

    for authserv_id in DEFAULT_AUTHSERV_IDS {
        if let Some(value) = values
            .iter()
            .copied()
            .find(|value| header_authserv_id_matches(value, authserv_id))
        {
            return Some(value);
        }
    }

    values.first().copied()
}

fn find_header<'a>(headers: &'a [GmailHeader], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|h| h.name.eq_ignore_ascii_case(name))
        .map(|h| h.value.as_str())
}

fn normalize_header(value: &str) -> String {
    unfold_header_value(value)
}

fn header_authserv_id_matches(header_value: &str, expected: &str) -> bool {
    extract_authserv_id(header_value)
        .as_deref()
        .is_some_and(|actual| actual.eq_ignore_ascii_case(expected))
}

fn extract_authserv_id(header_value: &str) -> Option<String> {
    let normalized = normalize_header(header_value);

    for part in normalized.split(';') {
        let token = part.trim();
        if token.is_empty() || token.get(..2).is_some_and(|p| p.eq_ignore_ascii_case("i=")) {
            continue;
        }
        if token.contains('=') {
            return None;
        }
        return token
            .split_whitespace()
            .next()
            .filter(|id| !id.is_empty())
            .map(str::to_string);
    }

    None
}

/// Parse a single `mechanism=result (detail)` pattern from the header.
fn parse_verdict(header_value: &str, mechanism: &str) -> Option<AuthVerdict> {
    let lower = header_value.to_lowercase();
    let mech_lower = mechanism.to_lowercase();

    // Find "mechanism=result"
    let pattern = format!("{mech_lower}=");
    let idx = lower.find(&pattern)?;
    let after = &header_value[idx + pattern.len()..];

    // Extract result word
    let result_word: String = after.chars().take_while(|c| c.is_alphanumeric()).collect();
    if result_word.is_empty() {
        return None;
    }

    // Extract optional parenthetical detail
    let after_result = &after[result_word.len()..].trim_start();
    let detail = if after_result.starts_with('(') {
        after_result
            .get(1..)
            .and_then(|s| s.find(')').map(|end| s[..end].trim().to_string()))
    } else {
        None
    };

    Some(AuthVerdict {
        result: result_word.to_lowercase(),
        detail,
    })
}

/// Parse multiple DKIM results - if any passes, use that one.
fn parse_dkim_verdicts(header_value: &str) -> AuthVerdict {
    let lower = header_value.to_lowercase();
    let mut verdicts = Vec::new();
    let mut search_from = 0;

    while let Some(idx) = lower[search_from..].find("dkim=") {
        let abs_idx = search_from + idx;
        let after = &header_value[abs_idx + 5..];
        let result_word: String = after.chars().take_while(|c| c.is_alphanumeric()).collect();

        if !result_word.is_empty() {
            let after_result = &after[result_word.len()..].trim_start();
            let detail = if after_result.starts_with('(') {
                after_result
                    .get(1..)
                    .and_then(|s| s.find(')').map(|end| s[..end].trim().to_string()))
            } else {
                None
            };
            verdicts.push(AuthVerdict {
                result: result_word.to_lowercase(),
                detail,
            });
        }

        search_from = abs_idx + 5;
    }

    if verdicts.is_empty() {
        return unknown_verdict();
    }

    // If any DKIM result passes, use it
    if let Some(pass) = verdicts.iter().find(|v| v.result == "pass") {
        return pass.clone();
    }

    // Otherwise use the first result
    verdicts.into_iter().next().unwrap_or_else(unknown_verdict)
}

/// Parse `Received-SPF` header as fallback (format: `result (detail) ...`).
fn parse_received_spf(header_value: &str) -> Option<AuthVerdict> {
    let normalized = normalize_header(header_value);
    let trimmed = normalized.trim();

    let result_word: String = trimmed
        .chars()
        .take_while(|c| c.is_alphanumeric())
        .collect();
    if result_word.is_empty() {
        return None;
    }

    let after = trimmed[result_word.len()..].trim_start();
    let detail = if after.starts_with('(') {
        after
            .get(1..)
            .and_then(|s| s.find(')').map(|end| s[..end].trim().to_string()))
    } else {
        None
    };

    Some(AuthVerdict {
        result: result_word.to_lowercase(),
        detail,
    })
}

fn unknown_verdict() -> AuthVerdict {
    AuthVerdict {
        result: "unknown".to_string(),
        detail: None,
    }
}

/// Compute the aggregate verdict from SPF, DKIM, and DMARC results.
fn compute_aggregate(spf: &AuthVerdict, dkim: &AuthVerdict, dmarc: &AuthVerdict) -> String {
    // DMARC pass means aggregate pass.
    if dmarc.result == "pass" {
        return "pass".to_string();
    }

    // DMARC fail means aggregate fail.
    if dmarc.result == "fail" {
        return "fail".to_string();
    }

    // Both SPF and DKIM fail means aggregate fail.
    let spf_failed = spf.result == "fail" || spf.result == "hardfail";
    let dkim_failed = dkim.result == "fail" || dkim.result == "hardfail";
    if spf_failed && dkim_failed {
        return "fail".to_string();
    }

    // All unknown means aggregate unknown.
    if spf.result == "unknown" && dkim.result == "unknown" && dmarc.result == "unknown" {
        return "unknown".to_string();
    }

    // Both SPF and DKIM pass with unknown DMARC means aggregate pass.
    if spf.result == "pass" && dkim.result == "pass" && dmarc.result == "unknown" {
        return "pass".to_string();
    }

    // Mixed results
    "warning".to_string()
}

#[cfg(test)]
mod tests {
    use super::{
        extract_authserv_id, parse_authentication_results,
        parse_authentication_results_for_authserv,
    };
    use crate::types::GmailHeader;

    fn header(name: &str, value: &str) -> GmailHeader {
        GmailHeader {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    #[test]
    fn extracts_arc_authserv_id_after_instance_tag() {
        assert_eq!(
            extract_authserv_id("i=1; mx.google.com; spf=pass").as_deref(),
            Some("mx.google.com")
        );
    }

    #[test]
    fn selects_default_gmail_authentication_results_header() {
        let headers = vec![
            header(
                "Authentication-Results",
                "relay.example; spf=fail dkim=fail dmarc=fail",
            ),
            header(
                "Authentication-Results",
                "mx.google.com; spf=pass dkim=pass dmarc=pass",
            ),
        ];

        let result = parse_authentication_results(&headers).expect("auth result");
        assert_eq!(result.spf.result, "pass");
        assert_eq!(result.dkim.result, "pass");
        assert_eq!(result.dmarc.result, "pass");
        assert_eq!(result.aggregate, "pass");
    }

    #[test]
    fn selects_configured_authserv_id() {
        let headers = vec![
            header(
                "Authentication-Results",
                "mx.google.com; spf=pass dkim=pass dmarc=pass",
            ),
            header(
                "Authentication-Results",
                "corp.example; spf=fail dkim=fail dmarc=fail",
            ),
        ];

        let result = parse_authentication_results_for_authserv(&headers, Some("corp.example"))
            .expect("auth result");
        assert_eq!(result.aggregate, "fail");
    }

    #[test]
    fn falls_back_to_first_authentication_results_header() {
        let headers = vec![
            header(
                "Authentication-Results",
                "relay-a.example; spf=pass dkim=pass dmarc=pass",
            ),
            header(
                "Authentication-Results",
                "relay-b.example; spf=fail dkim=fail dmarc=fail",
            ),
        ];

        let result = parse_authentication_results(&headers).expect("auth result");
        assert_eq!(result.aggregate, "pass");
    }

    #[test]
    fn selects_default_gmail_arc_authentication_results_header() {
        let headers = vec![
            header(
                "ARC-Authentication-Results",
                "i=1; relay.example; spf=fail dkim=fail dmarc=fail",
            ),
            header(
                "ARC-Authentication-Results",
                "i=1; mx.google.com; spf=pass dkim=pass dmarc=pass",
            ),
        ];

        let result = parse_authentication_results(&headers).expect("auth result");
        assert_eq!(result.aggregate, "pass");
    }
}
