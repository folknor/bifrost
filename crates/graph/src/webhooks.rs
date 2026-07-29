//! Microsoft Graph change notification subscription CRUD.

use serde::{Deserialize, Serialize};

use crate::error::GraphError;

use super::client::GraphClient;

const DEFAULT_EXPIRATION_MINUTES: u32 = 1440;
const MAX_EXPIRATION_MINUTES: u32 = 4230;
const CLIENT_STATE_BYTES: usize = 16;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphSubscription {
    change_type: String,
    notification_url: String,
    resource: String,
    expiration_date_time: String,
    client_state: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SubscriptionResponse {
    pub(crate) id: String,
    pub(crate) expiration_date_time: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RenewSubscriptionRequest {
    expiration_date_time: String,
}

pub(crate) async fn create_subscription(
    client: &GraphClient,
    resource: &str,
    notification_url: &str,
    client_state: Option<&str>,
    expiration_minutes: Option<u32>,
) -> Result<SubscriptionResponse, GraphError> {
    let minutes = expiration_minutes
        .unwrap_or(DEFAULT_EXPIRATION_MINUTES)
        .min(MAX_EXPIRATION_MINUTES);

    let body = GraphSubscription {
        change_type: "created,updated,deleted".to_string(),
        notification_url: notification_url.to_string(),
        resource: resource.to_string(),
        expiration_date_time: compute_expiry_iso8601(minutes),
        client_state: Some(match client_state {
            Some(client_state) => client_state.to_string(),
            None => generate_client_state()?,
        }),
    };

    let response: SubscriptionResponse = client.post("/subscriptions", &body).await?;
    tracing::info!(
        "[Graph webhooks] Created subscription {} for resource '{}' (expires {})",
        response.id,
        resource,
        response.expiration_date_time
    );
    Ok(response)
}

pub(crate) async fn renew_subscription(
    client: &GraphClient,
    subscription_id: &str,
    expiration_minutes: Option<u32>,
) -> Result<String, GraphError> {
    let minutes = expiration_minutes
        .unwrap_or(DEFAULT_EXPIRATION_MINUTES)
        .min(MAX_EXPIRATION_MINUTES);
    let new_expiry = compute_expiry_iso8601(minutes);
    let body = RenewSubscriptionRequest {
        expiration_date_time: new_expiry.clone(),
    };

    client
        .patch(&format!("/subscriptions/{subscription_id}"), &body)
        .await?;
    tracing::info!(
        "[Graph webhooks] Renewed subscription {subscription_id} (new expiry: {new_expiry})"
    );
    Ok(new_expiry)
}

pub(crate) fn subscription_is_gone(error: &GraphError) -> bool {
    matches!(error, GraphError::Response(response) if response.status == reqwest::StatusCode::NOT_FOUND || response.status == reqwest::StatusCode::GONE)
}

pub(crate) async fn delete_subscription(
    client: &GraphClient,
    subscription_id: &str,
) -> Result<(), GraphError> {
    let server_result = client
        .delete(&format!("/subscriptions/{subscription_id}"))
        .await;
    if let Err(error) = server_result {
        if !is_not_found(&error) {
            return Err(error);
        }
        tracing::info!(
            "[Graph webhooks] Subscription {subscription_id} already gone on server (404)"
        );
    }

    tracing::info!("[Graph webhooks] Deleted subscription {subscription_id}");
    Ok(())
}

fn generate_client_state() -> Result<String, GraphError> {
    let mut buf = [0u8; CLIENT_STATE_BYTES];
    getrandom::fill(&mut buf).map_err(|error| {
        // RNG failure is a host-environment problem, not a Graph
        // contract violation. Surface it as a transport "Network"
        // failure with `transmission_state: Unsent` so the recovery
        // mapping classifies it as a retryable client-side issue
        // (engine reopens the account); we never sent a byte.
        GraphError::Net(bifrost_net::Error::Network {
            message: format!("RNG failed: {error}"),
            transmission_state: bifrost_types::TransmissionState::Unsent,
            source: None,
        })
    })?;
    Ok(hex_encode(&buf))
}

fn is_not_found(error: &GraphError) -> bool {
    matches!(
        error,
        GraphError::Response(response) if response.status == reqwest::StatusCode::NOT_FOUND
    )
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{byte:02x}");
    }
    s
}

fn compute_expiry_iso8601(minutes: u32) -> String {
    let secs = now_unix() + i64::from(minutes) * 60;
    unix_to_iso8601(secs)
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .cast_signed()
}

fn unix_to_iso8601(secs: i64) -> String {
    const SECS_PER_DAY: i64 = 86400;
    let days = secs.div_euclid(SECS_PER_DAY);
    let day_secs = secs.rem_euclid(SECS_PER_DAY);

    let hours = day_secs / 3600;
    let minutes = (day_secs % 3600) / 60;
    let seconds = day_secs % 60;

    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    format!("{y:04}-{m:02}-{d:02}T{hours:02}:{minutes:02}:{seconds:02}Z")
}

fn parse_iso8601_to_unix(s: &str) -> i64 {
    let Some(s) = s.strip_suffix('Z') else {
        return 0;
    };
    let s = if let Some(dot_pos) = s.rfind('.') {
        &s[..dot_pos]
    } else {
        s
    };

    let parts: Vec<&str> = s.split('T').collect();
    if parts.len() != 2 {
        return 0;
    }

    let date_parts: Vec<i64> = parts[0].split('-').filter_map(|p| p.parse().ok()).collect();
    let time_parts: Vec<i64> = parts[1].split(':').filter_map(|p| p.parse().ok()).collect();

    if date_parts.len() != 3 || time_parts.len() != 3 {
        return 0;
    }

    let (y, m, d) = (date_parts[0], date_parts[1], date_parts[2]);
    let (hh, mm, ss) = (time_parts[0], time_parts[1], time_parts[2]);
    let y_adj = if m <= 2 { y - 1 } else { y };
    let era = if y_adj >= 0 { y_adj } else { y_adj - 399 } / 400;
    let yoe = y_adj - era * 400;
    let m_adj = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * m_adj + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;

    days * 86400 + hh * 3600 + mm * 60 + ss
}

pub(crate) fn is_expiring_soon(expiration_iso: &str, threshold_minutes: i64) -> bool {
    let expiry = parse_iso8601_to_unix(expiration_iso);
    let remaining = expiry - now_unix();
    remaining < threshold_minutes * 60
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscription_request_serialization() {
        let sub = GraphSubscription {
            change_type: "created,updated,deleted".to_string(),
            notification_url: "https://example.com/notify".to_string(),
            resource: "/me/messages".to_string(),
            expiration_date_time: "2024-06-15T12:00:00Z".to_string(),
            client_state: Some("abc123".to_string()),
        };

        let json = serde_json::to_string(&sub).expect("should serialize");
        assert!(json.contains("\"changeType\":\"created,updated,deleted\""));
        assert!(json.contains("\"notificationUrl\":\"https://example.com/notify\""));
        assert!(json.contains("\"resource\":\"/me/messages\""));
        assert!(json.contains("\"clientState\":\"abc123\""));
    }

    #[test]
    fn subscription_response_deserialization() {
        let json = r#"{
            "id": "sub-123",
            "expirationDateTime": "2024-06-15T12:00:00Z",
            "clientState": "secret123"
        }"#;

        let resp: SubscriptionResponse = serde_json::from_str(json).expect("should deserialize");
        assert_eq!(resp.id, "sub-123");
        assert_eq!(resp.expiration_date_time, "2024-06-15T12:00:00Z");
    }

    #[test]
    fn iso_roundtrip_parse() {
        let iso = unix_to_iso8601(1_718_450_400);
        assert_eq!(parse_iso8601_to_unix(&iso), 1_718_450_400);
    }

    #[test]
    fn hex_encode_lowercase() {
        assert_eq!(hex_encode(&[0x0a, 0xff]), "0aff");
    }

    #[test]
    fn renewal_threshold_compares_remaining_minutes() {
        // The worker wakes every 10 min and renews inside a 30 min
        // threshold; a subscription with 45 min left must not be renewed,
        // one with 15 min left must be.
        assert!(!is_expiring_soon(&compute_expiry_iso8601(45), 30));
        assert!(is_expiring_soon(&compute_expiry_iso8601(15), 30));
    }

    #[test]
    fn already_expired_subscription_is_expiring_soon() {
        assert!(is_expiring_soon("1970-01-01T00:00:01Z", 30));
    }

    #[test]
    fn fractional_seconds_expiry_parses_like_the_whole_second_form() {
        // Graph emits 7-digit fractional seconds on `expirationDateTime`.
        assert_eq!(
            parse_iso8601_to_unix("2099-01-01T00:00:00.0000000Z"),
            parse_iso8601_to_unix("2099-01-01T00:00:00Z")
        );
        assert!(!is_expiring_soon("2099-01-01T00:00:00.0000000Z", 30));
    }

    /// Documents current behavior, not an endorsement.
    /// `parse_iso8601_to_unix` has no error channel: a value it cannot read
    /// collapses to epoch 0, which `is_expiring_soon` then reports as "long
    /// expired". That direction is safe (renew), but it renews on every
    /// tick forever rather than reporting the bad value once.
    #[test]
    fn unreadable_expiry_reads_as_epoch_zero_and_forces_renewal() {
        for bad in ["", "2099-01-01", "not-a-timestamp", "2099-01-01T00:00"] {
            assert_eq!(parse_iso8601_to_unix(bad), 0, "{bad}");
            assert!(is_expiring_soon(bad, 30), "{bad}");
        }
    }

    #[test]
    fn numeric_utc_offsets_are_rejected_instead_of_silently_misread() {
        let utc = parse_iso8601_to_unix("2026-01-01T10:00:00Z");
        assert_ne!(utc, 0);
        assert_eq!(parse_iso8601_to_unix("2026-01-01T10:00:00+00:00"), 0);
        assert_eq!(parse_iso8601_to_unix("2026-01-01T10:00:00+05:00"), 0);
        assert_eq!(parse_iso8601_to_unix("2026-01-01T10:00:00-05:00"), 0);
    }

    #[test]
    fn iso_round_trip_spans_leap_days_and_year_boundaries() {
        for iso in [
            "2024-02-29T23:59:59Z",
            "2000-02-29T00:00:00Z",
            "2026-12-31T23:59:59Z",
            "2027-01-01T00:00:00Z",
        ] {
            assert_eq!(unix_to_iso8601(parse_iso8601_to_unix(iso)), iso);
        }
    }

    #[test]
    fn requested_expiry_is_the_minutes_offset_from_now() {
        // `create_subscription` / `renew_subscription` clamp their argument
        // to `MAX_EXPIRATION_MINUTES` before calling this, so pinning the
        // offset arithmetic pins the clamp's effect too.
        let remaining =
            parse_iso8601_to_unix(&compute_expiry_iso8601(MAX_EXPIRATION_MINUTES)) - now_unix();
        let want = i64::from(MAX_EXPIRATION_MINUTES) * 60;
        assert!(
            (remaining - want).abs() <= 2,
            "remaining {remaining} vs want {want}"
        );

        let default_remaining =
            parse_iso8601_to_unix(&compute_expiry_iso8601(DEFAULT_EXPIRATION_MINUTES)) - now_unix();
        assert!(default_remaining < want);
    }

    /// The legacy endpoint constructor still mints a random fallback when a
    /// caller does not opt into a receiver-owned account-wide clientState.
    /// The configured path is exercised by account construction; this pins
    /// only the fallback's entropy and encoding.
    /// `subscription_is_gone` is the gate on the renewal worker's
    /// recreate path: only a subscription Graph no longer has may be
    /// replaced by a fresh create. A throttle or an auth failure must
    /// stay a plain renewal failure - recreating on those would mint a
    /// duplicate subscription beside one the server still holds.
    #[test]
    fn only_a_vanished_subscription_routes_to_recreation() {
        let gone = |status| {
            GraphError::Response(crate::error::GraphResponseError::from_response(
                status,
                reqwest::header::HeaderMap::new(),
                bytes::Bytes::new(),
            ))
        };
        assert!(subscription_is_gone(&gone(reqwest::StatusCode::NOT_FOUND)));
        assert!(subscription_is_gone(&gone(reqwest::StatusCode::GONE)));
        for still_there in [
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            reqwest::StatusCode::UNAUTHORIZED,
            reqwest::StatusCode::FORBIDDEN,
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        ] {
            assert!(!subscription_is_gone(&gone(still_there)), "{still_there}");
        }
        assert!(!subscription_is_gone(&GraphError::Net(
            bifrost_net::Error::Network {
                message: "connection reset".to_string(),
                transmission_state: bifrost_types::TransmissionState::Unsent,
                source: None,
            }
        )));
    }

    #[test]
    fn each_client_state_is_a_distinct_unexported_secret() {
        let first = generate_client_state().expect("rng");
        let second = generate_client_state().expect("rng");
        assert_eq!(first.len(), CLIENT_STATE_BYTES * 2);
        assert_ne!(first, second);
    }
}
