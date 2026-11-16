use std::time::SystemTime;

use httpdate::HttpDate;

use super::{Header, HeaderName, HeaderValue};
use crate::BoxError;

/// Message `Date` header
///
/// Defined in [RFC2822](https://tools.ietf.org/html/rfc2822#section-3.3)
///
/// A minted `Date` is formatted by the shared `bifrost-mime` formatter (UTC,
/// whole seconds, `+0000`). A caller's own Date spelling is a raw header
/// ([`HeaderValue::new`]) and is emitted byte-for-byte; it is never parsed
/// and rewritten.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Date {
    time: SystemTime,
    formatted: String,
}

impl Date {
    /// Build a `Date` from [`SystemTime`]
    ///
    /// # Errors
    ///
    /// `InvalidInput` when the year falls outside the RFC 5322 range
    /// `1900..=9999`.
    pub fn new(st: SystemTime) -> Result<Self, crate::error::Error> {
        let invalid = || {
            crate::error::Error::InvalidInput(
                "Date year outside the RFC 5322 range 1900..=9999".to_owned(),
            )
        };
        let timestamp = jiff::Timestamp::try_from(st).map_err(|_| invalid())?;
        let formatted = bifrost_mime::format_rfc5322_date(timestamp).map_err(|_| invalid())?;
        Ok(Self {
            time: st,
            formatted,
        })
    }

    /// Get the current date
    ///
    /// Shortcut for `Date::new(SystemTime::now())`
    ///
    /// # Errors
    ///
    /// As [`Date::new`], for a system clock outside the RFC 5322 range.
    pub fn now() -> Result<Self, crate::error::Error> {
        Self::new(crate::time::now())
    }
}

impl Header for Date {
    fn name() -> HeaderName {
        HeaderName::new_from_ascii_str("Date")
    }

    fn parse(s: &str) -> Result<Self, BoxError> {
        let mut s = String::from(s);
        if s.ends_with("+0000") {
            // The httpdate crate expects the `Date` to end in ` GMT`, but email
            // uses `+0000` to indicate UTC, so we crudely fix this issue here.

            s.truncate(s.len() - "+0000".len());
            s.push_str("GMT");
        }

        Ok(Self::new(s.parse::<HttpDate>()?.into())?)
    }

    fn display(&self) -> HeaderValue {
        HeaderValue::structured(Self::name(), self.formatted.clone())
    }
}

impl TryFrom<SystemTime> for Date {
    type Error = crate::error::Error;

    fn try_from(st: SystemTime) -> Result<Self, Self::Error> {
        Self::new(st)
    }
}

impl From<Date> for SystemTime {
    fn from(this: Date) -> SystemTime {
        this.time
    }
}

#[cfg(test)]
mod test {
    use std::time::{Duration, SystemTime};

    use super::Date;
    use crate::message::header::{HeaderName, HeaderValue, Headers};

    #[test]
    fn format_date() {
        let mut headers = Headers::new();

        // Tue, 15 Nov 1994 08:12:31 GMT
        headers
            .set(Date::new(SystemTime::UNIX_EPOCH + Duration::from_secs(784887151)).unwrap())
            .unwrap();

        assert_eq!(
            headers.to_string(),
            "Date: Tue, 15 Nov 1994 08:12:31 +0000\r\n".to_owned()
        );

        // Tue, 15 Nov 1994 08:12:32 GMT
        headers
            .set(Date::new(SystemTime::UNIX_EPOCH + Duration::from_secs(784887152)).unwrap())
            .unwrap();

        assert_eq!(
            headers.to_string(),
            "Date: Tue, 15 Nov 1994 08:12:32 +0000\r\n"
        );
    }

    /// A minted Date outside the RFC 5322 years is refused, never written
    /// with a non-four-digit year.
    #[test]
    fn minted_date_outside_rfc_5322_years_is_refused() {
        // 10000-01-01T00:00:00Z
        let far = SystemTime::UNIX_EPOCH + Duration::from_secs(253_402_300_800);
        assert!(Date::new(far).is_err());
        // 1899-12-31T00:00:00Z
        let early = SystemTime::UNIX_EPOCH - Duration::from_secs(2_209_075_200);
        assert!(Date::new(early).is_err());
        assert!(Date::new(SystemTime::UNIX_EPOCH).is_ok());
    }

    #[test]
    fn parse_date() {
        let mut headers = Headers::new();

        headers
            .insert_raw(HeaderValue::new(
                HeaderName::new_from_ascii_str("Date"),
                "Tue, 15 Nov 1994 08:12:31 +0000".to_owned(),
            ))
            .unwrap();

        assert_eq!(
            headers.get::<Date>(),
            Some(Date::new(SystemTime::UNIX_EPOCH + Duration::from_secs(784887151)).unwrap())
        );

        headers
            .insert_raw(HeaderValue::new(
                HeaderName::new_from_ascii_str("Date"),
                "Tue, 15 Nov 1994 08:12:32 +0000".to_owned(),
            ))
            .unwrap();

        assert_eq!(
            headers.get::<Date>(),
            Some(Date::new(SystemTime::UNIX_EPOCH + Duration::from_secs(784887152)).unwrap())
        );
    }
}
