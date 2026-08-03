use super::{VacationResponse, VacationResponseId};

impl VacationResponse {
    pub(crate) fn id(&self) -> Option<&VacationResponseId> {
        self.id.as_ref()
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.is_enabled.unwrap_or(false)
    }

    pub(crate) fn from_date(&self) -> Option<i64> {
        self.from_date.map(jiff::Timestamp::as_second)
    }

    pub(crate) fn to_date(&self) -> Option<i64> {
        self.to_date.map(jiff::Timestamp::as_second)
    }

    pub(crate) fn subject(&self) -> Option<&str> {
        self.subject.as_deref()
    }

    pub(crate) fn text_body(&self) -> Option<&str> {
        self.text_body.as_deref()
    }

    pub(crate) fn html_body(&self) -> Option<&str> {
        self.html_body.as_deref()
    }
}
