use crate::core::field::Field;

use super::{Quota, QuotaId};

impl Quota {
    pub(crate) fn id(&self) -> Option<&QuotaId> {
        self.id.as_ref()
    }

    pub(crate) fn take_id(&mut self) -> QuotaId {
        self.id.take().unwrap_or_else(|| QuotaId::new(""))
    }

    pub(crate) fn resource_type(&self) -> Option<&str> {
        self.resource_type.as_deref()
    }

    pub(crate) fn used(&self) -> Option<u64> {
        self.used
    }

    pub(crate) fn hard_limit(&self) -> Option<u64> {
        self.hard_limit
    }

    pub(crate) fn scope(&self) -> Option<&str> {
        self.scope.as_deref()
    }

    pub(crate) fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub(crate) fn types(&self) -> Option<&[String]> {
        self.types.as_deref()
    }

    pub(crate) fn warn_limit(&self) -> Option<u64> {
        self.warn_limit.as_value().copied()
    }

    pub(crate) fn warn_limit_field(&self) -> &Field<u64> {
        &self.warn_limit
    }

    pub(crate) fn soft_limit(&self) -> Option<u64> {
        self.soft_limit.as_value().copied()
    }

    pub(crate) fn soft_limit_field(&self) -> &Field<u64> {
        &self.soft_limit
    }

    pub(crate) fn description(&self) -> Option<&str> {
        self.description.as_value().map(String::as_str)
    }

    pub(crate) fn description_field(&self) -> &Field<String> {
        &self.description
    }
}
