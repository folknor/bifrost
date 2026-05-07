use crate::core::field::Field;

use super::{Quota, QuotaId};

impl Quota {
    pub fn id(&self) -> Option<&QuotaId> {
        self.id.as_ref()
    }

    pub fn take_id(&mut self) -> QuotaId {
        self.id.take().unwrap_or_else(|| QuotaId::new(""))
    }

    pub fn resource_type(&self) -> Option<&str> {
        self.resource_type.as_deref()
    }

    pub fn used(&self) -> Option<u64> {
        self.used
    }

    pub fn hard_limit(&self) -> Option<u64> {
        self.hard_limit
    }

    pub fn scope(&self) -> Option<&str> {
        self.scope.as_deref()
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub fn types(&self) -> Option<&[String]> {
        self.types.as_deref()
    }

    pub fn warn_limit(&self) -> Option<u64> {
        self.warn_limit.as_value().copied()
    }

    pub fn warn_limit_field(&self) -> &Field<u64> {
        &self.warn_limit
    }

    pub fn soft_limit(&self) -> Option<u64> {
        self.soft_limit.as_value().copied()
    }

    pub fn soft_limit_field(&self) -> &Field<u64> {
        &self.soft_limit
    }

    pub fn description(&self) -> Option<&str> {
        self.description.as_value().map(String::as_str)
    }

    pub fn description_field(&self) -> &Field<String> {
        &self.description
    }
}
