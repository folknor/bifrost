use crate::DataType;

use super::{Keys, PushSubscription, PushSubscriptionId};

impl PushSubscription {
    pub(crate) fn id(&self) -> Option<&PushSubscriptionId> {
        self.id.as_ref()
    }

    pub(crate) fn take_id(&mut self) -> PushSubscriptionId {
        self.id
            .take()
            .unwrap_or_else(|| PushSubscriptionId::new(""))
    }

    pub(crate) fn device_client_id(&self) -> Option<&str> {
        self.device_client_id.as_deref()
    }

    pub(crate) fn url(&self) -> Option<&str> {
        self.url.as_deref()
    }

    pub(crate) fn keys(&self) -> Option<&Keys> {
        self.keys.as_ref()
    }

    pub(crate) fn verification_code(&self) -> Option<&str> {
        self.verification_code.as_deref()
    }

    pub(crate) fn expires(&self) -> Option<i64> {
        self.expires.map(|v| v.timestamp())
    }

    pub(crate) fn types(&self) -> Option<&[DataType]> {
        self.types.as_deref()
    }
}

impl Keys {
    pub(crate) fn p256dh(&self) -> Option<Vec<u8>> {
        use base64::{Engine, engine::general_purpose::URL_SAFE};
        URL_SAFE.decode(&self.p256dh).ok()
    }

    pub(crate) fn auth(&self) -> Option<Vec<u8>> {
        use base64::{Engine, engine::general_purpose::URL_SAFE};
        URL_SAFE.decode(&self.auth).ok()
    }
}
