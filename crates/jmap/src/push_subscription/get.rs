use crate::{DataType, Get};

use super::{Keys, PushSubscription};

impl PushSubscription<Get> {
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    pub fn take_id(&mut self) -> String {
        self.id.take().unwrap_or_default()
    }

    pub fn device_client_id(&self) -> Option<&str> {
        self.device_client_id.as_deref()
    }

    pub fn url(&self) -> Option<&str> {
        self.url.as_deref()
    }

    pub fn keys(&self) -> Option<&Keys> {
        self.keys.as_ref()
    }

    pub fn verification_code(&self) -> Option<&str> {
        self.verification_code.as_deref()
    }

    pub fn expires(&self) -> Option<i64> {
        self.expires.map(|v| v.timestamp())
    }

    pub fn types(&self) -> Option<&[DataType]> {
        self.types.as_deref()
    }
}

impl Keys {
    pub fn p256dh(&self) -> Option<Vec<u8>> {
        use base64::{Engine, engine::general_purpose::URL_SAFE};
        URL_SAFE.decode(&self.p256dh).ok()
    }

    pub fn auth(&self) -> Option<Vec<u8>> {
        use base64::{Engine, engine::general_purpose::URL_SAFE};
        URL_SAFE.decode(&self.auth).ok()
    }
}

crate::impl_get_object!(PushSubscription, ());
