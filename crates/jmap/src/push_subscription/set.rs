use crate::{DataType, core::set::from_timestamp};

use super::{Keys, PushSubscriptionCreate, PushSubscriptionPatch};

impl PushSubscriptionCreate {
    pub fn device_client_id(&mut self, device_client_id: impl Into<String>) -> &mut Self {
        self.device_client_id = Some(device_client_id.into());
        self
    }

    pub fn url(&mut self, url: impl Into<String>) -> &mut Self {
        self.url = Some(url.into());
        self
    }

    pub fn verification_code(&mut self, verification_code: impl Into<String>) -> &mut Self {
        self.verification_code = Some(verification_code.into());
        self
    }

    pub fn keys(&mut self, keys: Keys) -> &mut Self {
        self.keys = Some(keys);
        self
    }

    pub fn expires(&mut self, expires: i64) -> &mut Self {
        self.expires = Some(from_timestamp(expires));
        self
    }

    pub fn types(&mut self, types: Option<impl IntoIterator<Item = DataType>>) -> &mut Self {
        self.types = types.map(|s| s.into_iter().collect());
        self
    }
}

impl PushSubscriptionPatch {
    pub fn verification_code(&mut self, verification_code: impl Into<String>) -> &mut Self {
        self.verification_code = Some(verification_code.into());
        self
    }

    pub fn expires(&mut self, expires: i64) -> &mut Self {
        self.expires = Some(from_timestamp(expires));
        self
    }

    pub fn types(&mut self, types: Option<impl IntoIterator<Item = DataType>>) -> &mut Self {
        self.types = types.map(|s| s.into_iter().collect());
        self
    }
}

impl Keys {
    pub fn new(p256dh: &[u8], auth: &[u8]) -> Self {
        use base64::{Engine, engine::general_purpose::URL_SAFE};
        Keys {
            p256dh: URL_SAFE.encode(p256dh),
            auth: URL_SAFE.encode(auth),
        }
    }
}
