use super::{SetArguments, SieveScriptCreate, SieveScriptPatch, SieveScriptSet};

macro_rules! sieve_setters {
    ($t:ty) => {
        impl $t {
            pub fn name(&mut self, name: impl Into<String>) -> &mut Self {
                self.name = Some(name.into());
                self
            }

            pub fn blob_id(&mut self, blob_id: impl Into<String>) -> &mut Self {
                self.blob_id = Some(blob_id.into());
                self
            }
        }
    };
}

sieve_setters!(SieveScriptCreate);
sieve_setters!(SieveScriptPatch);

impl SetArguments {
    pub fn on_success_activate_script(&mut self, id: impl Into<String>) -> &mut Self {
        self.on_success_activate_script = Some(format!("#{}", id.into()));
        self
    }

    pub fn on_success_activate_script_id(&mut self, id: impl Into<String>) -> &mut Self {
        self.on_success_activate_script = Some(id.into());
        self
    }

    pub fn on_success_deactivate_script(&mut self, value: bool) -> &mut Self {
        self.on_success_deactivate_script = Some(value);
        self
    }
}

impl SieveScriptSet {
    #[must_use]
    pub fn on_success_activate_script(mut self, id: impl Into<String>) -> Self {
        self.arguments().on_success_activate_script(id);
        self
    }

    #[must_use]
    pub fn on_success_activate_script_id(mut self, id: impl Into<String>) -> Self {
        self.arguments().on_success_activate_script_id(id);
        self
    }

    #[must_use]
    pub fn on_success_deactivate_script(mut self, value: bool) -> Self {
        self.arguments().on_success_deactivate_script(value);
        self
    }
}
