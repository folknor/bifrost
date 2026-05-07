use crate::{
    Get, Set,
    core::set::{SetObject, SetObjectCreatable},
};

use super::{SetArguments, SieveScript};

impl SieveScript<Set> {
    pub fn name(&mut self, name: impl Into<String>) -> &mut Self {
        self.name = Some(name.into());
        self
    }

    pub fn blob_id(&mut self, blob_id: impl Into<String>) -> &mut Self {
        self.blob_id = Some(blob_id.into());
        self
    }
}

impl SetObject for SieveScript<Set> {
    type SetArguments = SetArguments;

    fn create_id(&self) -> Option<String> {
        self._create_id.map(|id| format!("c{id}"))
    }
}

impl SetObjectCreatable for SieveScript<Set> {
    fn new(_create_id: Option<usize>) -> Self {
        SieveScript {
            _create_id,
            _state: Default::default(),
            id: None,
            name: None,
            blob_id: None,
            is_active: None,
        }
    }
}

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

impl SetObject for SieveScript<Get> {
    type SetArguments = ();

    fn create_id(&self) -> Option<String> {
        None
    }
}

// -- Lifted method arguments (plans/API.md §5) --

use super::SieveScriptSet;

impl SieveScriptSet {
    pub fn on_success_activate_script(&mut self, id: impl Into<String>) -> &mut Self {
        self.arguments().on_success_activate_script(id);
        self
    }

    pub fn on_success_activate_script_id(&mut self, id: impl Into<String>) -> &mut Self {
        self.arguments().on_success_activate_script_id(id);
        self
    }

    pub fn on_success_deactivate_script(&mut self, value: bool) -> &mut Self {
        self.arguments().on_success_deactivate_script(value);
        self
    }
}
