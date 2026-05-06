use crate::{
    client::Client,
    core::set::SetObject,
};

use super::{Property, VacationResponse, VacationResponseGet, VacationResponseSet};

impl<Tr: crate::core::transport::HttpTransport> Client<Tr> {
    pub async fn vacation_response_create(
        &self,
        subject: impl Into<String>,
        text_body: Option<impl Into<String>>,
        html_body: Option<impl Into<String>>,
    ) -> crate::Result<VacationResponse> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        let mut set = VacationResponseSet::new(&account_id);
        let created_id = set
            .create()
            .is_enabled(true)
            .subject(Some(subject))
            .text_body(text_body)
            .html_body(html_body)
            .create_id()
            .unwrap();

        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.created(&created_id)
    }

    pub async fn vacation_response_enable(
        &self,
        subject: impl Into<String>,
        text_body: Option<impl Into<String>>,
        html_body: Option<impl Into<String>>,
    ) -> crate::Result<Option<VacationResponse>> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        let mut set = VacationResponseSet::new(&account_id);
        set.update("singleton")
            .is_enabled(true)
            .subject(Some(subject))
            .text_body(text_body)
            .html_body(html_body);

        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.updated("singleton")
    }

    pub async fn vacation_response_disable(&self) -> crate::Result<Option<VacationResponse>> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        let mut set = VacationResponseSet::new(&account_id);
        set.update("singleton").is_enabled(false);

        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.updated("singleton")
    }

    pub async fn vacation_response_set_dates(
        &self,
        from_date: Option<i64>,
        to_date: Option<i64>,
    ) -> crate::Result<Option<VacationResponse>> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        let mut set = VacationResponseSet::new(&account_id);
        set.update("singleton")
            .is_enabled(true)
            .from_date(from_date)
            .to_date(to_date);

        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.updated("singleton")
    }

    pub async fn vacation_response_get(
        &self,
        properties: Option<impl IntoIterator<Item = Property>>,
    ) -> crate::Result<Option<VacationResponse>> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        let mut get = VacationResponseGet::new(&account_id);
        get.ids(["singleton"]);
        if let Some(properties) = properties {
            get.properties(properties);
        }
        let handle = request.call(get)?;
        let mut response = request.send().await?;
        response.get(&handle).map(|mut r| r.take_list().pop())
    }

    pub async fn vacation_response_destroy(&self) -> crate::Result<()> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        let mut set = VacationResponseSet::new(&account_id);
        set.destroy(["singleton"]);
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.destroyed("singleton")
    }
}
