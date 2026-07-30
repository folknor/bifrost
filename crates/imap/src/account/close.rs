use std::sync::atomic::Ordering;

use bifrost_types::{AccountError, AccountFuture};

use super::ImapAccount;

pub(crate) fn close(account: ImapAccount) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        if account.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        account.shutdown.cancel();
        account.push.stop();
        account.pool.close().await;
        let mut first_error = None;
        if let Some(contacts) = &account.contacts
            && let Err(error) = contacts.close().await
        {
            first_error = Some(error);
        }
        if let Some(calendars) = &account.calendars
            && let Err(error) = calendars.close().await
            && first_error.is_none()
        {
            first_error = Some(error);
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    })
}
