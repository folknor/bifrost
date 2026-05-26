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
        Ok(())
    })
}
