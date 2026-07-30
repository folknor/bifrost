use std::sync::Arc;

use bifrost_google::account::GoogleAccountFactory;
use bifrost_types::{AccountFactory, AccountId, CursorScope};
use futures::StreamExt;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let Some(token) = std::env::var_os("GOOGLE_ACCESS_TOKEN") else {
        eprintln!("set GOOGLE_ACCESS_TOKEN to run this example");
        return Ok(());
    };

    let factory: Arc<dyn AccountFactory> = Arc::new(GoogleAccountFactory::from_access_token(
        token.to_string_lossy().into_owned(),
    ));

    let opened = factory
        .open(AccountId("google-example".to_string()))
        .await?;
    let account = opened.account;
    let _push_in_process = account.capabilities().push_in_process();
    let _cursor = account
        .establish_initial_cursor(CursorScope::Account)
        .await?;

    let mut scopes = account.discover_cursor_scopes();
    while let Some(_event) = scopes.next().await {}

    account.close().await?;
    Ok(())
}
