use std::env;
use std::sync::Arc;

use bifrost_imap::{AuthPolicy, Credentials, ImapAccountConfig, ImapAccountFactory, ImapConfig};
use bifrost_types::{AccountFactory, AccountId};

#[tokio::main]
async fn main() {
    let host = env::var("BIFROST_IMAP_HOST").expect("BIFROST_IMAP_HOST is required");
    let username = env::var("BIFROST_IMAP_USERNAME").expect("BIFROST_IMAP_USERNAME is required");
    let password = env::var("BIFROST_IMAP_PASSWORD").expect("BIFROST_IMAP_PASSWORD is required");

    let config = ImapAccountConfig::new(
        ImapConfig::tls(host),
        Credentials::password(username, password),
        AuthPolicy::default(),
    );
    let factory: Arc<dyn AccountFactory> = Arc::new(ImapAccountFactory::new(config));
    let account = factory
        .open(AccountId("imap-example".to_owned()))
        .await
        .expect("account opens");

    let _capabilities = account.capabilities();
    let _containers = account.containers_list().await.expect("containers list");
    account.close().await.expect("account closes");
}
