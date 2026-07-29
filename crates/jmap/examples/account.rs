use std::sync::Arc;
use std::time::Duration;

use bifrost_jmap::sync::{JmapAccountFactory, JmapCredentials, ReconnectPolicy};
use bifrost_types::{AccountFactory, AccountId};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    runtime.block_on(run())
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let factory: Arc<dyn AccountFactory> = Arc::new(
        JmapAccountFactory::builder(
            "https://jmap.example.test/session",
            JmapCredentials::bearer("replace-with-token"),
        )
        .reconnect_policy(ReconnectPolicy {
            initial: Duration::from_secs(1),
            max: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(20),
        })
        .build(),
    );

    let account = factory.open(AccountId("example-jmap".to_owned())).await?;
    let _capabilities = account.capabilities();
    let _containers = account.containers_list().await?;
    account.close().await?;

    Ok(())
}
