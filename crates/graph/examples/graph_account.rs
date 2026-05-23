use std::env;
use std::sync::Arc;

use bifrost_graph::account::{GraphAccountFactory, GraphClient};
use bifrost_types::{AccountFactory, AccountId};

#[tokio::main]
async fn main() {
    let access_token = env::var("BIFROST_GRAPH_ACCESS_TOKEN")
        .expect("set BIFROST_GRAPH_ACCESS_TOKEN to a Microsoft Graph OAuth token");
    let api_base = env::var("BIFROST_GRAPH_API_BASE")
        .unwrap_or_else(|_| "https://graph.microsoft.com/v1.0".to_string());

    let client = GraphClient::with_api_base(api_base, access_token);
    let factory: Arc<dyn AccountFactory> = Arc::new(GraphAccountFactory::new(client));
    let account_id = AccountId(
        env::var("BIFROST_GRAPH_ACCOUNT_ID").unwrap_or_else(|_| "graph-example".to_string()),
    );

    let account = match factory.open(account_id).await {
        Ok(account) => account,
        Err(error) => {
            eprintln!("failed to open Graph account: {error:?}");
            return;
        }
    };

    let capabilities = account.capabilities();
    println!(
        "graph account supports search: {}",
        capabilities.pim_methods.search
    );

    match account.containers_list().await {
        Ok(containers) => println!("graph account has {} mail folders", containers.len()),
        Err(error) => eprintln!("failed to list Graph containers: {error:?}"),
    }

    if let Err(error) = account.close().await {
        eprintln!("failed to close Graph account: {error:?}");
    }
}
