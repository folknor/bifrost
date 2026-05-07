/*
 * WebSocket Example
 *
 * Demonstrates JMAP over WebSocket - persistent connection with
 * real-time push notifications for state changes.
 *
 * Requires the `websockets` cargo feature (enabled by default).
 */

#[cfg(feature = "websockets")]
use bifrost_jmap::{client::Client, client_ws::WebSocketMessage};
#[cfg(feature = "websockets")]
use futures_util::StreamExt;

#[cfg(feature = "websockets")]
async fn websocket_example() -> bifrost_jmap::Result<()> {
    let client = Client::new()
        .credentials(("john@example.org", "secret"))
        .connect("https://jmap.example.org")
        .await?;

    // Open a WebSocket connection for push notifications
    let mut ws_stream = client.connect_ws().await?;

    // Enable push notifications for all data types
    client
        .enable_push_ws(None::<Vec<bifrost_jmap::DataType>>, None::<String>)
        .await?;

    println!("Listening for JMAP push notifications via WebSocket...");

    // Consume push notifications
    while let Some(message) = ws_stream.next().await {
        match message? {
            WebSocketMessage::Response(response) => {
                println!(
                    "Received method response (session state: {})",
                    response.session_state()
                );
            }
            WebSocketMessage::PushNotification(push) => {
                println!("Push notification: {push:?}");
            }
            _ => {}
        }
    }

    Ok(())
}

#[cfg(feature = "websockets")]
fn main() {
    let _f = websocket_example();
}

#[cfg(not(feature = "websockets"))]
fn main() {
    eprintln!("This example requires the `websockets` feature.");
}
