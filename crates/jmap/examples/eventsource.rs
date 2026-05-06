use futures_util::StreamExt;
use bifrost_jmap::{client::Client, DataType};

async fn event_source() {
    // Connect to the JMAP server using Basic authentication
    let client = Client::new()
        .credentials(("john@example.org", "secret"))
        .connect("https://jmap.example.org")
        .await
        .unwrap();

    // Open EventSource connection
    let mut stream = client
        .event_source(
            [
                DataType::Email,
                DataType::EmailDelivery,
                DataType::Mailbox,
                DataType::EmailSubmission,
                DataType::Identity,
            ]
            .into(),
            false,
            60.into(),
            None,
        )
        .await
        .unwrap();

    // Consume events
    while let Some(event) = stream.next().await {
        use bifrost_jmap::event_source::PushNotification;

        match event.unwrap() {
            PushNotification::StateChange(changes) => {
                println!("-> Change id: {:?}", changes.id());
                for account_id in changes.changed_accounts() {
                    println!(" Account {account_id} has changes:");
                    if let Some(account_changes) = changes.changes(account_id) {
                        for (type_state, state_id) in account_changes {
                            println!("   Type {type_state:?} has a new state {state_id}.");
                        }
                    }
                }
            }
            PushNotification::CalendarAlert(calendar_alert) => {
                println!(
                    "-> Calendar alert received for event {} (alert id {}).",
                    calendar_alert.calendar_event_id, calendar_alert.alert_id
                );
            }
            _ => {}
        }
    }
}

fn main() {
    let _c = event_source();
}
