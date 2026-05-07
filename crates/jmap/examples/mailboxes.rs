//! Mailbox CRUD against the protocol layer.
//!
//! Demonstrates:
//! - capability-typed account selection (`Client::primary_account`)
//! - one-shot method calls via `Account::call`
//! - batched/result-reference flows via `Account::build` + `Request::call`

use bifrost_jmap::{
    client::Client,
    core::{capability, set::SetObject},
    mailbox::{MailboxGet, MailboxQuery, MailboxSet, Role, query::Filter},
};

async fn mailboxes() {
    let client = Client::new()
        .credentials(("john@example.org", "secret"))
        .connect("https://jmap.example.org")
        .await
        .unwrap();

    let mail = client.primary_account::<capability::Mail>().unwrap();

    // Create a mailbox.
    let mut create = MailboxSet::new();
    let create_id = create
        .create()
        .name("My Mailbox")
        .role(Role::None)
        .create_id()
        .unwrap();
    let mailbox_id = mail
        .call(create)
        .await
        .unwrap()
        .created(&create_id)
        .unwrap()
        .take_id();

    // Rename it.
    let mut rename = MailboxSet::new();
    rename.update(&mailbox_id).name("My Renamed Mailbox");
    mail.call(rename).await.unwrap();

    // Query for the inbox id.
    let inbox_id = mail
        .call(MailboxQuery::new().filter(Filter::role(Role::Inbox)))
        .await
        .unwrap()
        .into_ids()
        .pop()
        .unwrap();

    // Print inbox details.
    let inbox = mail
        .call(MailboxGet::new().ids([&inbox_id]))
        .await
        .unwrap()
        .into_list()
        .pop();
    println!("{inbox:?}");

    // Move the new mailbox under inbox.
    let mut move_set = MailboxSet::new();
    move_set.update(&mailbox_id).parent_id(Some(&inbox_id));
    mail.call(move_set).await.unwrap();

    // Destroy it (and any contained messages).
    let mut destroy = MailboxSet::new().destroy([&mailbox_id]);
    destroy.arguments().on_destroy_remove_emails(true);
    mail.call(destroy).await.unwrap();
}

fn main() {
    let _c = mailboxes();
}
