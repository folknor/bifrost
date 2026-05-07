use bifrost_jmap::{
    client::Client,
    core::query::Filter,
    email::{self, Property},
    mailbox::{self, Role},
};

const TEST_MESSAGE: &[u8; 90] = br#"From: john@example.org
To: jane@example.org
Subject: Testing JMAP client

This is a test.
"#;

async fn messages() {
    // Connect to the JMAP server using Basic authentication
    let client = Client::new()
        .credentials(("john@example.org", "secret"))
        .connect("https://jmap.example.org")
        .await
        .unwrap();

    // Query mailboxes to obtain Inbox and Trash folder id
    let inbox_id = client
        .mailbox_query(
            mailbox::query::Filter::role(Role::Inbox).into(),
            None::<Vec<_>>,
        )
        .await
        .unwrap()
        .into_ids()
        .pop()
        .unwrap();
    let trash_id = client
        .mailbox_query(
            mailbox::query::Filter::role(Role::Trash).into(),
            None::<Vec<_>>,
        )
        .await
        .unwrap()
        .into_ids()
        .pop()
        .unwrap();

    // Import message into inbox
    client
        .email_import(TEST_MESSAGE.to_vec(), [&inbox_id], ["$draft"].into(), None)
        .await
        .unwrap();

    // Query mailbox
    let email_id = client
        .email_query(
            Filter::and([
                email::query::Filter::subject("test"),
                email::query::Filter::in_mailbox(&inbox_id),
                email::query::Filter::has_keyword("$draft"),
            ])
            .into(),
            [email::query::Comparator::from()].into(),
        )
        .await
        .unwrap()
        .into_ids()
        .pop()
        .unwrap();

    // Fetch message
    let email = client
        .email_get(
            &email_id,
            [Property::Subject, Property::Preview, Property::Keywords].into(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(email.preview().unwrap(), "This is a test.");
    assert_eq!(email.subject().unwrap(), "Testing JMAP client");
    assert_eq!(email.keywords(), ["$draft"]);

    // Remove the $draft keyword
    client
        .email_set_keyword(&email_id, "$draft", false)
        .await
        .unwrap();

    // Replace all keywords
    client
        .email_set_keywords(&email_id, ["$seen", "$important"])
        .await
        .unwrap();

    // Move the message to the Trash folder
    client
        .email_set_mailboxes(&email_id, [&trash_id])
        .await
        .unwrap();

    // Destroy the e-mail
    client.email_destroy(&email_id).await.unwrap();
}

fn main() {
    let _c = messages();
}
