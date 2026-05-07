//! Message lifecycle against the protocol layer: import, query, fetch,
//! tag, move, destroy.

use bifrost_jmap::{
    Bytes,
    client::Client,
    core::{capability, query::Filter},
    email::{self, EmailGet, EmailQuery, EmailSet, Property, import::EmailImportRequest},
    mailbox::{self, MailboxQuery, Role},
};

const TEST_MESSAGE: &[u8; 90] = br#"From: john@example.org
To: jane@example.org
Subject: Testing JMAP client

This is a test.
"#;

async fn messages() {
    let client = Client::new()
        .credentials(("john@example.org", "secret"))
        .connect("https://jmap.example.org")
        .await
        .unwrap();

    let mail = client.primary_account::<capability::Mail>().unwrap();

    // Resolve Inbox / Trash IDs.
    let inbox_id = mail
        .call(MailboxQuery::new().filter(mailbox::query::Filter::role(Role::Inbox)))
        .await
        .unwrap()
        .into_ids()
        .pop()
        .unwrap();
    let trash_id = mail
        .call(MailboxQuery::new().filter(mailbox::query::Filter::role(Role::Trash)))
        .await
        .unwrap()
        .into_ids()
        .pop()
        .unwrap();

    // Upload, then import as a draft into the inbox.
    let blob = mail
        .upload(Bytes::from_static(TEST_MESSAGE).to_vec(), None)
        .await
        .unwrap();

    let mut import = EmailImportRequest::new();
    import
        .email(blob.blob_id.as_str())
        .mailbox_ids([&inbox_id])
        .keywords(["$draft"]);
    mail.call(import).await.unwrap();

    // Query for the imported draft.
    let email_id = mail
        .call(
            EmailQuery::new()
                .filter(Filter::and([
                    email::query::Filter::subject("test"),
                    email::query::Filter::in_mailbox(&inbox_id),
                    email::query::Filter::has_keyword("$draft"),
                ]))
                .sort([email::query::Comparator::from()]),
        )
        .await
        .unwrap()
        .into_ids()
        .pop()
        .unwrap();

    // Fetch.
    let email = mail
        .call(EmailGet::new().ids([&email_id]).properties([
            Property::Subject,
            Property::Preview,
            Property::Keywords,
        ]))
        .await
        .unwrap()
        .into_list()
        .pop()
        .unwrap();
    assert_eq!(email.preview().unwrap(), "This is a test.");
    assert_eq!(email.subject().unwrap(), "Testing JMAP client");
    assert_eq!(email.keywords(), ["$draft"]);

    // Mark seen + important, drop $draft, move to Trash.
    let mut set = EmailSet::new();
    set.update(&email_id)
        .keyword("$draft", false)
        .keyword("$seen", true)
        .keyword("$important", true)
        .mailbox_id(&inbox_id, false)
        .mailbox_id(&trash_id, true);
    mail.call(set).await.unwrap();

    // Destroy.
    mail.call(EmailSet::new().destroy([&email_id]))
        .await
        .unwrap();
}

fn main() {
    let _c = messages();
}
