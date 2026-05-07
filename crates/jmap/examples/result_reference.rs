//! Result-reference example.
//!
//! Demonstrates JMAP result references: chaining method calls so the
//! output of one call feeds the input of the next, all in a single
//! HTTP round-trip.

use bifrost_jmap::{
    client::Client,
    core::{capability, query::Filter},
    email::{self, EmailGet, EmailQuery},
    mailbox::{self, MailboxGet, MailboxQuery},
};

async fn result_reference_example() -> bifrost_jmap::Result<()> {
    let client = Client::new()
        .credentials(("john@example.org", "secret"))
        .connect("https://jmap.example.org")
        .await?;

    let mail = client.primary_account::<capability::Mail>()?;

    // Build a batch request with result references. ONE HTTP round-trip
    // that queries for emails matching a filter, then fetches the
    // matched emails using the query's result IDs.
    let mut request = mail.build();

    let query_handle = request.call(EmailQuery::new().filter(
        Filter::<email::query::Filter>::and([
            email::query::Filter::subject("meeting"),
            email::query::Filter::has_keyword("$seen"),
        ]),
    ))?;

    let get_handle = request.call(
        EmailGet::new()
            .ids_ref(query_handle.result_reference("/ids"))
            .properties([
                email::Property::Subject,
                email::Property::From,
                email::Property::ReceivedAt,
            ]),
    )?;

    let mut response = request.send().await?;

    let emails = response.get(&get_handle)?;
    for email in emails.list() {
        println!("Subject: {:?}, From: {:?}", email.subject(), email.from());
    }

    // Second example: mailbox query + get.
    let mut request = mail.build();

    let query_handle = request.call(MailboxQuery::new())?;

    let get_handle = request.call(
        MailboxGet::new()
            .ids_ref(query_handle.result_reference("/ids"))
            .properties([mailbox::Property::Name, mailbox::Property::Role]),
    )?;

    let mut response = request.send().await?;
    let mailboxes = response.get(&get_handle)?;

    for mb in mailboxes.list() {
        println!(
            "Mailbox: {} (role: {:?})",
            mb.name().unwrap_or("?"),
            mb.role().cloned().unwrap_or(mailbox::Role::None)
        );
    }

    Ok(())
}

fn main() {
    let _f = result_reference_example();
}
