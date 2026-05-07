/*
 * Result Reference Example
 *
 * Demonstrates JMAP result references - chaining method calls so that
 * the output of one call feeds into the input of the next, all in a
 * single HTTP request.
 */

use bifrost_jmap::{
    client::Client,
    core::query::Filter,
    email::{self, EmailGet, EmailQuery},
    mailbox::{self, MailboxGet, MailboxQuery},
};

async fn result_reference_example() -> bifrost_jmap::Result<()> {
    let client = Client::new()
        .credentials(("john@example.org", "secret"))
        .connect("https://jmap.example.org")
        .await?;

    // Build a batch request with result references.
    // This sends ONE HTTP request that:
    //   1. Queries for emails matching a filter
    //   2. Fetches the matched emails using the query's result IDs
    let mut request = client.build();

    // Step 1: Query for emails with subject "meeting"
    let mut query = EmailQuery::new();
    query.filter(Filter::<email::query::Filter>::and([
        email::query::Filter::subject("meeting"),
        email::query::Filter::has_keyword("$seen"),
    ]));
    let query_handle = request.call(query)?;

    // Step 2: Fetch the emails found by the query.
    // Instead of hardcoding IDs, we reference the query's result.
    let mut get = EmailGet::new();
    get.ids_ref(query_handle.result_reference("/ids"));
    get.properties([
        email::Property::Subject,
        email::Property::From,
        email::Property::ReceivedAt,
    ]);
    let get_handle = request.call(get)?;

    // Send the batch - one HTTP round-trip for both calls
    let mut response = request.send().await?;

    // Extract typed results using the handles
    let emails = response.get(&get_handle)?;
    for email in emails.list() {
        println!("Subject: {:?}, From: {:?}", email.subject(), email.from());
    }

    // --- Second example: mailbox query + get ---

    let mut request = client.build();

    // Find all mailboxes, then fetch their details
    let query_handle = request.call(MailboxQuery::new())?;

    let mut get = MailboxGet::new();
    get.ids_ref(query_handle.result_reference("/ids"));
    get.properties([mailbox::Property::Name, mailbox::Property::Role]);
    let get_handle = request.call(get)?;

    let mut response = request.send().await?;
    let mailboxes = response.get(&get_handle)?;

    for mailbox in mailboxes.list() {
        println!(
            "Mailbox: {} (role: {:?})",
            mailbox.name().unwrap_or("?"),
            mailbox.role().cloned().unwrap_or(mailbox::Role::None)
        );
    }

    Ok(())
}

fn main() {
    let _f = result_reference_example();
}
