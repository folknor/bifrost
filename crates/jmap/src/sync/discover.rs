use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bifrost_types::{
    AccountStream, Batch, CursorScope, MembershipScope, PageBoundary, ScopeLifecycle, SyncEvent,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::mailbox::{Mailbox, MailboxChanges, MailboxGet, MailboxId, Property};
use crate::transport_reqwest::ReqwestTransport;

use super::capabilities::CoreLimits;

type MailAccount = crate::account::Account<ReqwestTransport>;

pub(crate) fn cursor_scopes(scopes: Vec<CursorScope>) -> AccountStream<SyncEvent<CursorScope>> {
    Box::pin(async_stream::stream! {
        if !scopes.is_empty() {
            yield SyncEvent::Batch(Batch {
                items: scopes,
                page_boundary: PageBoundary::Final,
                server_latency: Duration::ZERO,
                bytes_in: 0,
                checkpoint: None,
            });
        }
        yield SyncEvent::Done(None);
    })
}

pub(crate) fn memberships(mail: MailAccount) -> AccountStream<SyncEvent<MembershipScope>> {
    Box::pin(async_stream::stream! {
        let started = Instant::now();
        let response = mail
            .call(MailboxGet::new().properties([Property::Id]))
            .await;

        match response {
            Ok(response) => {
                let items = response
                    .into_list()
                    .into_iter()
                    .filter_map(|mut mailbox| {
                        let id = mailbox.take_id();
                        if id.as_str().is_empty() {
                            None
                        } else {
                            Some(MembershipScope::Mailbox(bifrost_types::MailboxId(
                                id.into_string(),
                            )))
                        }
                    })
                    .collect::<Vec<_>>();

                if !items.is_empty() {
                    yield SyncEvent::Batch(Batch {
                        items,
                        page_boundary: PageBoundary::Final,
                        server_latency: started.elapsed(),
                        bytes_in: 0,
                        checkpoint: None,
                    });
                }
                yield SyncEvent::Done(None);
            }
            Err(err) => {
                yield super::error::fatal_from_jmap(err, None);
            }
        }
    })
}

pub(crate) async fn fetch_mailbox_names(
    mail: &MailAccount,
) -> crate::Result<(String, HashMap<String, String>)> {
    let response = mail
        .call(MailboxGet::new().properties([Property::Id, Property::Name]))
        .await?;
    let state = response.state().to_string();
    let mut names = HashMap::new();

    for mut mailbox in response.into_list() {
        let id = mailbox.take_id();
        if !id.as_str().is_empty() {
            names.insert(id.into_string(), mailbox.name().unwrap_or("").to_string());
        }
    }

    Ok((state, names))
}

pub(crate) fn scope_lifecycle(
    mail: MailAccount,
    limits: CoreLimits,
    mailbox_state: Arc<Mutex<Option<String>>>,
    mailbox_names: Arc<Mutex<HashMap<String, String>>>,
    shutdown: CancellationToken,
) -> AccountStream<ScopeLifecycle> {
    Box::pin(async_stream::stream! {
        loop {
            if shutdown.is_cancelled() {
                break;
            }

            let since_state = {
                let guard = mailbox_state.lock().await;
                guard.clone()
            };

            let Some(since_state) = since_state else {
                tokio::time::sleep(Duration::from_secs(300)).await;
                continue;
            };

            let max_changes = NonZeroUsize::new(limits.max_objects_in_get.max(1));
            let Some(max_changes) = max_changes else {
                break;
            };

            let response = mail
                .call(MailboxChanges::new(since_state).max_changes(max_changes))
                .await;

            match response {
                Ok(response) => {
                    let created = response.created().to_vec();
                    let updated = response.updated().to_vec();
                    let destroyed = response.destroyed().to_vec();
                    set_mailbox_state(&mailbox_state, response.new_state().to_string()).await;

                    if !created.is_empty() || !updated.is_empty() {
                        if let Ok(fetched) = fetch_mailboxes(&mail, created.iter().chain(&updated)).await {
                            for mailbox in fetched {
                                let id = mailbox.id().map(ToString::to_string);
                                let name = mailbox.name().unwrap_or("").to_string();
                                let Some(id) = id else {
                                    continue;
                                };

                                if created
                                    .iter()
                                    .any(|created_id| created_id.as_str() == id.as_str())
                                {
                                    update_mailbox_name(&mailbox_names, id.clone(), name).await;
                                    yield ScopeLifecycle::Created(MembershipScope::Mailbox(
                                        bifrost_types::MailboxId(id),
                                    ));
                                } else {
                                    let old_name = replace_mailbox_name(
                                        &mailbox_names,
                                        id.clone(),
                                        name,
                                    )
                                    .await;
                                    if old_name.is_some() {
                                        let scope = MembershipScope::Mailbox(
                                            bifrost_types::MailboxId(id),
                                        );
                                        yield ScopeLifecycle::Renamed {
                                            old: scope.clone(),
                                            new: scope,
                                        };
                                    }
                                }
                            }
                        }
                    }

                    for destroyed_id in destroyed {
                        let id = destroyed_id.into_string();
                        remove_mailbox_name(&mailbox_names, &id).await;
                        yield ScopeLifecycle::Deleted(MembershipScope::Mailbox(
                            bifrost_types::MailboxId(id),
                        ));
                    }

                    if !response.has_more_changes() {
                        tokio::time::sleep(Duration::from_secs(300)).await;
                    }
                }
                Err(_) => {
                    tokio::time::sleep(Duration::from_secs(300)).await;
                }
            }
        }
    })
}

async fn fetch_mailboxes<'a>(
    mail: &MailAccount,
    ids: impl Iterator<Item = &'a MailboxId>,
) -> crate::Result<Vec<Mailbox>> {
    let ids = ids.cloned().collect::<Vec<_>>();
    if ids.is_empty() {
        return Ok(Vec::new());
    }

    Ok(mail
        .call(
            MailboxGet::new()
                .ids(ids)
                .properties([Property::Id, Property::Name]),
        )
        .await?
        .into_list())
}

async fn set_mailbox_state(state: &Arc<Mutex<Option<String>>>, value: String) {
    let mut guard = state.lock().await;
    *guard = Some(value);
}

async fn update_mailbox_name(
    names: &Arc<Mutex<HashMap<String, String>>>,
    id: String,
    name: String,
) {
    let mut guard = names.lock().await;
    guard.insert(id, name);
}

async fn replace_mailbox_name(
    names: &Arc<Mutex<HashMap<String, String>>>,
    id: String,
    name: String,
) -> Option<String> {
    let mut guard = names.lock().await;
    guard.insert(id, name)
}

async fn remove_mailbox_name(names: &Arc<Mutex<HashMap<String, String>>>, id: &str) {
    let mut guard = names.lock().await;
    guard.remove(id);
}
