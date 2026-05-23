//! Cross-crate type-level conformance for the four protocol Account
//! and AccountFactory implementations.
//!
//! Constructs each public factory, witnesses that `dyn Account` and
//! `dyn AccountFactory` are object-safe, and verifies the four
//! `AccountFactory::open(AccountId)` implementations compose with
//! `SyncEngine::attach` at the type level. The attach future is
//! constructed but never polled, so no network work runs.

use std::sync::Arc;

use bifrost_gmail::account::GmailAccountFactory;
use bifrost_graph::account::{GraphAccountFactory, GraphClient};
use bifrost_imap::{
    AuthPolicy, Credentials, ImapConfig,
    account::{ImapAccountConfig, ImapAccountFactory},
};
use bifrost_jmap::sync::{JmapAccountFactory, JmapCredentials};
use bifrost_net::StaticTokenSource;
use bifrost_sync::SyncEngine;
use bifrost_types::{Account, AccountFactory, AccountId};

fn _account_is_object_safe(_: &dyn Account) {}

fn _account_factory_is_object_safe(_: &dyn AccountFactory) {}

fn sync_engine() -> SyncEngine {
    SyncEngine::builder()
        .build()
        .expect("default engine config is valid")
}

fn _assert_attach_accepts(engine: &SyncEngine, factory: &Arc<dyn AccountFactory>) {
    let _future = engine.attach(
        AccountId("cross-crate-conformance".to_owned()),
        Arc::clone(factory),
    );
}

fn imap_factory() -> Arc<dyn AccountFactory> {
    let config = ImapAccountConfig::new(
        ImapConfig::tls("imap.example.test"),
        Credentials::password("user@example.test", "password"),
        AuthPolicy::default(),
    );
    Arc::new(ImapAccountFactory::new(config))
}

fn jmap_factory() -> Arc<dyn AccountFactory> {
    Arc::new(
        JmapAccountFactory::builder(
            "https://jmap.example.test/session",
            JmapCredentials::Bearer {
                token_source: StaticTokenSource::new("test-token", None),
            },
        )
        .build(),
    )
}

fn gmail_factory() -> Arc<dyn AccountFactory> {
    Arc::new(GmailAccountFactory::from_access_token("test-token"))
}

fn graph_factory() -> Arc<dyn AccountFactory> {
    Arc::new(GraphAccountFactory::new(GraphClient::new("test-token")))
}

#[test]
fn account_traits_are_object_safe() {
    let _account: fn(&dyn Account) = _account_is_object_safe;
    let _factory: fn(&dyn AccountFactory) = _account_factory_is_object_safe;
}

#[test]
fn imap_factory_composes_with_engine() {
    let engine = sync_engine();
    let factory: Arc<dyn AccountFactory> = imap_factory();
    _account_factory_is_object_safe(factory.as_ref());
    _assert_attach_accepts(&engine, &factory);
}

#[test]
fn jmap_factory_composes_with_engine() {
    let engine = sync_engine();
    let factory: Arc<dyn AccountFactory> = jmap_factory();
    _account_factory_is_object_safe(factory.as_ref());
    _assert_attach_accepts(&engine, &factory);
}

#[test]
fn gmail_factory_composes_with_engine() {
    let engine = sync_engine();
    let factory: Arc<dyn AccountFactory> = gmail_factory();
    _account_factory_is_object_safe(factory.as_ref());
    _assert_attach_accepts(&engine, &factory);
}

#[test]
fn graph_factory_composes_with_engine() {
    let engine = sync_engine();
    let factory: Arc<dyn AccountFactory> = graph_factory();
    _account_factory_is_object_safe(factory.as_ref());
    _assert_attach_accepts(&engine, &factory);
}

#[test]
fn all_four_factories_share_engine_signature() {
    let engine = sync_engine();
    let factories: Vec<Arc<dyn AccountFactory>> = vec![
        imap_factory(),
        jmap_factory(),
        gmail_factory(),
        graph_factory(),
    ];

    for factory in &factories {
        _assert_attach_accepts(&engine, factory);
    }
}
