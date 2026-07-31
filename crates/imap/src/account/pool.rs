use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::connection::ImapConnection;
use crate::error::Error;
use crate::types::MailboxName;

use super::factory::ImapAccountConfig;

struct PoolMember {
    conn: ImapConnection,
    selected: Option<MailboxName>,
}

pub(crate) struct Pool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    permits: Arc<Semaphore>,
    idle: Mutex<Vec<PoolMember>>,
    config: Arc<ImapAccountConfig>,
    meter: Option<bifrost_net::MeterSinkHandle>,
    bandwidth_cap: Arc<AtomicU64>,
    closed: std::sync::atomic::AtomicBool,
}

impl Pool {
    pub(crate) fn new(
        config: Arc<ImapAccountConfig>,
        primed: ImapConnection,
        data_cap: usize,
        meter: Option<bifrost_net::MeterSinkHandle>,
        bandwidth_cap: Arc<AtomicU64>,
    ) -> Self {
        Self {
            inner: Arc::new(PoolInner {
                permits: Arc::new(Semaphore::new(data_cap.max(1))),
                idle: Mutex::new(vec![PoolMember {
                    conn: primed,
                    selected: None,
                }]),
                config,
                meter,
                bandwidth_cap,
                closed: std::sync::atomic::AtomicBool::new(false),
            }),
        }
    }

    pub(crate) async fn checkout_for_folder(
        &self,
        folder: &MailboxName,
    ) -> Result<PooledConn, Error> {
        if self.inner.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(Error::closed());
        }
        let permit = Arc::clone(&self.inner.permits)
            .acquire_owned()
            .await
            .map_err(|_| Error::closed())?;
        let member = {
            let mut idle = self.inner.idle.lock().expect("pool lock poisoned");
            idle.retain(|member| member.conn.is_alive());
            let idx = idle
                .iter()
                .position(|member| member.selected.as_ref() == Some(folder));
            idx.map(|idx| idle.swap_remove(idx)).or_else(|| idle.pop())
        };
        let member = match member {
            Some(member) => member,
            None => {
                let (conn, _auth) = self
                    .inner
                    .config
                    .imap
                    .connect_authenticated_metered(
                        &self.inner.config.credentials,
                        &self.inner.config.auth_policy,
                        self.inner.meter.clone(),
                        Some(Arc::clone(&self.inner.bandwidth_cap)),
                    )
                    .await?;
                PoolMember {
                    conn,
                    selected: None,
                }
            }
        };
        Ok(PooledConn {
            member: Some(member),
            pool: Arc::clone(&self.inner),
            _permit: permit,
        })
    }

    /// Check out a pooled connection without imposing a selected-mailbox
    /// affinity. Callers that do not issue mailbox-relative commands can
    /// safely reuse any parked connection, including one left selected by a
    /// folder-scoped operation.
    pub(crate) async fn checkout_any(&self) -> Result<PooledConn, Error> {
        if self.inner.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(Error::closed());
        }
        let permit = Arc::clone(&self.inner.permits)
            .acquire_owned()
            .await
            .map_err(|_| Error::closed())?;
        let member = {
            let mut idle = self.inner.idle.lock().expect("pool lock poisoned");
            idle.retain(|member| member.conn.is_alive());
            idle.pop()
        };
        let member = match member {
            Some(member) => member,
            None => {
                let (conn, _auth) = self
                    .inner
                    .config
                    .imap
                    .connect_authenticated_metered(
                        &self.inner.config.credentials,
                        &self.inner.config.auth_policy,
                        self.inner.meter.clone(),
                        Some(Arc::clone(&self.inner.bandwidth_cap)),
                    )
                    .await?;
                PoolMember {
                    conn,
                    selected: None,
                }
            }
        };
        Ok(PooledConn {
            member: Some(member),
            pool: Arc::clone(&self.inner),
            _permit: permit,
        })
    }

    /// Dial the dedicated connection used exclusively by the long-lived IDLE
    /// loop. Ordinary account operations must use a checkout method instead.
    pub(crate) async fn dial_idle(&self) -> Result<ImapConnection, Error> {
        if self.inner.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(Error::closed());
        }
        let (conn, _auth) = self
            .inner
            .config
            .imap
            .connect_authenticated_metered(
                &self.inner.config.credentials,
                &self.inner.config.auth_policy,
                self.inner.meter.clone(),
                Some(Arc::clone(&self.inner.bandwidth_cap)),
            )
            .await?;
        Ok(conn)
    }

    pub(crate) async fn close(&self) {
        self.inner
            .closed
            .store(true, std::sync::atomic::Ordering::Release);
        let members = {
            let mut idle = self.inner.idle.lock().expect("pool lock poisoned");
            std::mem::take(&mut *idle)
        };
        for member in members {
            let _ = member.conn.logout().await;
        }
    }
}

pub(crate) struct PooledConn {
    member: Option<PoolMember>,
    pool: Arc<PoolInner>,
    _permit: OwnedSemaphorePermit,
}

impl PooledConn {
    /// The live connection of this checkout.
    ///
    /// Panics if the member has been taken: after `discard()`, or after a
    /// `deselect_target` fallback whose redial failed (the old member is
    /// already logged out by then, so there is nothing valid to return).
    /// Both are terminal for the checkout - every caller propagates the
    /// error and drops the `PooledConn` - so the panic marks a caller
    /// reusing a checkout it was told is dead, which is a bug worth being
    /// loud about rather than surfacing as a quiet dead-connection error.
    pub(crate) fn connection(&self) -> &ImapConnection {
        &self
            .member
            .as_ref()
            .expect("pooled connection present")
            .conn
    }

    pub(crate) fn set_selected(&mut self, folder: MailboxName) {
        if let Some(member) = &mut self.member {
            member.selected = Some(folder);
        }
    }

    /// Make a mailbox-management command safe for a target that this pooled
    /// member happens to have selected. RFC 3501 permits servers to reject
    /// DELETE and RENAME for the selected mailbox. Prefer UNSELECT because
    /// it keeps the checkout on its existing connection; old servers without
    /// UNSELECT replace the selected member with a freshly dialed connection.
    pub(crate) async fn deselect_target(
        &mut self,
        target: &MailboxName,
        timeout: std::time::Duration,
    ) -> Result<(), Error> {
        let selected_target = self
            .member
            .as_ref()
            .is_some_and(|member| member.selected.as_ref() == Some(target));
        if !selected_target {
            return Ok(());
        }

        match self.connection().unselect(timeout).await {
            Ok(()) => {
                self.member
                    .as_mut()
                    .expect("pooled connection present")
                    .selected = None;
                Ok(())
            }
            Err(Error::MissingCapability(_)) => self.replace_selected_member().await,
            Err(error) => Err(error),
        }
    }

    /// Replace this checkout's connection with a freshly dialed one.
    ///
    /// The old member is logged out and released BEFORE the replacement is
    /// dialed. Dialing first would hold cap+1 physical connections for the
    /// duration of the handshake, and a server that enforces its per-user
    /// connection limit rejects exactly the fallback that a pre-UNSELECT
    /// server needs. It also keeps the old session SELECTed on the target
    /// while the DELETE that follows is issued, which RFC 2683 2.2.2 warns
    /// against. Ordering the teardown first costs one round trip and makes
    /// the fallback work on the servers that require it.
    async fn replace_selected_member(&mut self) -> Result<(), Error> {
        if let Some(old) = self.member.take() {
            let _ = old.conn.logout().await;
        }
        if self.pool.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(Error::closed());
        }
        let (conn, _auth) = self
            .pool
            .config
            .imap
            .connect_authenticated_metered(
                &self.pool.config.credentials,
                &self.pool.config.auth_policy,
                self.pool.meter.clone(),
                Some(Arc::clone(&self.pool.bandwidth_cap)),
            )
            .await?;
        self.member = Some(PoolMember {
            conn,
            selected: None,
        });
        Ok(())
    }

    pub(crate) fn discard(&mut self) {
        self.member.take();
    }
}

impl Drop for PooledConn {
    fn drop(&mut self) {
        if let Some(member) = self.member.take()
            && !matches!(
                member.conn.session_state(),
                crate::connection::SessionState::Logout
            )
            && member.conn.is_alive()
            && !self.pool.closed.load(std::sync::atomic::Ordering::Acquire)
        {
            self.pool
                .idle
                .lock()
                .expect("pool lock poisoned")
                .push(member);
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;

    use super::super::factory::ImapAccountConfig;
    use super::{Pool, PoolMember};
    use crate::connection::test_support::{driver_pair, preauth_greeting};
    use crate::error::Error;
    use crate::types::{AuthPolicy, Capability, Credentials, MailboxName};

    /// A config whose dial target is unroutable on purpose: every test
    /// below must be served from parked members, so any code path that
    /// decides to dial shows up as a failure instead of silently passing.
    fn config() -> Arc<ImapAccountConfig> {
        Arc::new(ImapAccountConfig::new(
            crate::ImapConfig::plaintext("test.invalid"),
            Credentials::password("user", "pass"),
            AuthPolicy::default(),
        ))
    }

    fn folder(name: &str) -> MailboxName {
        MailboxName::new(name).unwrap()
    }

    /// A live connection tagged with an otherwise unused capability atom,
    /// so a checkout can be identified without writing a byte to the wire.
    ///
    /// The returned server end must stay alive for the duration of the
    /// test: dropping it ends the driver task, and the pool's liveness
    /// filter would then evict the member before affinity is consulted.
    async fn marked(mark: &str) -> (crate::ImapConnection, tokio::io::DuplexStream) {
        driver_pair(&preauth_greeting(&format!("IMAP4rev1 {mark}"))).await
    }

    fn pool_of(primed: crate::ImapConnection, data_cap: usize) -> Pool {
        Pool::new(
            config(),
            primed,
            data_cap,
            None,
            Arc::new(AtomicU64::new(0)),
        )
    }

    /// Park an extra member directly. `Pool::new` accepts only one primed
    /// connection and the only other way in is a real dial, which no
    /// hermetic test may perform.
    fn park(pool: &Pool, conn: crate::ImapConnection, selected: Option<MailboxName>) {
        pool.inner
            .idle
            .lock()
            .unwrap()
            .push(PoolMember { conn, selected });
    }

    fn set_primed_affinity(pool: &Pool, selected: MailboxName) {
        pool.inner.idle.lock().unwrap()[0].selected = Some(selected);
    }

    fn idle_len(pool: &Pool) -> usize {
        pool.inner.idle.lock().unwrap().len()
    }

    fn idle_selection(pool: &Pool) -> Vec<Option<MailboxName>> {
        pool.inner
            .idle
            .lock()
            .unwrap()
            .iter()
            .map(|member| member.selected.clone())
            .collect()
    }

    /// Checkout affinity is by selected mailbox, not by parking order.
    ///
    /// Two parked members, both usable; the one already SELECTed on the
    /// requested folder must win, otherwise every folder-scoped operation
    /// pays a re-SELECT round trip on a connection that was already there.
    #[tokio::test]
    async fn checkout_prefers_the_member_already_selected_on_the_folder() {
        let (primed, _primed_server) = marked("ACL").await;
        let (extra, _extra_server) = marked("BINARY").await;
        let pool = pool_of(primed, 2);
        set_primed_affinity(&pool, folder("INBOX"));
        park(&pool, extra, Some(folder("Archive")));

        let archive = pool.checkout_for_folder(&folder("Archive")).await.unwrap();
        assert!(
            archive
                .connection()
                .capabilities()
                .contains(&Capability::Binary),
            "the member selected on Archive must serve the Archive checkout",
        );

        let inbox = pool.checkout_for_folder(&folder("INBOX")).await.unwrap();
        assert!(
            inbox.connection().capabilities().contains(&Capability::Acl),
            "the member selected on INBOX must serve the INBOX checkout",
        );
    }

    /// `checkout_any` is the affinity-free lane: it reuses a parked member
    /// even though that member is selected on some unrelated mailbox.
    /// Dialing instead would burn a connection for a command that does not
    /// care which mailbox is selected.
    #[tokio::test]
    async fn checkout_any_reuses_a_member_selected_elsewhere() {
        let (primed, _primed_server) = marked("ACL").await;
        let pool = pool_of(primed, 2);
        set_primed_affinity(&pool, folder("Archive"));

        let conn = pool.checkout_any().await.unwrap();
        assert!(conn.connection().capabilities().contains(&Capability::Acl));
        assert_eq!(idle_len(&pool), 0, "the parked member was reused, not left");
    }

    /// Dropping a checkout parks it again WITH its selection, which is what
    /// makes the affinity above reachable on the next checkout.
    #[tokio::test]
    async fn dropping_a_checkout_parks_it_with_its_selection() {
        let (primed, _primed_server) = marked("ACL").await;
        let pool = pool_of(primed, 1);

        {
            let mut conn = pool.checkout_for_folder(&folder("INBOX")).await.unwrap();
            assert_eq!(idle_len(&pool), 0);
            conn.set_selected(folder("INBOX"));
        }

        assert_eq!(idle_selection(&pool), vec![Some(folder("INBOX"))]);
    }

    /// `discard` is the "this connection is suspect" exit: the member must
    /// not come back to the pool, or the next checkout hands the same
    /// broken session to another caller.
    #[tokio::test]
    async fn a_discarded_checkout_is_not_parked_again() {
        let (primed, _primed_server) = marked("ACL").await;
        let pool = pool_of(primed, 1);

        {
            let mut conn = pool.checkout_for_folder(&folder("INBOX")).await.unwrap();
            conn.discard();
        }

        assert_eq!(idle_len(&pool), 0, "a discarded member must not be parked");
    }

    /// Dropping a checkout releases its permit. A leaked permit is
    /// invisible until the pool is saturated, and at `data_cap == 1` it is
    /// a permanent deadlock, so pin it at the smallest cap.
    #[tokio::test]
    async fn dropping_a_checkout_releases_its_permit() {
        let (primed, _primed_server) = marked("ACL").await;
        let pool = pool_of(primed, 1);

        drop(pool.checkout_for_folder(&folder("INBOX")).await.unwrap());

        let second = tokio::time::timeout(
            Duration::from_secs(5),
            pool.checkout_for_folder(&folder("INBOX")),
        )
        .await
        .expect("the released permit must be reacquirable");
        assert!(second.is_ok());
    }

    /// `Semaphore::new(0)` never hands out a permit, so a misconfigured
    /// `pool_cap` of zero would hang the first checkout forever rather than
    /// erroring. `Pool::new` clamps to one; pin the clamp.
    #[tokio::test]
    async fn a_zero_data_cap_is_clamped_to_one_permit() {
        let (primed, _primed_server) = marked("ACL").await;
        let pool = pool_of(primed, 0);

        let conn = tokio::time::timeout(
            Duration::from_secs(5),
            pool.checkout_for_folder(&folder("INBOX")),
        )
        .await
        .expect("a zero cap must still yield one permit");
        assert!(conn.is_ok());
    }

    /// After `close`, every way into the pool refuses instead of dialing.
    /// A post-close dial would outlive the account and leak a session the
    /// consumer believes is gone.
    #[tokio::test]
    async fn close_gates_every_way_into_the_pool() {
        let (primed, primed_server) = marked("ACL").await;
        let pool = pool_of(primed, 1);
        // No responder for the LOGOUT close issues: dropping the server end
        // makes it fail fast instead of blocking on a read.
        drop(primed_server);

        tokio::time::timeout(Duration::from_secs(5), pool.close())
            .await
            .expect("close must not block on an unresponsive peer");

        assert!(matches!(
            pool.checkout_for_folder(&folder("INBOX")).await,
            Err(Error::Closed { .. })
        ));
        assert!(matches!(
            pool.checkout_any().await,
            Err(Error::Closed { .. })
        ));
        assert!(matches!(pool.dial_idle().await, Err(Error::Closed { .. })));
        assert_eq!(idle_len(&pool), 0, "close drains the parked members");
    }

    /// A checkout in flight when `close` runs is outside the idle list, so
    /// `close` cannot log it out. Its drop must not resurrect the pool by
    /// parking a live connection nobody will ever close.
    #[tokio::test]
    async fn a_checkout_outstanding_at_close_is_not_parked_on_drop() {
        let (primed, _primed_server) = marked("ACL").await;
        let pool = pool_of(primed, 1);

        let conn = pool.checkout_for_folder(&folder("INBOX")).await.unwrap();
        // The idle list is empty while the checkout is held, so close has
        // nothing to log out and cannot block.
        tokio::time::timeout(Duration::from_secs(5), pool.close())
            .await
            .expect("close must not block");
        drop(conn);

        assert_eq!(
            idle_len(&pool),
            0,
            "a closed pool must not accept a returning checkout",
        );
    }
}
