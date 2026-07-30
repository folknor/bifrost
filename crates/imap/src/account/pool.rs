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
