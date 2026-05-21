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
    closed: std::sync::atomic::AtomicBool,
}

impl Pool {
    pub(crate) fn new(
        config: Arc<ImapAccountConfig>,
        primed: ImapConnection,
        data_cap: usize,
    ) -> Self {
        Self {
            inner: Arc::new(PoolInner {
                permits: Arc::new(Semaphore::new(data_cap.max(1))),
                idle: Mutex::new(vec![PoolMember {
                    conn: primed,
                    selected: None,
                }]),
                config,
                closed: std::sync::atomic::AtomicBool::new(false),
            }),
        }
    }

    #[allow(dead_code)]
    pub(crate) async fn checkout(&self) -> Result<PooledConn, Error> {
        let permit = Arc::clone(&self.inner.permits)
            .acquire_owned()
            .await
            .map_err(|_| Error::Closed)?;
        let member = self.inner.idle.lock().expect("pool lock poisoned").pop();
        let member = match member {
            Some(member) => member,
            None => {
                let (conn, _auth) = self
                    .inner
                    .config
                    .imap
                    .connect_authenticated(
                        &self.inner.config.credentials,
                        &self.inner.config.auth_policy,
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

    pub(crate) async fn checkout_for_folder(
        &self,
        folder: &MailboxName,
    ) -> Result<PooledConn, Error> {
        let permit = Arc::clone(&self.inner.permits)
            .acquire_owned()
            .await
            .map_err(|_| Error::Closed)?;
        let member = {
            let mut idle = self.inner.idle.lock().expect("pool lock poisoned");
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
                    .connect_authenticated(
                        &self.inner.config.credentials,
                        &self.inner.config.auth_policy,
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

    pub(crate) async fn dial_idle(&self) -> Result<ImapConnection, Error> {
        let (conn, _auth) = self
            .inner
            .config
            .imap
            .connect_authenticated(
                &self.inner.config.credentials,
                &self.inner.config.auth_policy,
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
}

impl Drop for PooledConn {
    fn drop(&mut self) {
        if let Some(member) = self.member.take()
            && !matches!(
                member.conn.session_state(),
                crate::connection::SessionState::Logout
            )
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
