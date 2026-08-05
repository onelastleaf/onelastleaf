use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio::sync::Notify;

use super::super::PluginStore;
use crate::plugin::{PluginError, PluginJobId};

#[derive(Debug, Default)]
pub(in crate::plugin::store) struct JobTerminalNotifications {
    entries: Mutex<HashMap<PluginJobId, Weak<JobTerminalSignal>>>,
}

#[derive(Debug, Default)]
struct JobTerminalSignal {
    terminal: AtomicBool,
    changed: Notify,
}

struct JobTerminalSubscription {
    job_id: PluginJobId,
    signal: Arc<JobTerminalSignal>,
    notifications: Arc<JobTerminalNotifications>,
}

impl PluginStore {
    /// Waits until the durable row is terminal. Registering before reading SQL
    /// closes the race with a terminal commit that happens while the deadline
    /// task is starting.
    pub(crate) async fn wait_for_job_terminal(&self, job_id: PluginJobId) {
        let subscription = self.subscribe_job_terminal(job_id);
        loop {
            match self.get_job(job_id).await {
                Ok(job) if job.state.is_terminal() => {
                    self.publish_job_terminal(job_id);
                    return;
                }
                Ok(_) => {
                    subscription.wait().await;
                    return;
                }
                Err(PluginError::NotFound(_)) => return,
                Err(_) => {
                    tokio::select! {
                        () = subscription.wait() => return,
                        () = tokio::time::sleep(Duration::from_secs(1)) => {}
                    }
                }
            }
        }
    }

    pub(in crate::plugin::store) fn publish_job_terminal(&self, job_id: PluginJobId) {
        self.terminal_jobs.publish(job_id);
    }

    fn subscribe_job_terminal(&self, job_id: PluginJobId) -> JobTerminalSubscription {
        let signal = self.terminal_jobs.subscribe(job_id);
        JobTerminalSubscription {
            job_id,
            signal,
            notifications: Arc::clone(&self.terminal_jobs),
        }
    }
}

impl JobTerminalNotifications {
    fn entries(&self) -> std::sync::MutexGuard<'_, HashMap<PluginJobId, Weak<JobTerminalSignal>>> {
        self.entries
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    fn subscribe(&self, job_id: PluginJobId) -> Arc<JobTerminalSignal> {
        let mut entries = self.entries();
        if let Some(signal) = entries.get(&job_id).and_then(Weak::upgrade) {
            return signal;
        }
        let signal = Arc::new(JobTerminalSignal::default());
        entries.insert(job_id, Arc::downgrade(&signal));
        signal
    }

    fn publish(&self, job_id: PluginJobId) {
        let signal = self
            .entries()
            .remove(&job_id)
            .and_then(|signal| signal.upgrade());
        if let Some(signal) = signal {
            signal.terminal.store(true, Ordering::Release);
            signal.changed.notify_waiters();
        }
    }

    fn remove_if_last(&self, job_id: PluginJobId, signal: &Arc<JobTerminalSignal>) {
        let mut entries = self.entries();
        if Arc::strong_count(signal) == 1
            && entries
                .get(&job_id)
                .is_some_and(|entry| Weak::ptr_eq(entry, &Arc::downgrade(signal)))
        {
            entries.remove(&job_id);
        }
    }

    #[cfg(test)]
    fn live_entry_count(&self) -> usize {
        self.entries()
            .values()
            .filter(|entry| entry.strong_count() != 0)
            .count()
    }
}

impl JobTerminalSubscription {
    async fn wait(&self) {
        loop {
            if self.signal.terminal.load(Ordering::Acquire) {
                return;
            }
            let changed = self.signal.changed.notified();
            tokio::pin!(changed);
            if self.signal.terminal.load(Ordering::Acquire) {
                return;
            }
            changed.await;
        }
    }
}

impl Drop for JobTerminalSubscription {
    fn drop(&mut self) {
        self.notifications.remove_if_last(self.job_id, &self.signal);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{JobTerminalNotifications, JobTerminalSubscription};
    use crate::plugin::PluginJobId;

    fn subscribe(
        notifications: &Arc<JobTerminalNotifications>,
        job_id: PluginJobId,
    ) -> JobTerminalSubscription {
        JobTerminalSubscription {
            job_id,
            signal: notifications.subscribe(job_id),
            notifications: Arc::clone(notifications),
        }
    }

    #[test]
    fn dropping_the_last_waiter_removes_its_registry_entry() {
        let notifications = Arc::new(JobTerminalNotifications::default());
        let subscription = subscribe(&notifications, PluginJobId::new());
        assert_eq!(notifications.live_entry_count(), 1);
        drop(subscription);
        assert_eq!(notifications.live_entry_count(), 0);
    }

    #[tokio::test]
    async fn publication_before_waiting_cannot_be_lost() {
        let notifications = Arc::new(JobTerminalNotifications::default());
        let job_id = PluginJobId::new();
        let subscription = subscribe(&notifications, job_id);
        notifications.publish(job_id);
        tokio::time::timeout(std::time::Duration::from_secs(1), subscription.wait())
            .await
            .expect("terminal notification was lost");
        assert_eq!(notifications.live_entry_count(), 0);
    }
}
