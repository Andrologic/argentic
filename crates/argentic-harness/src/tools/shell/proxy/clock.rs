use std::time::{Duration, Instant};
use tokio::sync::watch;

pub(in crate::tools::shell) struct Clock(watch::Sender<Wait>);

#[derive(Default)]
struct Wait {
    elapsed: Duration,
    since: Option<Instant>,
}

impl Clock {
    pub(super) fn new() -> Self {
        Self(watch::channel(Wait::default()).0)
    }

    pub(in crate::tools::shell) fn deadline(&self, base: Instant) -> Instant {
        let wait = self.0.borrow();
        base + wait.elapsed + wait.since.map_or(Duration::ZERO, |since| since.elapsed())
    }

    pub(super) fn base_after(&self, duration: Duration) -> Instant {
        let wait = self.0.borrow();
        wait.since.unwrap_or_else(Instant::now) - wait.elapsed + duration
    }

    pub(super) fn pause(&self) -> Pause {
        self.0.send_modify(|wait| wait.since = Some(Instant::now()));
        Pause(self.0.clone())
    }

    pub(super) async fn expire(&self, base: Instant) {
        let mut updates = self.0.subscribe();
        loop {
            let deadline = {
                let wait = updates.borrow_and_update();
                wait.since.is_none().then(|| base + wait.elapsed)
            };
            if let Some(deadline) = deadline {
                tokio::select! {
                    biased;
                    _ = updates.changed() => {},
                    _ = tokio::time::sleep_until(deadline.into()) => return,
                }
            } else {
                let _ = updates.changed().await;
            }
        }
    }
}

pub(super) struct Pause(watch::Sender<Wait>);

impl Drop for Pause {
    fn drop(&mut self) {
        self.0.send_modify(|wait| {
            if let Some(since) = wait.since.take() {
                wait.elapsed += since.elapsed();
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn proxy_connection_budget_only_counts_active_time() {
        let clock = Clock::new();
        let base = clock.base_after(Duration::from_millis(10));
        let pause = clock.pause();
        assert!(
            tokio::time::timeout(Duration::from_millis(30), clock.expire(base))
                .await
                .is_err()
        );
        drop(pause);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), clock.expire(base))
                .await
                .is_ok()
        );
    }
}
