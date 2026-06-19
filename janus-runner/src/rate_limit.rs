use std::{
    collections::BTreeMap,
    sync::Mutex,
    time::{Duration, Instant},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateLimitExceeded {
    pub limit: u32,
    pub window_seconds: u64,
}

#[derive(Debug)]
pub struct RateLimiter {
    windows: Mutex<BTreeMap<String, RateWindow>>,
}

#[derive(Debug, Clone)]
struct RateWindow {
    started_at: Instant,
    count: u32,
}

impl RateLimiter {
    pub fn new() -> Self {
        Self {
            windows: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn check(
        &self,
        scope: &str,
        token_name: &str,
        limit: u32,
        window: Duration,
    ) -> Result<(), RateLimitExceeded> {
        let key = format!("{scope}:{token_name}");
        let now = Instant::now();
        let mut windows = self.windows.lock().expect("rate limiter mutex poisoned");
        prune_expired_windows(&mut windows, now, stale_window(window));

        let rate_window = windows.entry(key).or_insert_with(|| RateWindow {
            started_at: now,
            count: 0,
        });

        if now.duration_since(rate_window.started_at) >= window {
            rate_window.started_at = now;
            rate_window.count = 0;
        }

        if rate_window.count >= limit {
            return Err(RateLimitExceeded {
                limit,
                window_seconds: window.as_secs(),
            });
        }

        rate_window.count += 1;
        Ok(())
    }

    #[cfg(test)]
    fn window_count(&self) -> usize {
        self.windows
            .lock()
            .expect("rate limiter mutex poisoned")
            .len()
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

fn prune_expired_windows(
    windows: &mut BTreeMap<String, RateWindow>,
    now: Instant,
    max_age: Duration,
) {
    windows.retain(|_, rate_window| now.duration_since(rate_window.started_at) < max_age);
}

fn stale_window(window: Duration) -> Duration {
    window.checked_mul(2).unwrap_or(window)
}

#[cfg(test)]
mod tests {
    use super::RateLimiter;
    use std::{thread, time::Duration};

    #[test]
    fn rate_limit_resets_after_window() {
        let limiter = RateLimiter::new();

        limiter
            .check("jobs_run", "runner", 1, Duration::from_millis(10))
            .expect("first request should pass");
        assert!(
            limiter
                .check("jobs_run", "runner", 1, Duration::from_millis(10))
                .is_err()
        );

        thread::sleep(Duration::from_millis(12));

        limiter
            .check("jobs_run", "runner", 1, Duration::from_millis(10))
            .expect("window should reset");
    }

    #[test]
    fn prunes_stale_windows_during_checks() {
        let limiter = RateLimiter::new();

        limiter
            .check("jobs_run", "runner-a", 1, Duration::from_millis(10))
            .expect("first token should pass");
        assert_eq!(limiter.window_count(), 1);

        thread::sleep(Duration::from_millis(25));

        limiter
            .check("jobs_run", "runner-b", 1, Duration::from_millis(10))
            .expect("second token should pass");

        assert_eq!(limiter.window_count(), 1);
    }
}
