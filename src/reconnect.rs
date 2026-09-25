//! Bounded retries of complete, fresh synchronization sessions, never commands.
use crate::{Error, Result};
use serde::Deserialize;
use std::{future::Future, time::Duration};

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Policy {
    /// Additional sessions after the initial attempt. Zero preserves manual retry.
    pub attempts: u32,
    pub delay_seconds: u64,
    pub max_delay_seconds: u64,
    /// Overall command budget, including initial attempt and delays, when enabled.
    pub max_elapsed_seconds: u64,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            attempts: 0,
            delay_seconds: 5,
            max_delay_seconds: 60,
            max_elapsed_seconds: 3600,
        }
    }
}
impl Policy {
    pub fn validate(&self) -> Result<()> {
        if self.attempts > 32
            || self.delay_seconds == 0
            || self.max_delay_seconds < self.delay_seconds
            || self.max_delay_seconds > 86400
            || self.max_elapsed_seconds == 0
            || self.max_elapsed_seconds > 604800
        {
            return Err(Error::Config("invalid reconnect policy: attempts <= 32, positive delays <= 86400, elapsed budget <= 604800".into()));
        }
        Ok(())
    }
    fn delay(&self, retry: u32) -> Duration {
        Duration::from_secs(
            self.delay_seconds
                .saturating_mul(1u64 << retry.min(32))
                .min(self.max_delay_seconds),
        )
    }
}
pub fn retryable(error: &Error) -> bool {
    matches!(
        error,
        Error::Connection(_) | Error::Busy | Error::Remote { code: 9, .. }
    )
}
pub async fn run<T, F, Fut>(policy: &Policy, mut attempt: F) -> Result<T>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    policy.validate()?;
    if policy.attempts == 0 {
        return attempt(0).await;
    }
    let session_loop = async {
        for number in 0..=policy.attempts {
            match attempt(number).await {
                Ok(value) => return Ok(value),
                Err(error) if number < policy.attempts && retryable(&error) => {
                    let delay = policy.delay(number);
                    tracing::warn!(retry=number+1, delay_seconds=delay.as_secs(), %error, "reconnecting with a fresh synchronization session");
                    tokio::time::sleep(delay).await;
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!()
    };
    tokio::time::timeout(
        Duration::from_secs(policy.max_elapsed_seconds),
        session_loop,
    )
    .await
    .map_err(|_| {
        Error::Connection(
            "reconnect overall deadline exceeded; final state may require a fresh rescan".into(),
        )
    })?
}
