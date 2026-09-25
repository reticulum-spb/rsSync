use rrsync::{
    Error, Result,
    reconnect::{Policy, run},
};
use std::{cell::Cell, time::Duration};
fn policy() -> Policy {
    Policy {
        attempts: 4,
        delay_seconds: 2,
        max_delay_seconds: 5,
        max_elapsed_seconds: 60,
    }
}
#[tokio::test(start_paused = true)]
async fn retry_count_and_capped_backoff_are_bounded() {
    let calls = Cell::new(0);
    let started = tokio::time::Instant::now();
    let result: Result<()> = run(&policy(), |_| {
        calls.set(calls.get() + 1);
        std::future::ready(Err(Error::Connection("offline".into())))
    })
    .await;
    assert!(result.is_err());
    assert_eq!(calls.get(), 5);
    assert_eq!(started.elapsed(), Duration::from_secs(2 + 4 + 5 + 5));
}
#[tokio::test(start_paused = true)]
async fn success_after_connection_loss_or_busy_returns_once() {
    let calls = Cell::new(0);
    let value = run(&policy(), |n| {
        calls.set(calls.get() + 1);
        std::future::ready(match n {
            0 => Err(Error::Connection("closed".into())),
            1 => Err(Error::Remote {
                code: 9,
                text: "busy".into(),
            }),
            _ => Ok(42),
        })
    })
    .await
    .unwrap();
    assert_eq!(value, 42);
    assert_eq!(calls.get(), 3);
}
#[tokio::test(start_paused = true)]
async fn fatal_errors_never_retry_and_disabled_policy_has_one_attempt() {
    for error in [
        Error::PermissionDenied,
        Error::HashMismatch("data".into()),
        Error::Changed("source".into()),
        Error::Protocol("malformed".into()),
        Error::Config("quota".into()),
        Error::Transport("invalid proof".into()),
        Error::Io(std::io::Error::other("disk")),
        Error::Remote {
            code: 1,
            text: "denied".into(),
        },
        Error::Remote {
            code: 6,
            text: "disk".into(),
        },
        Error::Remote {
            code: 8,
            text: "quota".into(),
        },
    ] {
        let mut error = Some(error);
        let calls = Cell::new(0);
        let result: Result<()> = run(&policy(), |_| {
            calls.set(calls.get() + 1);
            std::future::ready(Err(error.take().expect("fatal error was retried")))
        })
        .await;
        assert!(result.is_err());
        assert_eq!(calls.get(), 1);
    }
    let calls = Cell::new(0);
    let _: Result<()> = run(&Policy::default(), |_| {
        calls.set(calls.get() + 1);
        std::future::ready(Err(Error::Connection("closed".into())))
    })
    .await;
    assert_eq!(calls.get(), 1);
}
#[tokio::test(start_paused = true)]
async fn total_budget_cancels_waiting_attempt_and_backoff() {
    struct Guard<'a>(&'a Cell<bool>);
    impl Drop for Guard<'_> {
        fn drop(&mut self) {
            self.0.set(true);
        }
    }
    let dropped = Cell::new(false);
    let mut p = policy();
    p.max_elapsed_seconds = 3;
    let started = tokio::time::Instant::now();
    let result: Result<()> = run(&p, |_| async {
        let _guard = Guard(&dropped);
        tokio::time::sleep(Duration::from_secs(100)).await;
        Ok(())
    })
    .await;
    assert!(result.is_err());
    assert!(dropped.get());
    assert_eq!(started.elapsed(), Duration::from_secs(3));
    p.delay_seconds = 5;
    let calls = Cell::new(0);
    let _: Result<()> = run(&p, |_| {
        calls.set(calls.get() + 1);
        std::future::ready(Err(Error::Busy))
    })
    .await;
    assert_eq!(calls.get(), 1);
}
