mod support;
use rrsync::{
    config::Permission,
    engine::{self, Server, SyncRequest},
    fs::Root,
    sync::{self, Options},
};
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use support::{Fault, SimulatedTransport};

struct Fixture {
    local: tempfile::TempDir,
    remote: tempfile::TempDir,
    server: Arc<Mutex<Server>>,
}
impl Fixture {
    fn new() -> Self {
        let local = tempfile::tempdir().unwrap();
        let remote = tempfile::tempdir().unwrap();
        let server = Arc::new(Mutex::new(Server::new(Root::open(remote.path()).unwrap())));
        Self {
            local,
            remote,
            server,
        }
    }
    fn link(&self, id: u8) -> SimulatedTransport {
        SimulatedTransport::new(
            Root::open(self.remote.path()).unwrap(),
            self.server.clone(),
            [id; 16],
        )
    }
    fn prepare(&self, push: bool) {
        let (source, destination) = if push {
            (self.local.path(), self.remote.path())
        } else {
            (self.remote.path(), self.local.path())
        };
        std::fs::create_dir(source.join("empty-dir")).unwrap();
        std::fs::write(source.join("a-first"), b"first committed").unwrap();
        std::fs::write(source.join("b-large"), vec![42; 256 * 1024]).unwrap();
        std::fs::write(source.join("empty-file"), b"").unwrap();
        std::fs::write(source.join("данные"), b"unicode").unwrap();
        std::fs::write(destination.join("b-large"), b"previous version").unwrap();
        std::fs::write(destination.join("extra"), b"keep until finish").unwrap();
    }
    async fn run(
        &self,
        link: &mut SimulatedTransport,
        push: bool,
        dry_run: bool,
    ) -> rrsync::Result<()> {
        let root = Root::open(self.local.path())?;
        let scan = sync::scan(&root, true)?;
        engine::synchronize(
            link,
            &root,
            scan,
            SyncRequest {
                push,
                path: String::new(),
                options: Options {
                    delete: true,
                    checksum: true,
                    dry_run,
                },
                deadline: Duration::from_secs(10),
            },
            |_| {},
        )
        .await
    }
    fn destination(&self, push: bool) -> &Path {
        if push {
            self.remote.path()
        } else {
            self.local.path()
        }
    }
    fn assert_equal(&self) {
        let left = sync::scan(&Root::open(self.local.path()).unwrap(), true).unwrap();
        let right = sync::scan(&Root::open(self.remote.path()).unwrap(), true).unwrap();
        assert_eq!(left.manifest, right.manifest);
    }
}
#[tokio::test(start_paused = true)]
async fn push_pull_and_repeat_use_the_same_engine_without_reticulum() {
    for push in [true, false] {
        let f = Fixture::new();
        f.prepare(push);
        let mut link = f.link(1);
        f.run(&mut link, push, false).await.unwrap();
        f.assert_equal();
        assert!(link.stats.closed);
        assert_eq!(
            if push {
                link.stats.uploads
            } else {
                link.stats.downloads
            },
            4
        );
        let mut repeat = f.link(2);
        f.run(&mut repeat, push, false).await.unwrap();
        assert_eq!((repeat.stats.uploads, repeat.stats.downloads), (0, 0));
    }
}
#[tokio::test(start_paused = true)]
async fn dry_run_and_read_permissions_preserve_destination() {
    for push in [true, false] {
        let f = Fixture::new();
        f.prepare(push);
        let destination = Root::open(f.destination(push)).unwrap();
        let before = sync::scan(&destination, true).unwrap().manifest;
        let mut link = f.link(1);
        f.run(&mut link, push, true).await.unwrap();
        assert_eq!(before, sync::scan(&destination, true).unwrap().manifest);
        assert_eq!(link.stats.requests, vec![1, 7]);
        assert_eq!((link.stats.uploads, link.stats.downloads), (0, 0));
        let mut reader = f.link(2);
        reader.permission = Permission::Read;
        let result = f.run(&mut reader, push, false).await;
        if push {
            assert!(result.is_err());
            assert_eq!(before, sync::scan(&destination, true).unwrap().manifest);
        } else {
            result.unwrap();
            f.assert_equal();
        }
    }
}
#[tokio::test(start_paused = true)]
async fn interrupted_push_and_pull_preserve_current_file_and_recover() {
    for push in [true, false] {
        let f = Fixture::new();
        f.prepare(push);
        let mut link = f.link(1);
        link.fault = Some(if push {
            Fault::InterruptUpload(2)
        } else {
            Fault::InterruptDownload(2)
        });
        assert!(f.run(&mut link, push, false).await.is_err());
        assert!(link.stats.closed);
        let target = f.destination(push);
        assert_eq!(
            std::fs::read(target.join("a-first")).unwrap(),
            b"first committed"
        );
        assert_eq!(
            std::fs::read(target.join("b-large")).unwrap(),
            b"previous version"
        );
        assert!(target.join("extra").exists());
        let mut retry = f.link(2);
        f.run(&mut retry, push, false).await.unwrap();
        f.assert_equal();
        assert_eq!(
            if push {
                retry.stats.uploads
            } else {
                retry.stats.downloads
            },
            3
        );
    }
}
#[tokio::test(start_paused = true)]
async fn lost_commit_response_never_replays_a_mutation() {
    let f = Fixture::new();
    f.prepare(true);
    let mut link = f.link(1);
    link.fault = Some(Fault::DropResponse(4));
    assert!(f.run(&mut link, true, false).await.is_err());
    assert_eq!(
        link.stats.requests.iter().filter(|&&tag| tag == 4).count(),
        1
    );
    assert_eq!(
        std::fs::read(f.remote.path().join("a-first")).unwrap(),
        b"first committed"
    );
    assert!(f.remote.path().join("extra").exists());
    let mut retry = f.link(2);
    f.run(&mut retry, true, false).await.unwrap();
    f.assert_equal();
    assert_eq!(retry.stats.uploads, 3);
}
#[tokio::test(start_paused = true)]
async fn lost_finish_response_is_recovered_by_rescan_in_both_directions() {
    for push in [true, false] {
        let f = Fixture::new();
        f.prepare(push);
        let mut link = f.link(1);
        link.fault = Some(Fault::DropResponse(7));
        assert!(f.run(&mut link, push, false).await.is_err());
        // The push server finished; the pull client has no confirmation to delete.
        assert_eq!(f.destination(push).join("extra").exists(), !push);
        let mut retry = f.link(2);
        f.run(&mut retry, push, false).await.unwrap();
        f.assert_equal();
        assert_eq!((retry.stats.uploads, retry.stats.downloads), (0, 0));
    }
}
#[tokio::test(start_paused = true)]
async fn lost_requests_and_duplicates_fail_closed_then_recover() {
    for fault in [
        Fault::DropRequest(3),
        Fault::DropRequest(4),
        Fault::DuplicateRequest(3),
        Fault::DuplicateRequest(4),
        Fault::FinishBeforeCommit,
        Fault::DropRequest(7),
    ] {
        let f = Fixture::new();
        f.prepare(true);
        let mut link = f.link(1);
        link.fault = Some(fault);
        assert!(f.run(&mut link, true, false).await.is_err());
        assert!(f.remote.path().join("extra").exists());
        let mut retry = f.link(2);
        f.run(&mut retry, true, false).await.unwrap();
        f.assert_equal();
    }
}
#[tokio::test(start_paused = true)]
async fn delayed_responses_respect_deadlines() {
    for delay in [Duration::from_secs(5), Duration::from_secs(15)] {
        let f = Fixture::new();
        f.prepare(true);
        let mut link = f.link(1);
        link.fault = Some(Fault::DelayResponse(3, delay));
        let result = f.run(&mut link, true, false).await;
        if delay < Duration::from_secs(10) {
            result.unwrap();
            f.assert_equal();
        } else {
            assert!(result.is_err());
            assert_eq!(link.stats.uploads, 0);
            assert!(f.remote.path().join("extra").exists());
        }
    }
}
#[tokio::test(start_paused = true)]
async fn corrupted_upload_never_installs_or_deletes() {
    let f = Fixture::new();
    f.prepare(true);
    let mut link = f.link(1);
    link.fault = Some(Fault::CorruptUpload(2));
    assert!(f.run(&mut link, true, false).await.is_err());
    assert_eq!(
        std::fs::read(f.remote.path().join("b-large")).unwrap(),
        b"previous version"
    );
    assert!(f.remote.path().join("extra").exists());
    let mut retry = f.link(2);
    f.run(&mut retry, true, false).await.unwrap();
    f.assert_equal();
}
#[tokio::test(start_paused = true)]
async fn server_restart_discards_uncommitted_transfer_state() {
    let f = Fixture::new();
    f.prepare(true);
    let mut link = f.link(1);
    link.fault = Some(Fault::RestartServerAfterUpload(2));
    assert!(f.run(&mut link, true, false).await.is_err());
    assert_eq!(
        std::fs::read(f.remote.path().join("b-large")).unwrap(),
        b"previous version"
    );
    assert!(f.remote.path().join("extra").exists());
    let mut retry = f.link(2);
    f.run(&mut retry, true, false).await.unwrap();
    f.assert_equal();
    assert_eq!(retry.stats.uploads, 3);
}
#[tokio::test(start_paused = true)]
async fn lost_close_keeps_export_busy_until_lease_expires() {
    let f = Fixture::new();
    f.prepare(true);
    let mut link = f.link(1);
    link.fault = Some(Fault::InterruptUpload(2));
    link.close_is_lost = true;
    assert!(f.run(&mut link, true, false).await.is_err());
    let mut busy = f.link(2);
    assert!(f.run(&mut busy, true, false).await.is_err());
    assert_eq!(
        f.server.lock().unwrap().expire(
            Instant::now() + Duration::from_secs(30),
            Duration::from_secs(10)
        ),
        Some([1; 16])
    );
    let mut retry = f.link(3);
    f.run(&mut retry, true, false).await.unwrap();
    f.assert_equal();
}
