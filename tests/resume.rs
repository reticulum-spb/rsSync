#[path = "support/v2.rs"]
mod support;
use rrsync::{
    config::Permission,
    engine::{
        self,
        v2::{self, Cache, Resume, Server},
    },
    fs::Root,
    protocol::{Message as Common, v2::Message},
    sync::{self, Options},
};
use std::{
    fs,
    sync::{Arc, Mutex},
    time::Duration,
};
use support::{Fault, SimulatedTransport};

struct Fixture {
    _tmp: tempfile::TempDir,
    local: std::path::PathBuf,
    remote: std::path::PathBuf,
    server_cache: Cache,
    client_cache: Cache,
    server: Arc<Mutex<Server>>,
    bytes: Vec<u8>,
}
impl Fixture {
    fn new(push: bool) -> Self {
        let tmp = match std::env::var_os("RRSYNC_RESUME_TEST_FILESYSTEM") {
            Some(path) => tempfile::Builder::new()
                .prefix("rrsync-v2-")
                .tempdir_in(path)
                .unwrap(),
            None => tempfile::tempdir().unwrap(),
        };
        let local = tmp.path().join("local");
        let remote = tmp.path().join("remote");
        let sc = tmp.path().join("server-state");
        let cc = tmp.path().join("client-state");
        for path in [&local, &remote, &sc, &cc] {
            fs::create_dir(path).unwrap();
        }
        let bytes: Vec<u8> = (0..11000).map(|i| (i % 251) as u8).collect();
        let (source, destination) = if push {
            (&local, &remote)
        } else {
            (&remote, &local)
        };
        fs::write(source.join("file"), &bytes).unwrap();
        fs::write(destination.join("file"), b"old").unwrap();
        fs::write(destination.join("extra"), b"keep until finish").unwrap();
        let server_cache = Cache {
            directory: sc,
            root_id: remote.to_str().unwrap().into(),
        };
        let client_cache = Cache {
            directory: cc,
            root_id: local.to_str().unwrap().into(),
        };
        let server = Arc::new(Mutex::new(Server::new(
            Root::open(&remote).unwrap(),
            server_cache.clone(),
        )));
        Self {
            _tmp: tmp,
            local,
            remote,
            server_cache,
            client_cache,
            server,
            bytes,
        }
    }
    fn link(&self) -> SimulatedTransport {
        SimulatedTransport::new(
            Root::open(&self.remote).unwrap(),
            self.server.clone(),
            [1; 16],
            self.server_cache.clone(),
        )
    }
    async fn run(
        &self,
        link: &mut SimulatedTransport,
        push: bool,
        dry_run: bool,
    ) -> rrsync::Result<()> {
        let root = Root::open(&self.local)?;
        v2::synchronize(
            link,
            &root,
            sync::scan(&root, true)?,
            engine::SyncRequest {
                push,
                path: String::new(),
                options: Options {
                    delete: true,
                    checksum: true,
                    dry_run,
                },
                deadline: Duration::from_secs(1),
            },
            Resume {
                cache: self.client_cache.clone(),
                peer: [8; 16],
                chunk_size: 4096,
            },
            |_| {},
        )
        .await
    }
    fn target(&self, push: bool) -> &std::path::Path {
        if push { &self.remote } else { &self.local }
    }
    fn intact(&self, push: bool) {
        assert_eq!(fs::read(self.target(push).join("file")).unwrap(), b"old");
        assert!(self.target(push).join("extra").exists());
    }
    fn complete(&self, push: bool) {
        assert_eq!(
            fs::read(self.target(push).join("file")).unwrap(),
            self.bytes
        );
        assert!(!self.target(push).join("extra").exists());
    }
}
#[tokio::test]
async fn push_pull_and_repeat_with_empty_files() {
    for push in [true, false] {
        let f = Fixture::new(push);
        let source = if push { &f.local } else { &f.remote };
        fs::write(source.join("empty"), []).unwrap();
        fs::create_dir(source.join("empty-dir")).unwrap();
        let mut link = f.link();
        f.run(&mut link, push, false).await.unwrap();
        f.complete(push);
        assert_eq!(link.stats.uploads + link.stats.downloads, 3);
        assert!(f.target(push).join("empty-dir").is_dir());
        assert_eq!(fs::read(f.target(push).join("empty")).unwrap(), b"");
        let mut repeat = f.link();
        f.run(&mut repeat, push, false).await.unwrap();
        assert_eq!(repeat.stats.uploads + repeat.stats.downloads, 0);
    }
}
#[tokio::test]
async fn interrupted_chunk_reuses_only_persisted_chunks_in_both_directions() {
    for push in [true, false] {
        let f = Fixture::new(push);
        let mut first = f.link();
        first.fault = Some(if push {
            Fault::InterruptUpload(2)
        } else {
            Fault::InterruptDownload(2)
        });
        assert!(f.run(&mut first, push, false).await.is_err());
        f.intact(push);
        let mut retry = f.link();
        retry.connection = [2; 16];
        f.run(&mut retry, push, false).await.unwrap();
        f.complete(push);
        assert_eq!(
            retry.stats.upload_bytes + retry.stats.download_bytes,
            f.bytes.len() as u64 - 4096
        );
        assert_eq!(retry.stats.uploads + retry.stats.downloads, 2);
    }
}
#[tokio::test(start_paused = true)]
async fn lost_chunk_ack_and_final_ack_recover_without_replaying_mutations() {
    for (push, tag, remaining) in [
        (true, 14, 6904),
        (false, 16, 6904),
        (true, 17, 0),
        (false, 18, 0),
    ] {
        let f = Fixture::new(push);
        let mut first = f.link();
        first.fault = Some(Fault::DropResponse(tag));
        assert!(f.run(&mut first, push, false).await.is_err());
        assert_eq!(
            first.stats.requests.iter().filter(|&&t| t == tag).count(),
            1
        );
        if tag != 17 {
            f.intact(push);
        }
        assert!(f.target(push).join("extra").exists());
        let mut retry = f.link();
        f.run(&mut retry, push, false).await.unwrap();
        f.complete(push);
        assert_eq!(
            retry.stats.upload_bytes + retry.stats.download_bytes,
            remaining
        );
    }
}
#[tokio::test]
async fn server_restart_and_source_change_do_not_trust_stale_state() {
    let f = Fixture::new(true);
    let mut first = f.link();
    first.fault = Some(Fault::RestartServerAfterUpload(2));
    assert!(f.run(&mut first, true, false).await.is_err());
    f.intact(true);
    let mut retry = f.link();
    f.run(&mut retry, true, false).await.unwrap();
    f.complete(true);
    assert_eq!(retry.stats.upload_bytes, f.bytes.len() as u64 - 4096);
    let f = Fixture::new(true);
    let mut first = f.link();
    first.fault = Some(Fault::InterruptUpload(2));
    assert!(f.run(&mut first, true, false).await.is_err());
    let mut changed = f.bytes.clone();
    changed[0] ^= 1;
    fs::write(f.local.join("file"), &changed).unwrap();
    let mut retry = f.link();
    f.run(&mut retry, true, false).await.unwrap();
    assert_eq!(retry.stats.upload_bytes, changed.len() as u64);
    assert_eq!(fs::read(f.remote.join("file")).unwrap(), changed);
}
#[tokio::test]
async fn changed_identity_and_corruption_cannot_reuse_unverified_data() {
    let f = Fixture::new(true);
    let mut first = f.link();
    first.fault = Some(Fault::InterruptUpload(2));
    assert!(f.run(&mut first, true, false).await.is_err());
    let mut retry = f.link();
    retry.peer = [9; 16];
    f.run(&mut retry, true, false).await.unwrap();
    assert_eq!(retry.stats.upload_bytes, f.bytes.len() as u64);
    let f = Fixture::new(true);
    let mut first = f.link();
    first.fault = Some(Fault::CorruptUpload(2));
    assert!(f.run(&mut first, true, false).await.is_err());
    f.intact(true);
    let mut retry = f.link();
    f.run(&mut retry, true, false).await.unwrap();
    f.complete(true);
    assert_eq!(retry.stats.upload_bytes, f.bytes.len() as u64 - 4096);
}
#[tokio::test]
async fn dry_run_and_permissions_do_not_create_receiver_state() {
    for push in [true, false] {
        let f = Fixture::new(push);
        let mut link = f.link();
        f.run(&mut link, push, true).await.unwrap();
        f.intact(push);
        assert_eq!(fs::read_dir(&f.server_cache.directory).unwrap().count(), 0);
        assert_eq!(fs::read_dir(&f.client_cache.directory).unwrap().count(), 0);
        link.permission = Permission::Read;
        if push {
            assert!(f.run(&mut link, push, false).await.is_err());
            f.intact(push);
        } else {
            f.run(&mut link, push, false).await.unwrap();
            f.complete(push);
        }
    }
}
#[tokio::test]
async fn invalid_order_and_early_finish_never_delete_or_install() {
    for fault in [Fault::DuplicateRequest(13), Fault::FinishBeforeCommit] {
        let f = Fixture::new(true);
        let mut link = f.link();
        link.fault = Some(fault);
        assert!(f.run(&mut link, true, false).await.is_err());
        f.intact(true);
    }
}
#[test]
fn peer_permission_change_and_lease_release_cache_lock() {
    let f = Fixture::new(true);
    let manifest = sync::scan(&Root::open(&f.local).unwrap(), true)
        .unwrap()
        .manifest;
    let start = || {
        Message::Common(Common::Start {
            push: true,
            options: Options {
                delete: true,
                checksum: true,
                dry_run: false,
            },
            path: String::new(),
            manifest: manifest.clone(),
        })
    };
    let mut server = f.server.lock().unwrap();
    server
        .handle([1; 16], [7; 16], Permission::Full, start())
        .unwrap();
    assert!(
        server
            .handle([2; 16], [7; 16], Permission::Full, start())
            .is_err()
    );
    assert!(server.active());
    assert!(
        server
            .handle(
                [1; 16],
                [7; 16],
                Permission::Read,
                Message::Common(Common::Finish)
            )
            .is_err()
    );
    assert!(!server.active());
    server
        .handle([1; 16], [7; 16], Permission::Full, start())
        .unwrap();
    let mut source = fs::File::open(f.local.join("file")).unwrap();
    let d = rrsync::chunks::Description::scan(&mut source, 4096).unwrap();
    server
        .handle(
            [1; 16],
            [7; 16],
            Permission::Full,
            Message::Description {
                index: 0,
                description: d.clone(),
            },
        )
        .unwrap();
    server
        .handle(
            [1; 16],
            [7; 16],
            Permission::Full,
            Message::ChunkBegin { index: 0, chunk: 0 },
        )
        .unwrap();
    assert!(server.accepts_file([1; 16], 4096, false));
    assert!(!server.accepts_file([2; 16], 4096, false));
    assert!(!server.accepts_file([1; 16], 4096, true));
    assert!(!server.accepts_file([1; 16], 1, false));
    assert_eq!(
        server.expire(
            std::time::Instant::now() + Duration::from_secs(2),
            Duration::from_secs(1)
        ),
        Some([1; 16])
    );
    server
        .handle([2; 16], [7; 16], Permission::Full, start())
        .unwrap();
    server
        .handle(
            [2; 16],
            [7; 16],
            Permission::Full,
            Message::Description {
                index: 0,
                description: d,
            },
        )
        .unwrap();
    assert!(
        server
            .handle(
                [2; 16],
                [9; 16],
                Permission::Full,
                Message::ChunkBegin { index: 0, chunk: 0 }
            )
            .is_err()
    );
    f.intact(true);
}

#[tokio::test(start_paused = true)]
async fn request_loss_deadlines_and_finish_ambiguity() {
    for fault in [
        Fault::DropRequest(14),
        Fault::DelayResponse(14, Duration::from_secs(2)),
    ] {
        let f = Fixture::new(true);
        let mut first = f.link();
        first.fault = Some(fault);
        assert!(f.run(&mut first, true, false).await.is_err());
        f.intact(true);
        let mut retry = f.link();
        f.run(&mut retry, true, false).await.unwrap();
        f.complete(true);
        let expected = if matches!(fault, Fault::DropRequest(_)) {
            11000
        } else {
            6904
        };
        assert_eq!(retry.stats.upload_bytes, expected);
    }
    for push in [true, false] {
        let f = Fixture::new(push);
        let mut first = f.link();
        first.fault = Some(Fault::DropResponse(7));
        assert!(f.run(&mut first, push, false).await.is_err());
        assert_eq!(fs::read(f.target(push).join("file")).unwrap(), f.bytes);
        assert_eq!(f.target(push).join("extra").exists(), !push);
        let mut retry = f.link();
        f.run(&mut retry, push, false).await.unwrap();
        f.complete(push);
        assert_eq!(retry.stats.uploads + retry.stats.downloads, 0);
    }
}

#[test]
fn repeated_start_releases_pending_store_for_new_session() {
    let f = Fixture::new(true);
    let manifest = sync::scan(&Root::open(&f.local).unwrap(), true)
        .unwrap()
        .manifest;
    let start = || {
        Message::Common(Common::Start {
            push: true,
            options: Options::default(),
            path: String::new(),
            manifest: manifest.clone(),
        })
    };
    let mut file = fs::File::open(f.local.join("file")).unwrap();
    let description = rrsync::chunks::Description::scan(&mut file, 4096).unwrap();
    let mut server = f.server.lock().unwrap();
    server
        .handle([1; 16], [7; 16], Permission::Full, start())
        .unwrap();
    server
        .handle(
            [1; 16],
            [7; 16],
            Permission::Full,
            Message::Description {
                index: 0,
                description: description.clone(),
            },
        )
        .unwrap();
    assert!(
        server
            .handle([1; 16], [7; 16], Permission::Full, start())
            .is_err()
    );
    assert!(!server.active());
    server
        .handle([2; 16], [7; 16], Permission::Full, start())
        .unwrap();
    server
        .handle(
            [2; 16],
            [7; 16],
            Permission::Full,
            Message::Description {
                index: 0,
                description,
            },
        )
        .unwrap();
}

#[tokio::test]
async fn persisted_corruption_and_missing_close_require_revalidation_and_lease() {
    let f = Fixture::new(true);
    let mut first = f.link();
    first.fault = Some(Fault::InterruptUpload(2));
    first.close_is_lost = true;
    assert!(f.run(&mut first, true, false).await.is_err());
    let mut busy = f.link();
    busy.connection = [2; 16];
    assert!(f.run(&mut busy, true, false).await.is_err());
    assert!(f.server.lock().unwrap().active());
    f.server.lock().unwrap().expire(
        std::time::Instant::now() + Duration::from_secs(2),
        Duration::from_secs(1),
    );
    let transfer = fs::read_dir(&f.server_cache.directory)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    fs::write(transfer.join("00000000.chunk"), vec![0; 4096]).unwrap();
    let mut retry = f.link();
    f.run(&mut retry, true, false).await.unwrap();
    f.complete(true);
    assert_eq!(retry.stats.upload_bytes, f.bytes.len() as u64);
}

#[test]
fn wrong_resource_receipt_and_changed_pull_source_abort_before_install() {
    let f = Fixture::new(true);
    let manifest = sync::scan(&Root::open(&f.local).unwrap(), true)
        .unwrap()
        .manifest;
    let mut source = fs::File::open(f.local.join("file")).unwrap();
    let description = rrsync::chunks::Description::scan(&mut source, 4096).unwrap();
    let mut server = f.server.lock().unwrap();
    server
        .handle(
            [1; 16],
            [7; 16],
            Permission::Full,
            Message::Common(Common::Start {
                push: true,
                options: Options::default(),
                path: String::new(),
                manifest,
            }),
        )
        .unwrap();
    server
        .handle(
            [1; 16],
            [7; 16],
            Permission::Full,
            Message::Description {
                index: 0,
                description,
            },
        )
        .unwrap();
    server
        .handle(
            [1; 16],
            [7; 16],
            Permission::Full,
            Message::ChunkBegin { index: 0, chunk: 0 },
        )
        .unwrap();
    use std::io::{Seek, Write};
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(&f.bytes[..4096]).unwrap();
    file.rewind().unwrap();
    server.receive(
        [1; 16],
        engine::ReceivedFile {
            transfer_id: [3; 32],
            file,
            size: 4096,
            has_metadata: false,
        },
    );
    assert!(
        server
            .handle(
                [1; 16],
                [7; 16],
                Permission::Full,
                Message::ChunkCommit {
                    index: 0,
                    chunk: 0,
                    resource: [4; 32]
                }
            )
            .is_err()
    );
    f.intact(true);
    let f = Fixture::new(false);
    let manifest = sync::scan(&Root::open(&f.local).unwrap(), true)
        .unwrap()
        .manifest;
    let mut server = f.server.lock().unwrap();
    server
        .handle(
            [1; 16],
            [7; 16],
            Permission::Read,
            Message::Common(Common::Start {
                push: false,
                options: Options {
                    delete: true,
                    ..Options::default()
                },
                path: String::new(),
                manifest,
            }),
        )
        .unwrap();
    server
        .handle(
            [1; 16],
            [7; 16],
            Permission::Read,
            Message::Describe {
                index: 0,
                chunk_size: 4096,
            },
        )
        .unwrap();
    fs::write(f.remote.join("file"), b"source changed after snapshot").unwrap();
    assert!(matches!(
        server.handle([1; 16], [7; 16], Permission::Read, Message::FileVerified(0)),
        Err(rrsync::Error::Changed(_))
    ));
    f.intact(false);
}
