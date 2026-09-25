use rrsync::{
    Error,
    config::Permission,
    engine::{ConnectionId, ReceivedFile, Server, ServerReply},
    fs::Root,
    protocol::Message,
    sync::{self, Manifest, Options},
};
use std::{
    io::Write,
    time::{Duration, Instant},
};
const OWNER: ConnectionId = [1; 16];
const OTHER: ConnectionId = [2; 16];
fn start(push: bool, dry_run: bool, manifest: Manifest) -> Message {
    Message::Start {
        push,
        options: Options {
            delete: true,
            dry_run,
            checksum: false,
        },
        path: String::new(),
        manifest,
    }
}
fn empty_start() -> Message {
    start(true, false, Manifest::default())
}
fn file(bytes: &[u8]) -> ReceivedFile {
    use std::io::Seek;
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(bytes).unwrap();
    file.rewind().unwrap();
    ReceivedFile {
        transfer_id: [3; 32],
        file,
        size: bytes.len() as u64,
        has_metadata: false,
    }
}
#[test]
fn dry_run_rejects_mutations_and_aborts_only_its_session() {
    let temp = tempfile::tempdir().unwrap();
    let mut server = Server::new(Root::open(temp.path()).unwrap());
    server
        .handle(
            OWNER,
            Permission::Full,
            Message::Start {
                push: true,
                options: Options {
                    dry_run: true,
                    ..Default::default()
                },
                path: "missing".into(),
                manifest: Manifest::default(),
            },
        )
        .unwrap();
    assert!(
        server
            .handle(OWNER, Permission::Full, Message::Begin(0))
            .is_err()
    );
    assert!(
        server
            .handle(OWNER, Permission::Full, Message::Finish)
            .is_err()
    );
    assert!(!temp.path().join("missing").exists());
    server
        .handle(
            OTHER,
            Permission::Full,
            start(true, true, Manifest::default()),
        )
        .unwrap();
    server
        .handle(OTHER, Permission::Full, Message::Finish)
        .unwrap();
}
#[test]
fn read_only_identity_can_pull_but_cannot_push() {
    let temp = tempfile::tempdir().unwrap();
    let mut server = Server::new(Root::open(temp.path()).unwrap());
    for dry_run in [false, true] {
        assert!(matches!(
            server.handle(
                OWNER,
                Permission::Read,
                start(true, dry_run, Manifest::default())
            ),
            Err(Error::PermissionDenied)
        ));
    }
    server
        .handle(
            OWNER,
            Permission::Read,
            start(false, false, Manifest::default()),
        )
        .unwrap();
    server
        .handle(OWNER, Permission::Read, Message::Finish)
        .unwrap();
}
#[test]
fn unauthorized_and_busy_peers_cannot_release_owner_session() {
    let temp = tempfile::tempdir().unwrap();
    let mut server = Server::new(Root::open(temp.path()).unwrap());
    assert!(matches!(
        server.handle(OTHER, Permission::Deny, empty_start()),
        Err(Error::PermissionDenied)
    ));
    server
        .handle(OWNER, Permission::Full, empty_start())
        .unwrap();
    assert!(
        server
            .handle(OTHER, Permission::Full, empty_start())
            .is_err()
    );
    server.disconnect(OTHER);
    server
        .handle(OWNER, Permission::Full, Message::Finish)
        .unwrap();
}
#[test]
fn only_committed_uploads_allow_deleting_extras() {
    let source = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    std::fs::write(source.path().join("file"), b"new data").unwrap();
    std::fs::write(target.path().join("file"), b"old").unwrap();
    std::fs::write(target.path().join("extra"), b"extra").unwrap();
    let manifest = sync::scan(&Root::open(source.path()).unwrap(), false)
        .unwrap()
        .manifest;
    let mut server = Server::new(Root::open(target.path()).unwrap());
    server
        .handle(
            OWNER,
            Permission::Full,
            start(true, false, manifest.clone()),
        )
        .unwrap();
    // An early FINISH is a protocol error, and recovery requires a fresh session.
    assert!(
        server
            .handle(OWNER, Permission::Full, Message::Finish)
            .is_err()
    );
    server
        .handle(OWNER, Permission::Full, start(true, false, manifest))
        .unwrap();
    server
        .handle(OWNER, Permission::Full, Message::Begin(0))
        .unwrap();
    assert!(server.accepts_file(OWNER, 8, false));
    assert!(!server.accepts_file(OTHER, 8, false));
    assert!(!server.accepts_file(OWNER, 9, false));
    assert!(!server.accepts_file(OWNER, 8, true));
    server.receive(OTHER, file(b"intruder"));
    server.receive(OWNER, file(b"new data"));
    assert_eq!(std::fs::read(target.path().join("file")).unwrap(), b"old");
    server
        .handle(
            OWNER,
            Permission::Full,
            Message::Commit {
                index: 0,
                resource: [3; 32],
            },
        )
        .unwrap();
    assert_eq!(
        std::fs::read(target.path().join("file")).unwrap(),
        b"new data"
    );
    assert!(target.path().join("extra").exists());
    server
        .handle(OWNER, Permission::Full, Message::Finish)
        .unwrap();
    assert!(!target.path().join("extra").exists());
}
#[test]
fn mismatched_transfer_receipt_aborts_without_installing() {
    let source = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    std::fs::write(source.path().join("file"), b"new data").unwrap();
    std::fs::write(target.path().join("file"), b"old").unwrap();
    let manifest = sync::scan(&Root::open(source.path()).unwrap(), false)
        .unwrap()
        .manifest;
    let mut server = Server::new(Root::open(target.path()).unwrap());
    server
        .handle(OWNER, Permission::Full, start(true, false, manifest))
        .unwrap();
    server
        .handle(OWNER, Permission::Full, Message::Begin(0))
        .unwrap();
    server.receive(OWNER, file(b"new data"));
    assert!(
        server
            .handle(
                OWNER,
                Permission::Full,
                Message::Commit {
                    index: 0,
                    resource: [4; 32]
                }
            )
            .is_err()
    );
    assert_eq!(std::fs::read(target.path().join("file")).unwrap(), b"old");
    assert!(!server.accepts_file(OWNER, 8, false));
}
#[test]
fn activity_renews_lease_and_expiry_releases_export() {
    let temp = tempfile::tempdir().unwrap();
    let mut server = Server::new(Root::open(temp.path()).unwrap());
    server
        .handle(OWNER, Permission::Full, empty_start())
        .unwrap();
    let now = Instant::now();
    server.touch(OWNER, now);
    assert_eq!(
        server.expire(now + Duration::from_secs(5), Duration::from_secs(10)),
        None
    );
    // Activity from another connection does not renew the owner's lease.
    server.touch(OTHER, now + Duration::from_secs(20));
    assert_eq!(
        server.expire(now + Duration::from_secs(11), Duration::from_secs(10)),
        Some(OWNER)
    );
    assert!(matches!(
        server
            .handle(OTHER, Permission::Full, empty_start())
            .unwrap(),
        ServerReply::Message(Message::Manifest(_))
    ));
}
