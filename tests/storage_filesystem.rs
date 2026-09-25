//! Run on a mounted filesystem with:
//! RRSYNC_TEST_FILESYSTEM=/mount/private-dir cargo test --test storage_filesystem -- --ignored
//! Only a randomly named child directory is modified and removed.
use rrsync::{
    chunks::{Description, Store, scope},
    fs::Root,
};
use std::{
    fs,
    io::{BufRead, BufReader, Cursor, Read, Write},
    path::Path,
    process::{Command, Stdio},
};

fn data() -> Vec<u8> {
    (0..700_001).map(|i| (i % 251) as u8).collect()
}
fn description(bytes: &[u8]) -> Description {
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(bytes).unwrap();
    Description::scan(&mut file, 262_144).unwrap()
}
fn context() -> [u8; 32] {
    scope([11; 16], true, "/export", "payload").unwrap()
}
fn directory(root: &Path) -> std::path::PathBuf {
    fs::read_dir(root)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.is_dir())
        .unwrap()
}

#[test]
#[ignore = "requires an explicitly selected mounted filesystem"]
fn persistent_chunks_on_selected_filesystem() {
    const CHILD: &str = "RRSYNC_FILESYSTEM_TEST_CHILD";
    let bytes = data();
    let description = description(&bytes);
    if let Some(path) = std::env::var_os(CHILD) {
        let store = Store::open(Path::new(&path), context(), description).unwrap();
        store
            .receive(0, &mut Cursor::new(&bytes[..262_144]))
            .unwrap();
        println!("CHUNK_READY");
        std::io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    }
    let selected = std::env::var_os("RRSYNC_TEST_FILESYSTEM")
        .expect("set RRSYNC_TEST_FILESYSTEM to a writable directory on the filesystem under test");
    let fixture = tempfile::Builder::new()
        .prefix("rrsync-fs-")
        .tempdir_in(selected)
        .unwrap();
    let state = fixture.path().join("state");
    fs::create_dir(&state).unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "persistent_chunks_on_selected_filesystem",
            "--ignored",
            "--nocapture",
        ])
        .env(CHILD, &state)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (sender, receiver) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if line.unwrap().contains("CHUNK_READY") {
                sender.send(()).unwrap();
                break;
            }
        }
    });
    let ready = receiver.recv_timeout(std::time::Duration::from_secs(15));
    child.kill().unwrap();
    child.wait().unwrap();
    reader.join().unwrap();
    ready.unwrap();
    let store = Store::open(&state, context(), description.clone()).unwrap();
    assert_eq!(store.missing(0).unwrap(), vec![1, 2]);
    assert!(Store::open(&state, context(), description.clone()).is_err());
    // Cached data does not depend on exact timestamps, Unix modes or hard links.
    let root = Root::open(&directory(&state)).unwrap();
    root.set_mtime("00000000/00000000.chunk", 1_700_000_001, 123_456_789)
        .unwrap();
    assert_eq!(store.missing(0).unwrap(), vec![1, 2]);
    for (index, part) in bytes.chunks(262_144).enumerate() {
        store.receive(index, &mut Cursor::new(part)).unwrap();
    }
    assert!(
        store
            .receive(0, &mut Cursor::new(vec![0; 262_144]))
            .is_err()
    );
    assert!(store.missing(0).unwrap().is_empty());
    fs::write(
        directory(&state).join("00000000/00000001.chunk"),
        b"damaged",
    )
    .unwrap();
    assert_eq!(store.missing(0).unwrap(), vec![1]);
    store
        .receive(1, &mut Cursor::new(&bytes[262_144..524_288]))
        .unwrap();
    let mut assembled = store.assemble().unwrap();
    let mut actual = Vec::new();
    assembled.read_to_end(&mut actual).unwrap();
    assert_eq!(actual, bytes);
    let destination = Root::open(fixture.path()).unwrap();
    fs::write(fixture.path().join("installed"), b"old").unwrap();
    // Exercise installation rename and fsync on the selected filesystem too.
    destination
        .stage(
            "installed",
            &mut Cursor::new(&actual),
            actual.len() as u64,
            Some(description.hash()),
            1_700_000_001,
            123_456_789,
        )
        .unwrap()
        .commit()
        .unwrap();
    assert_eq!(fs::read(fixture.path().join("installed")).unwrap(), bytes);
    drop(store);
    let store = Store::open(&state, context(), description).unwrap();
    assert!(store.missing(0).unwrap().is_empty());
    store.clear().unwrap();
    assert_eq!(fs::read_dir(directory(&state)).unwrap().count(), 2);
}

#[test]
#[ignore = "requires an explicitly selected mounted filesystem"]
fn quota_and_cache_lock_on_selected_filesystem() {
    use rrsync::chunks::Limits;
    let selected = std::env::var_os("RRSYNC_TEST_FILESYSTEM").expect("set RRSYNC_TEST_FILESYSTEM");
    let fixture = tempfile::Builder::new()
        .prefix("rrsync-quota-")
        .tempdir_in(selected)
        .unwrap();
    let bytes = data();
    let d = description(&bytes);
    let limits = Limits {
        max_bytes: bytes.len() as u64,
        max_transfers: 4,
        retention_seconds: 0,
    };
    let store = Store::open_limited(fixture.path(), context(), d.clone(), limits).unwrap();
    store
        .receive(0, &mut Cursor::new(&bytes[..262144]))
        .unwrap();
    assert!(Store::open_limited(fixture.path(), [1; 32], d.clone(), limits).is_err());
    drop(store);
    assert!(Store::open_limited(fixture.path(), [1; 32], d.clone(), limits).is_err());
    let store = Store::open_limited(fixture.path(), context(), d, limits).unwrap();
    assert_eq!(store.missing(0).unwrap(), vec![1, 2]);
    for (i, part) in bytes.chunks(262144).enumerate().skip(1) {
        store.receive(i, &mut Cursor::new(part)).unwrap();
    }
    let mut actual = Vec::new();
    store.assemble().unwrap().read_to_end(&mut actual).unwrap();
    assert_eq!(actual, bytes);
    store.clear().unwrap();
}
