use rrsync::chunks::{Description, MAX_CHUNK_SIZE, MAX_CHUNKS, MIN_CHUNK_SIZE, Store, scope};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{BufRead, BufReader, Cursor, Read, Write},
    os::unix::fs::symlink,
    process::{Command, Stdio},
};

fn bytes() -> Vec<u8> {
    (0..9500).map(|i| (i % 251) as u8).collect()
}
fn description(data: &[u8], size: u32) -> Description {
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(data).unwrap();
    Description::scan(&mut file, size).unwrap()
}
fn context() -> [u8; 32] {
    scope([7; 16], true, "/export", "file").unwrap()
}
fn chunk_dir(path: &std::path::Path) -> std::path::PathBuf {
    fs::read_dir(path)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.is_dir())
        .unwrap()
}
fn fill(store: &Store, data: &[u8]) {
    for (index, part) in data.chunks(MIN_CHUNK_SIZE as usize).enumerate() {
        store.receive(index, &mut Cursor::new(part)).unwrap();
    }
}
fn assembled(store: &Store) -> Vec<u8> {
    let mut data = Vec::new();
    store.assemble().unwrap().read_to_end(&mut data).unwrap();
    data
}

#[test]
fn geometry_and_hashes_are_bounded_and_deterministic() {
    for size in [0, MIN_CHUNK_SIZE - 1, MAX_CHUNK_SIZE + 1, u32::MAX] {
        assert!(Description::count(1, size).is_err());
    }
    assert!(Description::count(u64::MAX, MIN_CHUNK_SIZE).is_err());
    assert_eq!(
        Description::count(
            MAX_CHUNKS as u64 * u64::from(MIN_CHUNK_SIZE),
            MIN_CHUNK_SIZE
        )
        .unwrap(),
        MAX_CHUNKS
    );
    let data = bytes();
    let d = description(&data, MIN_CHUNK_SIZE);
    assert_eq!(d, description(&data, MIN_CHUNK_SIZE));
    assert_eq!(d.size(), 9500);
    assert_eq!(d.chunk_size(), 4096);
    assert_eq!(d.length(2).unwrap(), 1308);
    assert!(d.length(3).is_err());
    assert_eq!(d.hash(), <[u8; 32]>::from(Sha256::digest(&data)));
    for (part, hash) in data.chunks(4096).zip(d.hashes()) {
        assert_eq!(*hash, <[u8; 32]>::from(Sha256::digest(part)));
    }
    assert!(Description::new(9500, 4096, d.hash(), vec![]).is_err());
    assert!(Description::new(0, 4096, [1; 32], vec![]).is_err());
}

#[test]
fn killed_writer_leaves_only_verified_chunks_available() {
    const CHILD: &str = "RRSYNC_CHUNK_TEST_CHILD";
    if let Some(path) = std::env::var_os(CHILD) {
        let data = bytes();
        let store = Store::open(
            std::path::Path::new(&path),
            context(),
            description(&data, 4096),
        )
        .unwrap();
        store.receive(0, &mut Cursor::new(&data[..4096])).unwrap();
        // Simulate a staging file left by a kill before publication.
        fs::write(
            chunk_dir(std::path::Path::new(&path)).join(".rrsync-abandoned"),
            b"partial",
        )
        .unwrap();
        println!("CHUNK_READY");
        std::io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    }
    let tmp = tempfile::tempdir().unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "killed_writer_leaves_only_verified_chunks_available",
            "--nocapture",
        ])
        .env(CHILD, tmp.path())
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
    let ready = receiver.recv_timeout(std::time::Duration::from_secs(10));
    child.kill().unwrap();
    child.wait().unwrap();
    reader.join().unwrap();
    ready.unwrap();
    let data = bytes();
    let store = Store::open(tmp.path(), context(), description(&data, 4096)).unwrap();
    assert_eq!(store.missing().unwrap(), vec![1, 2]);
    store
        .receive(1, &mut Cursor::new(&data[4096..8192]))
        .unwrap();
    store.receive(2, &mut Cursor::new(&data[8192..])).unwrap();
    assert_eq!(assembled(&store), data);
    store.clear().unwrap();
    assert_eq!(fs::read_dir(chunk_dir(tmp.path())).unwrap().count(), 2); // lock and activity
}

#[test]
fn failed_or_duplicate_receive_preserves_verified_state() {
    let tmp = tempfile::tempdir().unwrap();
    let data = bytes();
    let store = Store::open(tmp.path(), context(), description(&data, 4096)).unwrap();
    store.receive(0, &mut Cursor::new(&data[..4096])).unwrap();
    for bad in [vec![0; 4096], vec![0; 4095], vec![0; 4097]] {
        assert!(store.receive(0, &mut Cursor::new(bad)).is_err());
        assert_eq!(store.missing().unwrap(), vec![1, 2]);
    }
    assert!(store.receive(3, &mut Cursor::new([])).is_err());
    assert!(store.assemble().is_err());
    fill(&store, &data);
    assert!(store.missing().unwrap().is_empty());
    assert_eq!(assembled(&store), data);
    assert_eq!(fs::read_dir(chunk_dir(tmp.path())).unwrap().count(), 5);
}

#[test]
fn corrupt_chunks_and_inconsistent_whole_hash_never_assemble() {
    let tmp = tempfile::tempdir().unwrap();
    let data = bytes();
    let d = description(&data, 4096);
    let store = Store::open(tmp.path(), context(), d.clone()).unwrap();
    fill(&store, &data);
    fs::write(chunk_dir(tmp.path()).join("00000001.chunk"), vec![0; 4096]).unwrap();
    assert_eq!(store.missing().unwrap(), vec![1]);
    assert!(store.assemble().is_err());
    store
        .receive(1, &mut Cursor::new(&data[4096..8192]))
        .unwrap();
    assert_eq!(assembled(&store), data);
    let bad = Description::new(d.size(), d.chunk_size(), [1; 32], d.hashes().to_vec()).unwrap();
    let other = Store::open(tmp.path(), context(), bad).unwrap();
    fill(&other, &data);
    assert!(other.missing().unwrap().is_empty());
    assert!(other.assemble().is_err());
}

#[test]
fn scope_and_description_changes_do_not_reuse_state() {
    let tmp = tempfile::tempdir().unwrap();
    let data = bytes();
    let d = description(&data, 4096);
    let store = Store::open(tmp.path(), context(), d.clone()).unwrap();
    fill(&store, &data);
    for changed in [
        scope([8; 16], true, "/export", "file"),
        scope([7; 16], false, "/export", "file"),
        scope([7; 16], true, "/other", "file"),
        scope([7; 16], true, "/export", "other"),
    ] {
        let other = Store::open(tmp.path(), changed.unwrap(), d.clone()).unwrap();
        assert_eq!(other.missing().unwrap(), vec![0, 1, 2]);
    }
    let other = Store::open(tmp.path(), context(), description(&data, 8192)).unwrap();
    assert_eq!(other.missing().unwrap(), vec![0, 1]);
    let mut changed = data.clone();
    changed[0] ^= 1;
    let other = Store::open(tmp.path(), context(), description(&changed, 4096)).unwrap();
    assert_eq!(other.missing().unwrap(), vec![0, 1, 2]);
    assert!(scope([7; 16], true, "/export", "../file").is_err());
}

#[test]
fn lock_is_exclusive_and_survives_eviction() {
    let tmp = tempfile::tempdir().unwrap();
    let d = description(&bytes(), 4096);
    let store = Store::open(tmp.path(), context(), d.clone()).unwrap();
    assert!(Store::open(tmp.path(), context(), d.clone()).is_err());
    fill(&store, &bytes());
    store.clear().unwrap();
    assert!(Store::open(tmp.path(), context(), d.clone()).is_err());
    drop(store);
    assert!(Store::open(tmp.path(), context(), d).is_ok());
}

#[test]
fn state_paths_reject_symlinks_and_linked_locks() {
    let tmp = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let external = outside.path().join("data");
    fs::write(&external, b"unchanged").unwrap();
    let d = description(&bytes(), 4096);
    let store = Store::open(tmp.path(), context(), d.clone()).unwrap();
    let dir = chunk_dir(tmp.path());
    symlink(&external, dir.join("00000000.chunk")).unwrap();
    assert!(store.missing().is_err());
    store.clear().unwrap();
    assert_eq!(fs::read(&external).unwrap(), b"unchanged");
    drop(store);
    fs::remove_file(dir.join("lock")).unwrap();
    symlink(&external, dir.join("lock")).unwrap();
    assert!(Store::open(tmp.path(), context(), d.clone()).is_err());
    fs::remove_file(dir.join("lock")).unwrap();
    fs::hard_link(&external, dir.join("lock")).unwrap();
    assert!(Store::open(tmp.path(), context(), d).is_err());
    assert_eq!(fs::read(&external).unwrap(), b"unchanged");
}

#[test]
fn empty_file_has_no_chunks_and_exact_boundary_has_no_tail() {
    let tmp = tempfile::tempdir().unwrap();
    let d = description(&[], 4096);
    assert!(d.hashes().is_empty());
    let store = Store::open(tmp.path(), context(), d).unwrap();
    assert!(store.missing().unwrap().is_empty());
    assert!(assembled(&store).is_empty());
    let d = description(&vec![42; 8192], 4096);
    assert_eq!(d.hashes().len(), 2);
    assert_eq!(d.length(1).unwrap(), 4096);
}

#[test]
fn larger_chunks_stream_and_reject_reader_failure() {
    struct Interrupted;
    impl Read for Interrupted {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("injected stream failure"))
        }
    }
    let tmp = tempfile::tempdir().unwrap();
    let data = vec![23; 1024 * 1024 + 1];
    let d = description(&data, 256 * 1024);
    assert_eq!(d.hashes().len(), 5);
    assert_eq!(d.length(4).unwrap(), 1);
    let store = Store::open(tmp.path(), context(), d).unwrap();
    for (index, chunk) in data.chunks(256 * 1024).enumerate() {
        store.receive(index, &mut Cursor::new(chunk)).unwrap();
    }
    assert!(store.receive(0, &mut Interrupted).is_err());
    assert!(store.missing().unwrap().is_empty());
    assert_eq!(assembled(&store), data);
}

#[test]
fn cache_quota_reserves_missing_bytes_and_excludes_concurrent_owners() {
    use rrsync::chunks::Limits;
    let tmp = tempfile::tempdir().unwrap();
    let data = bytes();
    let d = description(&data, 4096);
    let limits = Limits {
        max_bytes: data.len() as u64,
        max_transfers: 4,
        retention_seconds: 0,
    };
    let store = Store::open_limited(tmp.path(), context(), d.clone(), limits).unwrap();
    assert!(Store::open_limited(tmp.path(), [9; 32], d.clone(), limits).is_err());
    store.receive(0, &mut Cursor::new(&data[..4096])).unwrap();
    drop(store);
    let store = Store::open_limited(tmp.path(), context(), d.clone(), limits).unwrap();
    assert_eq!(store.missing().unwrap(), vec![1, 2]);
    drop(store);
    assert!(Store::open_limited(tmp.path(), [9; 32], d.clone(), limits).is_err());
    assert!(
        Store::open_limited(
            tmp.path(),
            [8; 32],
            d.clone(),
            Limits {
                max_bytes: 100000,
                max_transfers: 1,
                retention_seconds: 0
            }
        )
        .is_err()
    );
    let store = Store::open_limited(tmp.path(), context(), d, limits).unwrap();
    fill(&store, &data);
    assert_eq!(assembled(&store), data);
    store.clear().unwrap();
}

#[test]
fn expiry_rejects_symlinks_without_deleting_cached_or_external_data() {
    use rrsync::chunks::Limits;
    let tmp = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let external = outside.path().join("payload");
    fs::write(&external, b"outside").unwrap();
    let data = bytes();
    let d = description(&data, 4096);
    let store = Store::open(tmp.path(), context(), d.clone()).unwrap();
    fill(&store, &data);
    drop(store);
    let dir = chunk_dir(tmp.path());
    let mut activity = b"RRSYNC01".to_vec();
    activity.extend(1u64.to_be_bytes());
    fs::write(dir.join("activity"), activity).unwrap();
    symlink(&external, dir.join("00000003.chunk")).unwrap();
    let limits = Limits {
        max_bytes: 100000,
        max_transfers: 4,
        retention_seconds: 1,
    };
    assert!(Store::open_limited(tmp.path(), [9; 32], d, limits).is_err());
    assert_eq!(fs::read(&external).unwrap(), b"outside");
    assert_eq!(fs::read(dir.join("00000000.chunk")).unwrap(), &data[..4096]);
}
