use rrsync::{
    chunks::{Description, Limits, Store},
    fs::Root,
};
use std::{
    fs,
    io::{Cursor, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

fn fixture() -> tempfile::TempDir {
    if let Some(path) = std::env::var_os("RRSYNC_EXPIRY_TEST_FILESYSTEM") {
        tempfile::Builder::new()
            .prefix("rrsync-expiry-")
            .tempdir_in(path)
            .unwrap()
    } else {
        tempfile::tempdir().unwrap()
    }
}
fn description() -> Description {
    let mut f = tempfile::tempfile().unwrap();
    f.write_all(&[42; 4096]).unwrap();
    Description::scan(&mut f, 4096).unwrap()
}
fn limits(retention_seconds: u64) -> Limits {
    Limits {
        max_bytes: 4096,
        max_transfers: 1,
        retention_seconds,
    }
}
fn directory(path: &Path) -> PathBuf {
    fs::read_dir(path)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.is_dir())
        .unwrap()
}
fn timestamp(path: &Path, seconds: u64) {
    let mut bytes = b"RRSYNC01".to_vec();
    bytes.extend(seconds.to_be_bytes());
    fs::write(path.join("activity"), bytes).unwrap();
}
fn seed(path: &Path) -> PathBuf {
    let store = Store::open_limited(path, [1; 32], description(), limits(3600)).unwrap();
    store.receive(0, &mut Cursor::new([42; 4096])).unwrap();
    drop(store);
    directory(path)
}

#[test]
fn expiry_reclaims_bytes_staging_and_directory_quota_without_using_mtime() {
    let tmp = fixture();
    let old = seed(tmp.path());
    fs::write(old.join(".rrsync-abandoned"), b"partial").unwrap();
    timestamp(&old, 1);
    // A recent filesystem timestamp must not protect an expired content record.
    let root = Root::open(&old).unwrap();
    root.set_mtime("activity", 1_800_000_001, 123456789)
        .unwrap();
    let store = Store::open_limited(tmp.path(), [2; 32], description(), limits(3600)).unwrap();
    assert!(!old.exists());
    assert_eq!(store.missing().unwrap(), vec![0]);
    store.receive(0, &mut Cursor::new([42; 4096])).unwrap();
    store.clear().unwrap();
    drop(store);
    let empty = directory(tmp.path());
    timestamp(&empty, 1);
    drop(Store::open_limited(tmp.path(), [3; 32], description(), limits(3600)).unwrap());
    assert!(!empty.exists());
}

#[test]
fn disabled_expiry_and_current_transfer_keep_verified_chunks() {
    let tmp = fixture();
    let old = seed(tmp.path());
    timestamp(&old, 1);
    assert!(Store::open_limited(tmp.path(), [2; 32], description(), limits(0)).is_err());
    assert!(old.join("00000000.chunk").exists());
    let current = Store::open_limited(tmp.path(), [1; 32], description(), limits(1)).unwrap();
    assert!(current.missing().unwrap().is_empty());
    assert!(Store::open_limited(tmp.path(), [2; 32], description(), limits(1)).is_err());
    drop(current);
    // Closing refreshed activity; an immediate competing transfer cannot evict it.
    assert!(Store::open_limited(tmp.path(), [2; 32], description(), limits(3600)).is_err());
}

#[test]
fn missing_corrupt_and_future_activity_get_conservative_retention() {
    for state in ["missing", "corrupt", "future"] {
        let tmp = fixture();
        let old = seed(tmp.path());
        match state {
            "missing" => fs::remove_file(old.join("activity")).unwrap(),
            "corrupt" => fs::write(old.join("activity"), b"bad").unwrap(),
            _ => timestamp(&old, u64::MAX),
        }
        Root::open(&old)
            .unwrap()
            .set_mtime("00000000.chunk", 1_000_000_001, 0)
            .unwrap();
        assert!(Store::open_limited(tmp.path(), [2; 32], description(), limits(1)).is_err());
        assert_eq!(fs::read(old.join("00000000.chunk")).unwrap(), [42; 4096]);
        let record = fs::read(old.join("activity")).unwrap();
        assert_eq!(record.len(), 16);
        let time = u64::from_be_bytes(record[8..].try_into().unwrap());
        if state == "future" {
            assert_eq!(time, u64::MAX);
        } else {
            assert!(
                time <= SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
            );
        }
    }
}

#[test]
fn shared_library_owners_prevent_cleanup_and_exclusive_owners_prevent_open() {
    let tmp = fixture();
    let old = seed(tmp.path());
    let library = Store::open(tmp.path(), [1; 32], description()).unwrap();
    timestamp(&old, 1);
    assert!(Store::open_limited(tmp.path(), [2; 32], description(), limits(1)).is_err());
    assert!(library.missing().unwrap().is_empty());
    drop(library);
    timestamp(&old, 1);
    let exclusive = Store::open_limited(tmp.path(), [2; 32], description(), limits(1)).unwrap();
    assert!(!old.exists());
    assert!(Store::open(tmp.path(), [1; 32], description()).is_err());
    assert!(!old.exists());
    drop(exclusive);
}

#[test]
fn unexpected_objects_stop_cleanup_before_deleting_transfer_contents() {
    let tmp = fixture();
    let old = seed(tmp.path());
    timestamp(&old, 1);
    fs::write(old.join("unrecognized"), b"keep").unwrap();
    assert!(Store::open_limited(tmp.path(), [2; 32], description(), limits(1)).is_err());
    assert!(old.join("00000000.chunk").exists());
    assert_eq!(fs::read(old.join("unrecognized")).unwrap(), b"keep");
}

#[test]
fn interrupted_eviction_is_recoverable() {
    let tmp = fixture();
    let old = seed(tmp.path());
    timestamp(&old, 1);
    fs::remove_file(old.join("00000000.chunk")).unwrap();
    drop(Store::open_limited(tmp.path(), [2; 32], description(), limits(1)).unwrap());
    assert!(!old.exists());
}
