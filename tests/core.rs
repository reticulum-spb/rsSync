use rrsync::{
    fs::{Root, snapshot, validate},
    protocol::Message,
    sync::{self, Action, Kind, Options},
};
use std::{
    fs,
    io::{Cursor, Read},
    os::unix::fs::symlink,
};
fn options() -> Options {
    Options {
        delete: true,
        checksum: true,
        dry_run: false,
    }
}
#[test]
fn paths_reject_traversal() {
    for s in [
        "",
        "../foo",
        "../../etc/passwd",
        "/foo",
        "foo/../../../bar",
        "foo//bar",
        "foo/./bar",
        "foo/",
        "a\0b",
    ] {
        assert!(validate(s).is_err(), "{s}");
    }
    assert!(validate("папка/文件.txt").is_ok());
}
#[test]
fn manifest_plan_and_install() {
    let s = tempfile::tempdir().unwrap();
    let d = tempfile::tempdir().unwrap();
    fs::create_dir(s.path().join("empty")).unwrap();
    fs::write(s.path().join("файл"), b"hello").unwrap();
    fs::write(d.path().join("old"), b"old").unwrap();
    let sr = Root::open(s.path()).unwrap();
    let dr = Root::open(d.path()).unwrap();
    let a = sync::scan(&sr, true).unwrap();
    let b = sync::scan(&dr, true).unwrap();
    let p = sync::plan(&a.manifest, &b.manifest, options()).unwrap();
    assert_eq!(p.files.len(), 1);
    assert_eq!(p.removals.len(), 1);
    sync::prepare(&dr, &a.manifest, &p).unwrap();
    for &i in &p.files {
        let e = &a.manifest.entries[i];
        let mut f = snapshot(&sr, &e.path, &a.stamps[&e.path]).unwrap();
        sync::install(&dr, e, &b.manifest, &p, &mut f).unwrap();
    }
    sync::finish(&dr, &a.manifest, &p).unwrap();
    let after = sync::scan(&dr, true).unwrap();
    let p2 = sync::plan(&a.manifest, &after.manifest, options()).unwrap();
    assert!(p2.files.is_empty());
    assert!(p2.removals.is_empty());
    assert!(p2.operations.iter().all(|o| o.action == Action::Skip));
}
#[test]
fn skip_symlink_and_protect_destination() {
    let s = tempfile::tempdir().unwrap();
    let d = tempfile::tempdir().unwrap();
    symlink("/etc", s.path().join("protected")).unwrap();
    fs::create_dir(d.path().join("protected")).unwrap();
    fs::write(d.path().join("protected/data"), b"preserve").unwrap();
    let a = sync::scan(&Root::open(s.path()).unwrap(), false).unwrap();
    let b = sync::scan(&Root::open(d.path()).unwrap(), false).unwrap();
    assert_eq!(a.manifest.entries[0].kind, Kind::Protected);
    let p = sync::plan(
        &a.manifest,
        &b.manifest,
        Options {
            delete: true,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(p.removals.is_empty());
    assert!(p.files.is_empty());
}
#[test]
fn symlink_cannot_escape_root() {
    let t = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    fs::write(outside.path().join("secret"), b"secret").unwrap();
    symlink(outside.path(), t.path().join("escape")).unwrap();
    let r = Root::open(t.path()).unwrap();
    assert!(r.open_file("escape/secret").is_err());
    assert!(r.mkdir("escape/new").is_err());
    assert!(
        r.stage("escape/secret", &mut Cursor::new(b"x"), 1, None, 0, 0)
            .is_err()
    );
    assert_eq!(fs::read(outside.path().join("secret")).unwrap(), b"secret");
}
#[test]
fn atomic_failure_and_empty_file() {
    let t = tempfile::tempdir().unwrap();
    fs::write(t.path().join("file"), b"old").unwrap();
    let r = Root::open(t.path()).unwrap();
    assert!(
        r.stage("file", &mut Cursor::new(b"short"), 10, None, 0, 0)
            .is_err()
    );
    assert_eq!(fs::read(t.path().join("file")).unwrap(), b"old");
    assert!(
        r.stage("file", &mut Cursor::new(b"new"), 3, Some([0; 32]), 0, 0)
            .is_err()
    );
    assert_eq!(fs::read(t.path().join("file")).unwrap(), b"old");
    {
        let _staged = r
            .stage("file", &mut Cursor::new(b"new"), 3, None, 0, 0)
            .unwrap();
        assert_eq!(fs::read(t.path().join("file")).unwrap(), b"old");
    }
    assert_eq!(fs::read_dir(t.path()).unwrap().count(), 1);
    r.stage("file", &mut Cursor::new(b""), 0, None, 1234, 42)
        .unwrap()
        .commit()
        .unwrap();
    assert!(fs::read(t.path().join("file")).unwrap().is_empty());
}
#[test]
fn type_conflicts_require_delete() {
    let s = tempfile::tempdir().unwrap();
    let d = tempfile::tempdir().unwrap();
    fs::write(s.path().join("object"), b"file").unwrap();
    fs::create_dir(d.path().join("object")).unwrap();
    fs::write(d.path().join("object/old"), b"old").unwrap();
    let sr = Root::open(s.path()).unwrap();
    let dr = Root::open(d.path()).unwrap();
    let a = sync::scan(&sr, true).unwrap();
    let b = sync::scan(&dr, true).unwrap();
    assert!(
        sync::plan(
            &a.manifest,
            &b.manifest,
            Options {
                checksum: true,
                ..Default::default()
            }
        )
        .is_err()
    );
    let p = sync::plan(&a.manifest, &b.manifest, options()).unwrap();
    sync::prepare(&dr, &a.manifest, &p).unwrap();
    let e = &a.manifest.entries[0];
    let mut f = sr.open_file("object").unwrap();
    sync::install(&dr, e, &b.manifest, &p, &mut f).unwrap();
    assert_eq!(fs::read(d.path().join("object")).unwrap(), b"file");
}
#[test]
fn checksum_detects_same_metadata_changes() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    fs::write(a.path().join("f"), b"abc").unwrap();
    fs::write(b.path().join("f"), b"xyz").unwrap();
    let ar = Root::open(a.path()).unwrap();
    let br = Root::open(b.path()).unwrap();
    ar.set_mtime("f", 123, 0).unwrap();
    br.set_mtime("f", 123, 0).unwrap();
    let aa = sync::scan(&ar, true).unwrap();
    let bb = sync::scan(&br, true).unwrap();
    assert!(
        sync::plan(&aa.manifest, &bb.manifest, Options::default())
            .unwrap()
            .files
            .is_empty()
    );
    assert_eq!(
        sync::plan(&aa.manifest, &bb.manifest, options())
            .unwrap()
            .files
            .len(),
        1
    );
}
#[test]
fn source_change_rejected() {
    let t = tempfile::tempdir().unwrap();
    fs::write(t.path().join("f"), b"old").unwrap();
    let r = Root::open(t.path()).unwrap();
    let a = sync::scan(&r, false).unwrap();
    fs::write(t.path().join("f"), b"changed").unwrap();
    assert!(snapshot(&r, "f", &a.stamps["f"]).is_err());
}
#[test]
fn missing_destination_dry_plan_does_not_create() {
    let t = tempfile::tempdir().unwrap();
    let path = t.path().join("missing/nested");
    let r = Root::destination(&path).unwrap();
    assert!(sync::scan(&r, false).unwrap().manifest.entries.is_empty());
    assert!(!path.exists());
}
#[test]
fn wire_roundtrip_and_malformed() {
    let t = tempfile::tempdir().unwrap();
    fs::write(t.path().join("f"), b"x").unwrap();
    let m = sync::scan(&Root::open(t.path()).unwrap(), true)
        .unwrap()
        .manifest;
    let data = Message::Manifest(m.clone()).encode().unwrap();
    let Message::Manifest(decoded) = Message::decode(&data).unwrap() else {
        panic!()
    };
    assert_eq!(m, decoded);
    for i in 0..data.len() {
        assert!(Message::decode(&data[..i]).is_err());
    }
    let mut extra = data.clone();
    extra.push(0);
    assert!(Message::decode(&extra).is_err());
    for bytes in [
        vec![2, 8],
        vec![1, 99],
        vec![1, 2, 255, 255, 255, 255],
        vec![1, 1, 3],
    ] {
        assert!(Message::decode(&bytes).is_err());
    }
    assert_eq!(Message::Finish.encode().unwrap(), [1, 7]);
}
#[test]
fn streaming_copy_exceeds_native_segment() {
    let t = tempfile::tempdir().unwrap();
    let root = Root::open(t.path()).unwrap();
    let size = 3 * 1024 * 1024;
    let mut reader = std::io::repeat(42).take(size);
    root.stage("large", &mut reader, size, None, 1, 0)
        .unwrap()
        .commit()
        .unwrap();
    assert_eq!(fs::metadata(t.path().join("large")).unwrap().len(), size);
}
#[test]
fn deletion_preflight_rejects_changed_file() {
    let t = tempfile::tempdir().unwrap();
    fs::write(t.path().join("extra"), b"old").unwrap();
    let root = Root::open(t.path()).unwrap();
    let b = sync::scan(&root, false).unwrap();
    let source = Default::default();
    let p = sync::plan(
        &source,
        &b.manifest,
        Options {
            delete: true,
            ..Default::default()
        },
    )
    .unwrap();
    fs::write(t.path().join("extra"), b"changed").unwrap();
    assert!(sync::finish(&root, &source, &p).is_err());
    assert!(t.path().join("extra").exists());
}

#[test]
fn invalid_manifest_structure_and_limits() {
    use rrsync::sync::{Entry, Manifest};
    let entry = Entry {
        path: "a".into(),
        kind: Kind::File,
        size: 1,
        mtime: 0,
        ns: 0,
        hash: None,
    };
    assert!(
        Manifest {
            entries: vec![entry.clone(), entry.clone()]
        }
        .validate(false)
        .is_err()
    );
    let mut child = entry.clone();
    child.path = "missing/child".into();
    assert!(
        Manifest {
            entries: vec![child]
        }
        .validate(false)
        .is_err()
    );
    let mut reserved = entry.clone();
    reserved.path = ".rrsync-internal".into();
    assert!(
        Manifest {
            entries: vec![reserved]
        }
        .validate(false)
        .is_err()
    );
    let mut oversized = entry.clone();
    oversized.size = rrsync::sync::MAX_FILE + 1;
    assert!(
        Manifest {
            entries: vec![oversized]
        }
        .validate(false)
        .is_err()
    );
    assert!(
        Manifest {
            entries: vec![entry]
        }
        .validate(true)
        .is_err()
    );
}
#[test]
fn arbitrary_wire_bytes_never_panic() {
    let mut seed = 17u64;
    for len in 0..512 {
        let mut data = vec![0; len];
        for byte in &mut data {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            *byte = (seed >> 32) as u8;
        }
        if len > 1 {
            data[0] = 1;
            data[1] = (len % 9 + 1) as u8;
        }
        let _ = Message::decode(&data);
    }
}
