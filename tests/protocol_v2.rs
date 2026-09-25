use rrsync::{
    chunks::{Description, PAGE_CHUNKS},
    protocol::{
        self, Message as V1,
        v2::{Message, validate_missing},
    },
    sync::{MAX_ENTRIES, MAX_FILE, Manifest, Options},
};
fn hash() -> [u8; 32] {
    hex::decode("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")
        .unwrap()
        .try_into()
        .unwrap()
}
fn description() -> Description {
    Description::header(5, 4096, hash()).unwrap()
}
fn messages() -> Vec<Message> {
    vec![
        Message::Common(V1::Start {
            push: true,
            options: Options::default(),
            path: "backup".into(),
            manifest: Manifest::default(),
        }),
        Message::Common(V1::Manifest(Manifest::default())),
        Message::Common(V1::Finish),
        Message::Common(V1::Ok),
        Message::Common(V1::Error {
            code: 4,
            text: "bad hash".into(),
        }),
        Message::Describe {
            index: 7,
            chunk_size: 4096,
        },
        Message::Description {
            index: 7,
            description: description(),
        },
        Message::HashesGet {
            index: 7,
            start: 128,
        },
        Message::Hashes {
            index: 7,
            start: 128,
            hashes: vec![hash()],
        },
        Message::Missing {
            index: 7,
            start: 128,
            count: 3,
            chunks: vec![128, 130],
        },
        Message::ChunkBegin {
            index: 7,
            chunk: 32767,
        },
        Message::ChunkCommit {
            index: 7,
            chunk: 32767,
            resource: [5; 32],
        },
        Message::ChunkGet {
            index: 7,
            chunk: 32767,
        },
        Message::ChunkVerified {
            index: 7,
            chunk: 32767,
        },
        Message::FileCommit(7),
        Message::FileVerified(7),
    ]
}
#[test]
fn every_message_roundtrips_and_rejects_truncation_or_trailing_bytes() {
    for m in messages() {
        let bytes = m.encode().unwrap();
        assert_eq!(Message::decode(&bytes).unwrap().encode().unwrap(), bytes);
        for n in 0..bytes.len() {
            assert!(Message::decode(&bytes[..n]).is_err(), "{m:?} {n}");
        }
        let mut extra = bytes;
        extra.push(0);
        assert!(Message::decode(&extra).is_err());
    }
}
#[test]
fn fixed_vectors_for_other_implementations() {
    for (m, expected) in [
        (
            Message::Describe {
                index: 7,
                chunk_size: 4096,
            },
            "020a0000000700001000",
        ),
        (
            Message::HashesGet {
                index: 7,
                start: 128,
            },
            "02130000000700000080",
        ),
        (
            Message::Missing {
                index: 7,
                start: 128,
                count: 3,
                chunks: vec![128, 130],
            },
            "020c000000070000008000000003000000020000008000000082",
        ),
        (
            Message::ChunkGet {
                index: 7,
                chunk: 32767,
            },
            "020f0000000700007fff",
        ),
        (Message::FileCommit(7), "021100000007"),
        (Message::Common(V1::Finish), "0207"),
    ] {
        assert_eq!(hex::encode(m.encode().unwrap()), expected);
        assert_eq!(
            hex::encode(
                Message::decode(&hex::decode(expected).unwrap())
                    .unwrap()
                    .encode()
                    .unwrap()
            ),
            expected
        );
    }
    let header = Message::Description {
        index: 7,
        description: description(),
    }
    .encode()
    .unwrap();
    assert_eq!(
        hex::encode(header),
        concat!(
            "020b00000007000000000000000500001000",
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        )
    );
    assert_eq!(
        hex::encode(
            Message::Hashes {
                index: 7,
                start: 0,
                hashes: vec![hash()]
            }
            .encode()
            .unwrap()
        ),
        concat!(
            "0214000000070000000000000001",
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        )
    );
}
#[test]
fn versions_and_old_unpaged_commands_are_rejected() {
    for m in messages() {
        assert!(V1::decode(&m.encode().unwrap()).is_err());
    }
    for tag in [0, 3, 4, 5, 6, 21, 127, 255] {
        assert!(Message::decode(&[2, tag]).is_err());
    }
    for version in [0, 1, 3, 255] {
        assert!(Message::decode(&[version, 8]).is_err());
    }
    assert_ne!(protocol::REQUEST_PATH, protocol::v2::REQUEST_PATH);
    let mut old = Message::Description {
        index: 0,
        description: description(),
    }
    .encode()
    .unwrap();
    old.extend(1u32.to_be_bytes());
    old.extend(hash());
    assert!(Message::decode(&old).is_err());
    assert!(
        Message::decode(&hex::decode("020c0000000700000003000000000000000200000006").unwrap())
            .is_err()
    );
}
#[test]
fn descriptions_and_pages_reject_bad_geometry_before_allocation() {
    let bytes = Message::Description {
        index: 7,
        description: description(),
    }
    .encode()
    .unwrap();
    for (offset, field) in [
        (6, (MAX_FILE + 1).to_be_bytes().to_vec()),
        (14, 0u32.to_be_bytes().to_vec()),
        (6, 0u64.to_be_bytes().to_vec()),
    ] {
        let mut bad = bytes.clone();
        bad[offset..offset + field.len()].copy_from_slice(&field);
        assert!(Message::decode(&bad).is_err());
    }
    for count in [0, 129, u32::MAX] {
        let mut bytes = vec![2, 20];
        bytes.extend(0u32.to_be_bytes());
        bytes.extend(0u32.to_be_bytes());
        bytes.extend(count.to_be_bytes());
        assert!(Message::decode(&bytes).is_err());
    }
    let d = Description::header(MAX_FILE, 4096, [1; 32]).unwrap();
    assert_eq!(d.chunks(), 32768);
    assert!(d.set_page(0, &[[0; 32]; 127]).is_err());
    assert!(d.set_page(1, &[[0; 32]; 128]).is_err());
    assert!(d.page(32768).is_err());
}
#[test]
fn missing_lists_are_page_bounded_ordered_and_range_checked() {
    for chunks in [vec![128, 128], vec![129, 128], vec![127], vec![131]] {
        assert!(
            Message::Missing {
                index: 0,
                start: 128,
                count: 3,
                chunks
            }
            .encode()
            .is_err()
        );
    }
    assert!(validate_missing(&[128, 130], 128, 3).is_ok());
    assert!(validate_missing(&[], 128, 3).is_ok());
    assert!(validate_missing(&[], 1, 3).is_err());
    assert!(validate_missing(&[], 0, 0).is_err());
    assert!(validate_missing(&[], 0, PAGE_CHUNKS + 1).is_err());
    let page = Message::Hashes {
        index: 0,
        start: 32768 - 128,
        hashes: vec![[0; 32]; 128],
    }
    .encode()
    .unwrap();
    assert_eq!(page.len(), 4110);
    assert!(Message::decode(&page).is_ok());
}
#[test]
fn file_indices_and_common_fields_are_checked() {
    assert!(Message::FileCommit(MAX_ENTRIES as u32).encode().is_err());
    assert!(
        Message::Describe {
            index: 0,
            chunk_size: 1
        }
        .encode()
        .is_err()
    );
    assert!(Message::decode(&[2, 1, 2, 0]).is_err());
    assert!(Message::decode(&[2, 1, 1, 8]).is_err());
    assert!(Message::decode(&vec![0; protocol::MAX_CONTROL + 1]).is_err());
}
#[test]
fn arbitrary_v2_payloads_never_panic() {
    let mut state = 0xa37c_058du32;
    for len in 0..512 {
        let mut bytes = vec![2, (len % 22) as u8];
        for _ in 0..len {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            bytes.push(state as u8);
        }
        let _ = Message::decode(&bytes);
    }
}
