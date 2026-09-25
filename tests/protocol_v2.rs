use rrsync::{
    chunks::{Description, MAX_CHUNKS},
    protocol::{
        self, Message as V1,
        v2::{Message, validate_missing},
    },
    sync::{MAX_ENTRIES, MAX_FILE, Manifest, Options},
};
fn description() -> Description {
    let hash: [u8; 32] =
        hex::decode("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")
            .unwrap()
            .try_into()
            .unwrap();
    Description::new(5, 4096, hash, vec![hash]).unwrap()
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
        Message::Missing {
            index: 7,
            chunks: vec![0, 2, 6],
        },
        Message::ChunkBegin { index: 7, chunk: 2 },
        Message::ChunkCommit {
            index: 7,
            chunk: 2,
            resource: [5; 32],
        },
        Message::ChunkGet { index: 7, chunk: 2 },
        Message::ChunkVerified { index: 7, chunk: 2 },
        Message::FileCommit(7),
        Message::FileVerified(7),
    ]
}
#[test]
fn every_message_roundtrips_and_rejects_truncation_or_trailing_bytes() {
    for message in messages() {
        let bytes = message.encode().unwrap();
        assert_eq!(Message::decode(&bytes).unwrap().encode().unwrap(), bytes);
        for n in 0..bytes.len() {
            assert!(Message::decode(&bytes[..n]).is_err(), "{message:?}, {n}");
        }
        let mut extra = bytes;
        extra.push(0);
        assert!(Message::decode(&extra).is_err());
    }
}
#[test]
fn fixed_vectors_for_other_implementations() {
    for (message, expected) in [
        (
            Message::Describe {
                index: 7,
                chunk_size: 4096,
            },
            "020a0000000700001000",
        ),
        (
            Message::Missing {
                index: 7,
                chunks: vec![0, 2, 6],
            },
            "020c0000000700000003000000000000000200000006",
        ),
        (
            Message::ChunkGet { index: 7, chunk: 2 },
            "020f0000000700000002",
        ),
        (Message::FileCommit(7), "021100000007"),
        (Message::Common(V1::Finish), "0207"),
    ] {
        assert_eq!(hex::encode(message.encode().unwrap()), expected);
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
    let expected = concat!(
        "020b00000007000000000000000500001000",
        "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
        "00000001",
        "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    );
    assert_eq!(
        hex::encode(
            Message::Description {
                index: 7,
                description: description()
            }
            .encode()
            .unwrap()
        ),
        expected
    );
}
#[test]
fn versions_and_legacy_file_commands_cannot_mix() {
    assert_ne!(protocol::REQUEST_PATH, protocol::v2::REQUEST_PATH);
    for message in messages() {
        assert!(V1::decode(&message.encode().unwrap()).is_err());
    }
    assert!(Message::decode(&V1::Finish.encode().unwrap()).is_err());
    for old in [
        V1::Begin(0),
        V1::Commit {
            index: 0,
            resource: [0; 32],
        },
        V1::Get(0),
        V1::Verified(0),
    ] {
        let mut bytes = old.encode().unwrap();
        bytes[0] = 2;
        assert!(Message::decode(&bytes).is_err());
        assert!(Message::Common(old).encode().is_err());
    }
    for version in [0, 1, 3, 255] {
        assert!(Message::decode(&[version, 8]).is_err());
    }
    for tag in [0, 19, 127, 255] {
        assert!(Message::decode(&[2, tag]).is_err());
    }
}
#[test]
fn descriptions_reject_bad_geometry_before_hash_allocation() {
    let bytes = Message::Description {
        index: 7,
        description: description(),
    }
    .encode()
    .unwrap();
    // Header(2), manifest index(4), size(8), chunk size(4), full hash(32), count(4).
    for (offset, field) in [
        (6, (MAX_FILE + 1).to_be_bytes().to_vec()),
        (14, 0u32.to_be_bytes().to_vec()),
        (50, u32::MAX.to_be_bytes().to_vec()),
        (50, 0u32.to_be_bytes().to_vec()),
    ] {
        let mut bad = bytes.clone();
        bad[offset..offset + field.len()].copy_from_slice(&field);
        assert!(Message::decode(&bad).is_err());
    }
    let mut bad = bytes.clone();
    bad[6..14].copy_from_slice(&0u64.to_be_bytes());
    bad[50..54].copy_from_slice(&0u32.to_be_bytes());
    bad.truncate(54);
    assert!(Message::decode(&bad).is_err()); // invalid empty-file hash
    let large =
        Description::new(MAX_FILE + 1, 16 * 1024 * 1024, [0; 32], vec![[0; 32]; 8]).unwrap();
    assert!(
        Message::Description {
            index: 0,
            description: large
        }
        .encode()
        .is_err()
    );
}
#[test]
fn missing_lists_are_bounded_ordered_and_checked_against_description() {
    for chunks in [vec![0, 0], vec![1, 0], vec![MAX_CHUNKS as u32]] {
        assert!(
            Message::Missing {
                index: 0,
                chunks: chunks.clone()
            }
            .encode()
            .is_err()
        );
        let mut bytes = vec![2, 12];
        bytes.extend(0u32.to_be_bytes());
        bytes.extend((chunks.len() as u32).to_be_bytes());
        for chunk in chunks {
            bytes.extend(chunk.to_be_bytes());
        }
        assert!(Message::decode(&bytes).is_err());
    }
    let mut bytes = vec![2, 12];
    bytes.extend(0u32.to_be_bytes());
    bytes.extend(u32::MAX.to_be_bytes());
    assert!(Message::decode(&bytes).is_err());
    assert!(validate_missing(&[1], 1).is_err());
    assert!(validate_missing(&[], 0).is_ok());
    assert!(validate_missing(&[0, 2], 3).is_ok());
    let all: Vec<u32> = (0..MAX_CHUNKS as u32).collect();
    let bytes = Message::Missing {
        index: 0,
        chunks: all,
    }
    .encode()
    .unwrap();
    assert!(Message::decode(&bytes).is_ok());
}
#[test]
fn file_and_chunk_indices_and_common_fields_are_checked() {
    assert!(Message::FileCommit(MAX_ENTRIES as u32).encode().is_err());
    assert!(
        Message::ChunkGet {
            index: 0,
            chunk: MAX_CHUNKS as u32
        }
        .encode()
        .is_err()
    );
    assert!(
        Message::Describe {
            index: 0,
            chunk_size: 1
        }
        .encode()
        .is_err()
    );
    let mut bytes = Message::ChunkGet { index: 0, chunk: 0 }.encode().unwrap();
    bytes[2..6].copy_from_slice(&(MAX_ENTRIES as u32).to_be_bytes());
    assert!(Message::decode(&bytes).is_err());
    bytes[2..6].copy_from_slice(&0u32.to_be_bytes());
    bytes[6..10].copy_from_slice(&(MAX_CHUNKS as u32).to_be_bytes());
    assert!(Message::decode(&bytes).is_err());
    assert!(Message::decode(&[2, 1, 2, 0]).is_err()); // invalid boolean
    assert!(Message::decode(&[2, 1, 1, 8]).is_err()); // unknown flags
    assert!(Message::decode(&vec![0; protocol::MAX_CONTROL + 1]).is_err());
}
#[test]
fn arbitrary_v2_payloads_never_panic() {
    let mut state = 0xa37c_058du32;
    for len in 0..512 {
        let mut bytes = vec![2, (len % 20) as u8];
        for _ in 0..len {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            bytes.push(state as u8);
        }
        let _ = Message::decode(&bytes);
    }
}
