//! Chunk-transfer codec. Not yet enabled by the Reticulum adapter or sync engine.
//! Version and request path are distinct from v1; legacy file commands are rejected.
use super::{MAX_CONTROL, Reader, Writer};
use crate::{
    Error, Result,
    chunks::{Description, MAX_CHUNKS},
    sync::{MAX_ENTRIES, MAX_FILE},
};

pub const VERSION: u8 = 2;
pub const REQUEST_PATH: &str = "/rrsync/v2";

#[derive(Debug)]
pub enum Message {
    /// Only START, MANIFEST, FINISH, OK and ERROR retain their v1 field layouts.
    Common(super::Message),
    Describe {
        index: u32,
        chunk_size: u32,
    },
    Description {
        index: u32,
        description: Description,
    },
    Missing {
        index: u32,
        chunks: Vec<u32>,
    },
    ChunkBegin {
        index: u32,
        chunk: u32,
    },
    ChunkCommit {
        index: u32,
        chunk: u32,
        resource: [u8; 32],
    },
    ChunkGet {
        index: u32,
        chunk: u32,
    },
    ChunkVerified {
        index: u32,
        chunk: u32,
    },
    FileCommit(u32),
    FileVerified(u32),
}
fn invalid(text: &str) -> Error {
    Error::Protocol(text.into())
}
fn file_index(index: u32) -> Result<()> {
    if index as usize >= MAX_ENTRIES {
        return Err(invalid("file index outside manifest bound"));
    }
    Ok(())
}
fn chunk_index(index: u32) -> Result<()> {
    if index as usize >= MAX_CHUNKS {
        return Err(invalid("chunk index outside bound"));
    }
    Ok(())
}
fn common_tag(tag: u8) -> bool {
    matches!(tag, 1 | 2 | 7 | 8 | 9)
}

/// Check a MISSING reply against its agreed description, before sending data.
/// Used by both the encoder and the future session coordinator.
pub fn validate_missing(chunks: &[u32], count: usize) -> Result<()> {
    if count > MAX_CHUNKS || chunks.len() > count {
        return Err(invalid("missing chunk count outside bound"));
    }
    let mut previous = None;
    for &chunk in chunks {
        if chunk as usize >= count || previous.is_some_and(|p| p >= chunk) {
            return Err(invalid(
                "missing chunks must be strictly increasing and in range",
            ));
        }
        previous = Some(chunk);
    }
    Ok(())
}
impl Message {
    pub fn encode(&self) -> Result<Vec<u8>> {
        if let Self::Common(message) = self {
            let mut bytes = message.encode()?;
            if !common_tag(bytes[1]) {
                return Err(invalid("legacy whole-file command in v2"));
            }
            bytes[0] = VERSION;
            return Ok(bytes);
        }
        let mut w = Writer(vec![VERSION]);
        match self {
            Self::Describe { index, chunk_size } => {
                file_index(*index)?;
                Description::count(0, *chunk_size)?;
                w.u8(10);
                w.u32(*index);
                w.u32(*chunk_size);
            }
            Self::Description { index, description } => {
                file_index(*index)?;
                if description.size() > MAX_FILE {
                    return Err(invalid("file exceeds manifest limit"));
                }
                w.u8(11);
                w.u32(*index);
                w.0.extend(description.size().to_be_bytes());
                w.u32(description.chunk_size());
                w.0.extend(description.hash());
                w.u32(description.hashes().len() as u32);
                for hash in description.hashes() {
                    w.0.extend(hash);
                }
            }
            Self::Missing { index, chunks } => {
                file_index(*index)?;
                validate_missing(chunks, MAX_CHUNKS)?;
                w.u8(12);
                w.u32(*index);
                w.u32(chunks.len() as u32);
                for chunk in chunks {
                    w.u32(*chunk);
                }
            }
            Self::ChunkBegin { index, chunk }
            | Self::ChunkCommit { index, chunk, .. }
            | Self::ChunkGet { index, chunk }
            | Self::ChunkVerified { index, chunk } => {
                file_index(*index)?;
                chunk_index(*chunk)?;
                w.u8(match self {
                    Self::ChunkBegin { .. } => 13,
                    Self::ChunkCommit { .. } => 14,
                    Self::ChunkGet { .. } => 15,
                    Self::ChunkVerified { .. } => 16,
                    _ => unreachable!(),
                });
                w.u32(*index);
                w.u32(*chunk);
                if let Self::ChunkCommit { resource, .. } = self {
                    w.0.extend(resource);
                }
            }
            Self::FileCommit(index) | Self::FileVerified(index) => {
                file_index(*index)?;
                w.u8(if matches!(self, Self::FileCommit(_)) {
                    17
                } else {
                    18
                });
                w.u32(*index);
            }
            Self::Common(_) => unreachable!(),
        }
        if w.0.len() > MAX_CONTROL {
            return Err(invalid("control message limit"));
        }
        Ok(w.0)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_CONTROL {
            return Err(invalid("control message limit"));
        }
        let mut r = Reader(bytes);
        if r.u8()? != VERSION {
            return Err(invalid("version mismatch"));
        }
        let tag = r.u8()?;
        if common_tag(tag) {
            // Share v1's bounded manifest/flags/string validation, not its version.
            let mut common = bytes.to_vec();
            common[0] = super::VERSION;
            return Ok(Self::Common(super::Message::decode(&common)?));
        }
        if !(10..=18).contains(&tag) {
            return Err(invalid("unknown v2 message tag"));
        }
        let index = r.u32()?;
        file_index(index)?;
        let message = match tag {
            10 => {
                let chunk_size = r.u32()?;
                Description::count(0, chunk_size)?;
                Self::Describe { index, chunk_size }
            }
            11 => {
                let size = u64::from_be_bytes(r.take(8)?.try_into().unwrap());
                if size > MAX_FILE {
                    return Err(invalid("file exceeds manifest limit"));
                }
                let chunk_size = r.u32()?;
                let expected = Description::count(size, chunk_size)?;
                let hash = r.take(32)?.try_into().unwrap();
                let count = r.u32()? as usize;
                if count != expected {
                    return Err(invalid("chunk hash count mismatch"));
                }
                // Validate all bytes before allocating, even within the count bound.
                let raw = r.take(count * 32)?;
                let hashes = raw
                    .chunks_exact(32)
                    .map(|h| h.try_into().unwrap())
                    .collect();
                Self::Description {
                    index,
                    description: Description::new(size, chunk_size, hash, hashes)?,
                }
            }
            12 => {
                let count = r.u32()? as usize;
                if count > MAX_CHUNKS {
                    return Err(invalid("missing chunk count outside bound"));
                }
                let raw = r.take(count * 4)?;
                let chunks: Vec<_> = raw
                    .chunks_exact(4)
                    .map(|n| u32::from_be_bytes(n.try_into().unwrap()))
                    .collect();
                validate_missing(&chunks, MAX_CHUNKS)?;
                Self::Missing { index, chunks }
            }
            13..=16 => {
                let chunk = r.u32()?;
                chunk_index(chunk)?;
                match tag {
                    13 => Self::ChunkBegin { index, chunk },
                    14 => Self::ChunkCommit {
                        index,
                        chunk,
                        resource: r.take(32)?.try_into().unwrap(),
                    },
                    15 => Self::ChunkGet { index, chunk },
                    16 => Self::ChunkVerified { index, chunk },
                    _ => unreachable!(),
                }
            }
            17 => Self::FileCommit(index),
            18 => Self::FileVerified(index),
            _ => unreachable!(),
        };
        if !r.0.is_empty() {
            return Err(invalid("trailing bytes"));
        }
        Ok(message)
    }
}
