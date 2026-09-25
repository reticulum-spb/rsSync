//! Persistent verified chunk storage for v2; protocol v1 does not use it.
//! Callers must negotiate a fresh description after authentication on every run.
use crate::{
    Error, Result,
    fs::{Root, Stamp, hash_file},
};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{Read, Seek, Write},
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

// Safety bounds, not a default or performance recommendation.
pub const MIN_CHUNK_SIZE: u32 = 4096;
pub const MAX_CHUNK_SIZE: u32 = 16 * 1024 * 1024;
// Leave room for the lock and abandoned staging files below Root's entry limit.
pub const MAX_CHUNKS: usize = 8192;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Description {
    size: u64,
    chunk_size: u32,
    hash: [u8; 32],
    hashes: Vec<[u8; 32]>,
}
impl Description {
    /// Validate geometry before allocating a hash table from peer input.
    pub fn count(size: u64, chunk_size: u32) -> Result<usize> {
        if !(MIN_CHUNK_SIZE..=MAX_CHUNK_SIZE).contains(&chunk_size) {
            return Err(Error::Protocol("chunk size outside safety bounds".into()));
        }
        let count = size.div_ceil(u64::from(chunk_size));
        if count > MAX_CHUNKS as u64 {
            return Err(Error::Protocol("chunk count limit exceeded".into()));
        }
        Ok(count as usize)
    }
    pub fn new(size: u64, chunk_size: u32, hash: [u8; 32], hashes: Vec<[u8; 32]>) -> Result<Self> {
        if hashes.len() != Self::count(size, chunk_size)? {
            return Err(Error::Protocol("chunk hash count mismatch".into()));
        }
        if size == 0 && hash != <[u8; 32]>::from(Sha256::digest([])) {
            return Err(Error::HashMismatch("empty chunk description".into()));
        }
        Ok(Self {
            size,
            chunk_size,
            hash,
            hashes,
        })
    }
    /// Read an existing file/snapshot with a fixed 64 KiB buffer.
    pub fn scan(file: &mut File, chunk_size: u32) -> Result<Self> {
        let before = Stamp::of(&file.metadata()?);
        let count = Self::count(before.size, chunk_size)?;
        file.rewind()?;
        let mut whole = Sha256::new();
        let mut hashes = Vec::with_capacity(count);
        let mut buf = [0u8; 65_536];
        let mut remaining = before.size;
        while remaining > 0 {
            let length = remaining.min(u64::from(chunk_size));
            let mut left = length;
            let mut chunk = Sha256::new();
            while left > 0 {
                let n = left.min(buf.len() as u64) as usize;
                file.read_exact(&mut buf[..n])?;
                chunk.update(&buf[..n]);
                whole.update(&buf[..n]);
                left -= n as u64;
            }
            hashes.push(chunk.finalize().into());
            remaining -= length;
        }
        if file.read(&mut buf[..1])? != 0 || Stamp::of(&file.metadata()?) != before {
            return Err(Error::Changed("chunk source".into()));
        }
        file.rewind()?;
        Self::new(before.size, chunk_size, whole.finalize().into(), hashes)
    }
    pub fn size(&self) -> u64 {
        self.size
    }
    pub fn chunk_size(&self) -> u32 {
        self.chunk_size
    }
    pub fn hash(&self) -> [u8; 32] {
        self.hash
    }
    pub fn hashes(&self) -> &[[u8; 32]] {
        &self.hashes
    }
    pub fn length(&self, index: usize) -> Result<u64> {
        if index >= self.hashes.len() {
            return Err(Error::Protocol("chunk index out of range".into()));
        }
        Ok((self.size - index as u64 * u64::from(self.chunk_size)).min(u64::from(self.chunk_size)))
    }
    fn key(&self, scope: [u8; 32]) -> String {
        let mut digest = Sha256::new();
        digest.update(b"rrsync-local-chunks-v1\0");
        digest.update(scope);
        digest.update(self.size.to_be_bytes());
        digest.update(self.chunk_size.to_be_bytes());
        digest.update(self.hash);
        for hash in &self.hashes {
            digest.update(hash);
        }
        hex::encode(digest.finalize())
    }
}

/// Bind cached bytes to an authenticated peer and a specific receiving context.
/// `root` identifies the local export/destination; `path` is relative to that root.
/// Direction is from the requesting client's perspective. This is not a wire ID.
pub fn scope(peer: [u8; 16], push: bool, root: &str, path: &str) -> Result<[u8; 32]> {
    crate::fs::validate(path)?;
    if root.is_empty() || root.len() > crate::fs::MAX_PATH || root.contains('\0') {
        return Err(Error::InvalidPath(root.into()));
    }
    let mut hash = Sha256::new();
    hash.update(b"rrsync-local-scope-v1\0");
    hash.update(peer);
    hash.update([u8::from(push)]);
    for field in [root, path] {
        hash.update((field.len() as u32).to_be_bytes());
        hash.update(field.as_bytes());
    }
    Ok(hash.finalize().into())
}

/// Exclusive, persistent cache for one freshly agreed file description.
/// Use a private, pre-existing state directory outside the synchronized tree.
/// Files themselves are the receipt journal; no unverified bitmap is trusted.
pub struct Store {
    root: Root,
    description: Description,
    _lock: File,
    _cache_lock: File,
}
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_bytes: u64,
    pub max_transfers: usize,
    /// Expire other inactive transfers before quota checks; zero disables eviction.
    pub retention_seconds: u64,
}
impl Store {
    /// Reserve space for all missing chunks while exclusively owning this cache.
    /// Quota counts logical payload/staging bytes, not filesystem allocation overhead.
    pub fn open_limited(
        directory: &Path,
        scope: [u8; 32],
        description: Description,
        limits: Limits,
    ) -> Result<Self> {
        let root = Root::open(directory)?;
        let lock = root.lock_state("cache.lock")?;
        let key = description.key(scope);
        if limits.retention_seconds > 0 {
            expire(&root, &key, limits.retention_seconds, now()?)?;
        }
        let mut bytes = 0u64;
        let mut transfers = 0usize;
        let mut found = false;
        for (name, metadata) in root.children("")? {
            if name == "cache.lock" {
                continue;
            }
            if name.len() != 64
                || !name.bytes().all(|b| b.is_ascii_hexdigit())
                || !metadata.is_dir()
            {
                return Err(Error::Config("unexpected object in chunk cache".into()));
            }
            transfers += 1;
            found |= name == key;
            for (child, meta) in root.children(&name)? {
                if !meta.is_file() {
                    return Err(Error::Config("unsupported object in chunk cache".into()));
                }
                if child != "lock" && child != "activity" {
                    bytes = bytes
                        .checked_add(meta.len())
                        .ok_or_else(|| Error::Config("cache size overflow".into()))?;
                }
            }
        }
        if transfers + usize::from(!found) > limits.max_transfers || bytes > limits.max_bytes {
            return Err(Error::Config(
                "chunk cache quota exceeded; clean stale state while all users are stopped".into(),
            ));
        }
        let store = Self::open_locked(&root, scope, description, lock)?;
        let needed = store
            .missing()?
            .into_iter()
            .try_fold(0u64, |sum, i| -> Result<u64> {
                Ok(sum + store.description.length(i)?)
            })?;
        if needed > limits.max_bytes - bytes {
            return Err(Error::Config("chunk cache byte quota exceeded".into()));
        }
        Ok(store)
    }
    pub fn open(directory: &Path, scope: [u8; 32], description: Description) -> Result<Self> {
        let parent = Root::open(directory)?;
        let lock = parent.lock_state_shared("cache.lock")?;
        Self::open_locked(&parent, scope, description, lock)
    }
    fn open_locked(
        parent: &Root,
        scope: [u8; 32],
        description: Description,
        cache_lock: File,
    ) -> Result<Self> {
        let key = description.key(scope);
        parent.mkdir(&key)?;
        let root = parent.subtree(&key)?;
        let lock = root.lock_state("lock")?;
        write_activity(&root, now()?)?;
        Ok(Self {
            root,
            description,
            _lock: lock,
            _cache_lock: cache_lock,
        })
    }
    fn name(&self, index: usize) -> Result<String> {
        self.description.length(index)?;
        Ok(format!("{index:08x}.chunk"))
    }
    /// Wrong size/hash is treated as missing. Unsafe paths and I/O errors fail closed.
    fn verified(&self, index: usize) -> Result<Option<File>> {
        let name = self.name(index)?;
        let mut file = match self.root.open_file(&name) {
            Ok(file) => file,
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        if file.metadata()?.len() != self.description.length(index)?
            || hash_file(&mut file)? != self.description.hashes[index]
        {
            return Ok(None);
        }
        Ok(Some(file))
    }
    pub fn missing(&self) -> Result<Vec<usize>> {
        let mut missing = Vec::new();
        for index in 0..self.description.hashes.len() {
            if self.verified(index)?.is_none() {
                missing.push(index);
            }
        }
        Ok(missing)
    }
    /// Publish only after exact length/hash verification and fsync; duplicates are safe.
    pub fn receive(&self, index: usize, reader: &mut impl Read) -> Result<()> {
        let name = self.name(index)?;
        self.root
            .stage_content(
                &name,
                reader,
                self.description.length(index)?,
                self.description.hashes[index],
            )?
            .commit()?;
        write_activity(&self.root, now()?)
    }
    /// Assemble to an anonymous file, verifying chunks and the full digest again.
    /// Installation/mtime/deletion remain the sync engine's responsibility.
    pub fn assemble(&self) -> Result<File> {
        let mut result = tempfile::tempfile()?;
        let mut whole = Sha256::new();
        let mut buf = [0u8; 65_536];
        for index in 0..self.description.hashes.len() {
            let mut file = self
                .verified(index)?
                .ok_or_else(|| Error::HashMismatch(format!("missing or damaged chunk {index}")))?;
            let mut remaining = self.description.length(index)?;
            while remaining > 0 {
                let n = remaining.min(buf.len() as u64) as usize;
                file.read_exact(&mut buf[..n])?;
                whole.update(&buf[..n]);
                result.write_all(&buf[..n])?;
                remaining -= n as u64;
            }
        }
        if <[u8; 32]>::from(whole.finalize()) != self.description.hash {
            return Err(Error::HashMismatch("assembled file".into()));
        }
        result.rewind()?;
        Ok(result)
    }
    /// Explicit eviction under the transfer lock, also removing abandoned staging files.
    /// Keep the lock inode and directory so another owner cannot bypass flock.
    pub fn clear(&self) -> Result<()> {
        for (name, metadata) in self.root.children("")? {
            if name == "lock" || name == "activity" {
                continue;
            }
            let chunk_name = name
                .strip_suffix(".chunk")
                .is_some_and(|s| s.len() == 8 && s.bytes().all(|b| b.is_ascii_hexdigit()));
            if metadata.is_file() && (chunk_name || name.starts_with(".rrsync-")) {
                self.root.remove(&name, false)?;
            }
        }
        write_activity(&self.root, now()?)
    }
}

// Activity is an atomic, fixed-size content record, independent of filesystem mtime.
fn now() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .map_err(|_| Error::Config("system clock is before Unix epoch".into()))
}
fn write_activity(root: &Root, seconds: u64) -> Result<()> {
    let mut record = *b"RRSYNC01\0\0\0\0\0\0\0\0";
    record[8..].copy_from_slice(&seconds.to_be_bytes());
    root.stage_content(
        "activity",
        &mut &record[..],
        16,
        Sha256::digest(record).into(),
    )?
    .commit()
}
fn read_activity(root: &Root) -> Result<Option<u64>> {
    let mut file = match root.open_file("activity") {
        Ok(file) => file,
        Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    if file.metadata()?.len() != 16 {
        return Ok(None);
    }
    let mut record = [0; 16];
    file.read_exact(&mut record)?;
    if &record[..8] != b"RRSYNC01" {
        return Ok(None);
    }
    Ok(Some(u64::from_be_bytes(record[8..].try_into().unwrap())))
}
fn chunk_name(name: &str) -> bool {
    name.strip_suffix(".chunk")
        .is_some_and(|s| s.len() == 8 && s.bytes().all(|b| b.is_ascii_hexdigit()))
}
/// Caller owns the cache-wide exclusive lock. All Store open paths participate.
fn expire(parent: &Root, current: &str, retention: u64, now: u64) -> Result<()> {
    for (name, metadata) in parent.children("")? {
        if name == "cache.lock" || name == current {
            continue;
        }
        if name.len() != 64 || !name.bytes().all(|b| b.is_ascii_hexdigit()) || !metadata.is_dir() {
            return Err(Error::Config("unexpected object in chunk cache".into()));
        }
        let root = parent.subtree(&name)?;
        let _lock = root.lock_state("lock")?;
        let entries = root.children("")?;
        // Validate the entire directory before deleting any of its contents.
        if entries.iter().any(|(name, meta)| {
            !meta.is_file()
                || !(name == "lock"
                    || name == "activity"
                    || chunk_name(name)
                    || name.starts_with(".rrsync-"))
        }) {
            return Err(Error::Config(
                "unexpected object in expiring chunk cache".into(),
            ));
        }
        let Some(last) = read_activity(&root)? else {
            tracing::warn!(transfer=%name, "missing or invalid cache activity; starting retention period");
            write_activity(&root, now)?;
            continue;
        };
        // Future dates (clock rollback) are retained; no subtraction overflow.
        if now.checked_sub(last).is_none_or(|age| age < retention) {
            continue;
        }
        for (child, _) in &entries {
            if child != "lock" && child != "activity" {
                root.remove(child, false)?;
            }
        }
        // Remove the activity record last so interrupted cleanup can be resumed.
        root.remove("activity", false)?;
        root.remove("lock", false)?;
        parent.remove(&name, true)?;
        tracing::info!(transfer=%name, "expired inactive chunk cache");
    }
    Ok(())
}
impl Drop for Store {
    fn drop(&mut self) {
        // Both locks are still held. Failed/aborted transfers get a fresh grace period.
        if let Err(error) = now().and_then(|time| write_activity(&self.root, time)) {
            tracing::warn!(%error, "could not update cache activity on close");
        }
    }
}
