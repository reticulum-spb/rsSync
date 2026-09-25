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
    os::unix::fs::FileExt,
    path::Path,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

// Safety bounds, not a default or performance recommendation.
pub const MIN_CHUNK_SIZE: u32 = 4096;
pub const MAX_CHUNK_SIZE: u32 = 16 * 1024 * 1024;
pub const PAGE_CHUNKS: usize = 128;
pub const SHARD_CHUNKS: usize = 256;

#[derive(Clone, Debug)]
pub struct Description {
    size: u64,
    chunk_size: u32,
    hash: [u8; 32],
    hashes: Arc<File>,
}
impl Description {
    /// Validate geometry before allocating a hash table from peer input.
    pub fn count(size: u64, chunk_size: u32) -> Result<usize> {
        if !(MIN_CHUNK_SIZE..=MAX_CHUNK_SIZE).contains(&chunk_size) {
            return Err(Error::Protocol("chunk size outside safety bounds".into()));
        }
        let count = size.div_ceil(u64::from(chunk_size));
        if size > crate::sync::MAX_FILE || count > u32::MAX as u64 {
            return Err(Error::Protocol("chunk count limit exceeded".into()));
        }
        Ok(count as usize)
    }
    /// Header only; pages are negotiated into an anonymous disk-backed hash table.
    pub fn header(size: u64, chunk_size: u32, hash: [u8; 32]) -> Result<Self> {
        Self::count(size, chunk_size)?;
        if size == 0 && hash != <[u8; 32]>::from(Sha256::digest([])) {
            return Err(Error::HashMismatch("empty chunk description".into()));
        }
        Ok(Self {
            size,
            chunk_size,
            hash,
            hashes: Arc::new(tempfile::tempfile()?),
        })
    }
    pub fn chunks(&self) -> usize {
        Self::count(self.size, self.chunk_size).unwrap()
    }
    pub fn page_len(&self, start: usize) -> Result<usize> {
        if start >= self.chunks() || !start.is_multiple_of(PAGE_CHUNKS) {
            return Err(Error::Protocol("invalid hash page offset".into()));
        }
        Ok((self.chunks() - start).min(PAGE_CHUNKS))
    }
    pub fn set_page(&self, start: usize, hashes: &[[u8; 32]]) -> Result<()> {
        if hashes.len() != self.page_len(start)? {
            return Err(Error::Protocol("invalid hash page length".into()));
        }
        for (i, hash) in hashes.iter().enumerate() {
            self.hashes.write_all_at(hash, ((start + i) as u64) * 32)?;
        }
        Ok(())
    }
    pub fn chunk_hash(&self, index: usize) -> Result<[u8; 32]> {
        self.length(index)?;
        let mut hash = [0; 32];
        self.hashes.read_exact_at(&mut hash, index as u64 * 32)?;
        Ok(hash)
    }
    pub fn page(&self, start: usize) -> Result<Vec<[u8; 32]>> {
        (start..start + self.page_len(start)?)
            .map(|i| self.chunk_hash(i))
            .collect()
    }
    /// Read an existing file/snapshot with a fixed 64 KiB buffer.
    pub fn scan(file: &mut File, chunk_size: u32) -> Result<Self> {
        let before = Stamp::of(&file.metadata()?);
        Self::count(before.size, chunk_size)?;
        file.rewind()?;
        let mut whole = Sha256::new();
        let mut hashes = tempfile::tempfile()?;
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
            hashes.write_all(&chunk.finalize())?;
            remaining -= length;
        }
        if file.read(&mut buf[..1])? != 0 || Stamp::of(&file.metadata()?) != before {
            return Err(Error::Changed("chunk source".into()));
        }
        file.rewind()?;
        Ok(Self {
            size: before.size,
            chunk_size,
            hash: whole.finalize().into(),
            hashes: Arc::new(hashes),
        })
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
    pub fn length(&self, index: usize) -> Result<u64> {
        if index >= self.chunks() {
            return Err(Error::Protocol("chunk index out of range".into()));
        }
        Ok((self.size - index as u64 * u64::from(self.chunk_size)).min(u64::from(self.chunk_size)))
    }
    fn key(&self, scope: [u8; 32]) -> String {
        let mut digest = Sha256::new();
        digest.update(b"rrsync-local-chunks-paged\0");
        digest.update(scope);
        digest.update(self.size.to_be_bytes());
        digest.update(self.chunk_size.to_be_bytes());
        digest.update(self.hash);
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
    last_activity: Mutex<u64>,
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
            bytes = bytes
                .checked_add(payload_bytes(&root.subtree(&name)?)?)
                .ok_or_else(|| Error::Config("cache size overflow".into()))?;
        }
        if transfers + usize::from(!found) > limits.max_transfers || bytes > limits.max_bytes {
            return Err(Error::Config(
                "chunk cache quota exceeded; clean stale state while all users are stopped".into(),
            ));
        }
        let store = Self::open_locked(&root, scope, description, lock)?;
        // Reserve absent/wrong-size chunks before their hashes are negotiated.
        // Existing bytes are already included above, including corrupt chunks.
        let mut needed = 0;
        for i in 0..store.description.chunks() {
            let name = store.name(i)?;
            let length = store.description.length(i)?;
            if !store.root.exists(&name)? || store.root.metadata(&name)?.len() != length {
                needed += length;
            }
        }
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
        let activity = now()?;
        write_activity(&root, activity)?;
        Ok(Self {
            root,
            description,
            last_activity: Mutex::new(activity),
            _lock: lock,
            _cache_lock: cache_lock,
        })
    }
    fn name(&self, index: usize) -> Result<String> {
        self.description.length(index)?;
        Ok(format!("{:08x}/{index:08x}.chunk", index / SHARD_CHUNKS))
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
            || hash_file(&mut file)? != self.description.chunk_hash(index)?
        {
            return Ok(None);
        }
        Ok(Some(file))
    }
    pub fn missing(&self, start: usize) -> Result<Vec<usize>> {
        if self.description.chunks() == 0 && start == 0 {
            return Ok(vec![]);
        }
        let count = self.description.page_len(start)?;
        let mut missing = Vec::new();
        for index in start..start + count {
            if self.verified(index)?.is_none() {
                missing.push(index);
            }
        }
        Ok(missing)
    }
    /// Publish only after exact length/hash verification and fsync; duplicates are safe.
    pub fn receive(&self, index: usize, reader: &mut impl Read) -> Result<()> {
        let name = self.name(index)?;
        self.root.mkdir(name.split_once('/').unwrap().0)?;
        self.root
            .stage_content(
                &name,
                reader,
                self.description.length(index)?,
                self.description.chunk_hash(index)?,
            )?
            .commit()?;
        self.touch()
    }
    /// Assemble to an anonymous file, verifying chunks and the full digest again.
    /// Installation/mtime/deletion remain the sync engine's responsibility.
    pub fn assemble(&self) -> Result<File> {
        let mut result = tempfile::tempfile()?;
        let mut whole = Sha256::new();
        let mut buf = [0u8; 65_536];
        for index in 0..self.description.chunks() {
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
        payload_bytes(&self.root)?; // Validate all paths before removal.
        remove_payload(&self.root)?;
        self.touch()
    }
    fn touch(&self) -> Result<()> {
        // Serialize timestamp sampling and publication, including concurrent callers.
        let mut last = self
            .last_activity
            .lock()
            .map_err(|_| Error::Config("cache activity lock poisoned".into()))?;
        refresh_activity(&mut last, now()?, |time| write_activity(&self.root, time))
    }
}

fn refresh_activity(
    last: &mut u64,
    seconds: u64,
    persist: impl FnOnce(u64) -> Result<()>,
) -> Result<()> {
    if *last != seconds {
        persist(seconds)?;
        // Only a successful durable publication may suppress the next write.
        *last = seconds;
    }
    Ok(())
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
        payload_bytes(&root)?; // Validate the whole transfer before any deletion.
        let Some(last) = read_activity(&root)? else {
            tracing::warn!(transfer=%name, "missing or invalid cache activity; starting retention period");
            write_activity(&root, now)?;
            continue;
        };
        // Future dates (clock rollback) are retained; no subtraction overflow.
        if now.checked_sub(last).is_none_or(|age| age < retention) {
            continue;
        }
        remove_payload(&root)?;
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
        if let Err(error) = self.touch() {
            tracing::warn!(%error, "could not update cache activity on close");
        }
    }
}

fn shard_name(name: &str) -> bool {
    name.len() == 8 && name.bytes().all(|b| b.is_ascii_hexdigit())
}
// One directory listing at a time; no tree-sized list of chunk paths in memory.
fn payload_bytes(root: &Root) -> Result<u64> {
    let mut bytes = 0u64;
    for (name, meta) in root.children("")? {
        if meta.is_file() && (name == "lock" || name == "activity") {
            continue;
        }
        if meta.is_file() && name.starts_with(".rrsync-") {
            bytes += meta.len();
            continue;
        }
        if !meta.is_dir() || !shard_name(&name) {
            return Err(Error::Config("unexpected object in chunk cache".into()));
        }
        let shard = u32::from_str_radix(&name, 16).unwrap() as usize;
        for (child, meta) in root.children(&name)? {
            let valid = chunk_name(&child)
                && usize::from_str_radix(&child[..8], 16).unwrap() / SHARD_CHUNKS == shard;
            if !meta.is_file() || !(valid || child.starts_with(".rrsync-")) {
                return Err(Error::Config("unexpected object in chunk shard".into()));
            }
            bytes = bytes
                .checked_add(meta.len())
                .ok_or_else(|| Error::Config("cache size overflow".into()))?;
        }
    }
    Ok(bytes)
}
fn remove_payload(root: &Root) -> Result<()> {
    let mut staging = Vec::new();
    for (name, meta) in root.children("")? {
        if name == "lock" || name == "activity" {
            continue;
        }
        if meta.is_dir() {
            let children = root
                .children(&name)?
                .into_iter()
                .map(|(child, _)| child)
                .collect::<Vec<_>>();
            root.remove_files(&name, &children)?;
            root.remove(&name, true)?;
        } else {
            staging.push(name);
        }
    }
    if !staging.is_empty() {
        root.remove_files("", &staging)?;
    }
    Ok(())
}

#[cfg(test)]
mod activity_tests {
    use super::*;

    #[test]
    fn identical_activity_is_reused_but_clock_changes_are_persisted() {
        let mut last = 100;
        let mut writes = Vec::new();
        for time in [100, 100, 101, 101, 99, 99, 100] {
            refresh_activity(&mut last, time, |value| {
                writes.push(value);
                Ok(())
            })
            .unwrap();
        }
        assert_eq!(writes, [101, 99, 100]);
        assert_eq!(last, 100);
    }

    #[test]
    fn failed_activity_publication_is_retried() {
        let mut last = 100;
        let error = refresh_activity(&mut last, 101, |_| {
            Err(std::io::Error::other("injected publication failure").into())
        });
        assert!(error.is_err());
        assert_eq!(last, 100);
        let mut retried = false;
        refresh_activity(&mut last, 101, |value| {
            assert_eq!(value, 101);
            retried = true;
            Ok(())
        })
        .unwrap();
        assert!(retried);
        assert_eq!(last, 101);
    }
}
