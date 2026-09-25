use crate::{Error, Result};
use rustix::fs::{self as rx, AtFlags, Mode, OFlags, ResolveFlags};
use sha2::{Digest, Sha256};
use std::{
    fs::{File, Metadata},
    io::{Read, Seek, Write},
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::Path,
    sync::Arc,
};

pub const MAX_PATH: usize = 4096;
pub fn validate(path: &str) -> Result<()> {
    if path.is_empty()
        || path.len() > MAX_PATH
        || path.contains('\0')
        || path.split('/').count() > 128
        || path
            .split('/')
            .any(|c| c.is_empty() || c == "." || c == ".." || c.len() > 255)
    {
        return Err(Error::InvalidPath(path.into()));
    }
    Ok(())
}
#[derive(Clone, Debug)]
pub struct Root {
    fd: Arc<File>,
    prefix: String,
}
impl Root {
    /// Stable absolute root name, including a not-yet-created destination suffix.
    pub fn resolved_path(&self) -> Result<std::path::PathBuf> {
        Ok(
            std::fs::read_link(format!("/proc/self/fd/{}", self.fd.as_raw_fd()))?
                .join(&self.prefix),
        )
    }
    pub fn open(path: &Path) -> Result<Self> {
        let fd = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags((OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC).bits() as i32)
            .open(path)?;
        Ok(Self {
            fd: Arc::new(fd),
            prefix: String::new(),
        })
    }
    /// Resolve missing local destinations without creating them (also for dry-run).
    pub fn destination(path: &Path) -> Result<Self> {
        let absolute = std::path::absolute(path)?;
        let mut base = absolute.as_path();
        let mut names = vec![];
        while !base.try_exists()? {
            names.push(
                base.file_name()
                    .ok_or_else(|| Error::InvalidPath(path.display().to_string()))?
                    .to_str()
                    .ok_or_else(|| Error::InvalidPath("non-UTF-8 root".into()))?
                    .to_owned(),
            );
            base = base
                .parent()
                .ok_or_else(|| Error::InvalidPath(path.display().to_string()))?;
        }
        let mut root = Self::open(base)?;
        names.reverse();
        root.prefix = names.join("/");
        if !root.prefix.is_empty() {
            validate(&root.prefix)?;
        }
        Ok(root)
    }
    pub fn subtree(&self, relative: &str) -> Result<Self> {
        if !relative.is_empty() {
            validate(relative)?;
        }
        Ok(Self {
            fd: self.fd.clone(),
            prefix: self.full(relative)?,
        })
    }
    fn full(&self, path: &str) -> Result<String> {
        if !path.is_empty() {
            validate(path)?;
        }
        let p = match (self.prefix.is_empty(), path.is_empty()) {
            (true, _) => path.into(),
            (_, true) => self.prefix.clone(),
            _ => format!("{}/{path}", self.prefix),
        };
        if p.len() > MAX_PATH {
            return Err(Error::InvalidPath(p));
        }
        Ok(p)
    }
    fn open_full(&self, path: &str, flags: OFlags) -> Result<File> {
        Ok(rx::openat2(
            &*self.fd,
            if path.is_empty() { "." } else { path },
            flags | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
            ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS,
        )?
        .into())
    }
    pub fn open_file(&self, path: &str) -> Result<File> {
        let f = self.open_full(&self.full(path)?, OFlags::RDONLY | OFlags::NONBLOCK)?;
        if !f.metadata()?.is_file() {
            return Err(Error::InvalidPath(path.into()));
        }
        Ok(f)
    }
    /// Lock private resume state. Keep the lock inode in place across owners.
    pub(crate) fn lock_state(&self, path: &str) -> Result<File> {
        self.lock_state_with(path, false)
    }
    pub(crate) fn lock_state_shared(&self, path: &str) -> Result<File> {
        self.lock_state_with(path, true)
    }
    fn lock_state_with(&self, path: &str, shared: bool) -> Result<File> {
        let f: File = rx::openat2(
            &*self.fd,
            self.full(path)?.as_str(),
            OFlags::RDWR | OFlags::CREATE | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
            Mode::from_bits_truncate(0o600),
            ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS,
        )?
        .into();
        if !f.metadata()?.is_file() || f.metadata()?.nlink() != 1 {
            return Err(Error::InvalidPath("invalid resume lock".into()));
        }
        rx::flock(
            &f,
            if shared {
                rx::FlockOperation::NonBlockingLockShared
            } else {
                rx::FlockOperation::NonBlockingLockExclusive
            },
        )?;
        Ok(f)
    }
    pub fn metadata(&self, path: &str) -> Result<Metadata> {
        self.open_full(&self.full(path)?, OFlags::PATH)
            .and_then(|f| Ok(f.metadata()?))
    }
    pub fn exists(&self, path: &str) -> Result<bool> {
        match self.metadata(path) {
            Ok(_) => Ok(true),
            Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
        }
    }
    pub fn children(&self, path: &str) -> Result<Vec<(String, Metadata)>> {
        let dir = self.open_full(&self.full(path)?, OFlags::RDONLY | OFlags::DIRECTORY)?;
        let mut out = vec![];
        for entry in std::fs::read_dir(format!("/proc/self/fd/{}", dir.as_raw_fd()))? {
            let entry = entry?;
            let name = entry.file_name().into_string().map_err(|_| {
                Error::InvalidPath("non-UTF-8 filename cannot be represented in protocol v1".into())
            })?;
            let fd = rx::openat(
                &dir,
                name.as_str(),
                OFlags::PATH | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )?;
            let file: File = fd.into();
            out.push((name, file.metadata()?));
            if out.len() > crate::sync::MAX_ENTRIES {
                return Err(Error::Protocol("directory entry limit exceeded".into()));
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }
    pub fn mkdir(&self, path: &str) -> Result<()> {
        let full = self.full(path)?;
        let mut parent = String::new();
        for name in full.split('/').filter(|s| !s.is_empty()) {
            let dir = self.open_full(&parent, OFlags::RDONLY | OFlags::DIRECTORY)?;
            match rx::mkdirat(&dir, name, Mode::from_bits_truncate(0o755)) {
                Ok(()) => dir.sync_all()?,
                Err(rustix::io::Errno::EXIST) => {}
                Err(e) => return Err(e.into()),
            }
            if !parent.is_empty() {
                parent.push('/');
            }
            parent.push_str(name);
            self.open_full(&parent, OFlags::RDONLY | OFlags::DIRECTORY)?;
        }
        Ok(())
    }
    fn parent(&self, path: &str) -> Result<(File, String)> {
        validate(path)?;
        let full = self.full(path)?;
        let (parent, name) = full.rsplit_once('/').unwrap_or(("", &full));
        Ok((
            self.open_full(parent, OFlags::RDONLY | OFlags::DIRECTORY)?,
            name.into(),
        ))
    }
    pub fn remove(&self, path: &str, directory: bool) -> Result<()> {
        let (dir, name) = self.parent(path)?;
        rx::unlinkat(
            &dir,
            name.as_str(),
            if directory {
                AtFlags::REMOVEDIR
            } else {
                AtFlags::empty()
            },
        )?;
        dir.sync_all()?;
        Ok(())
    }
    pub fn set_mtime(&self, path: &str, sec: i64, ns: u32) -> Result<()> {
        let f = self.open_full(&self.full(path)?, OFlags::RDONLY | OFlags::NONBLOCK)?;
        set_mtime(&f, sec, ns)
    }
    pub fn stage(
        &self,
        path: &str,
        reader: &mut impl Read,
        size: u64,
        hash: Option<[u8; 32]>,
        sec: i64,
        ns: u32,
    ) -> Result<Staged> {
        self.stage_inner(path, reader, size, hash, Some((sec, ns)))
    }
    /// Stage verified payloads without requiring filesystem timestamp semantics.
    pub(crate) fn stage_content(
        &self,
        path: &str,
        reader: &mut impl Read,
        size: u64,
        hash: [u8; 32],
    ) -> Result<Staged> {
        self.stage_inner(path, reader, size, Some(hash), None)
    }
    fn stage_inner(
        &self,
        path: &str,
        reader: &mut impl Read,
        size: u64,
        hash: Option<[u8; 32]>,
        mtime: Option<(i64, u32)>,
    ) -> Result<Staged> {
        let (dir, name) = self.parent(path)?;
        // tempfile creates securely with O_EXCL and a randomized name in the same directory.
        let temp = tempfile::Builder::new()
            .prefix(".rrsync-")
            .tempfile_in(format!("/proc/self/fd/{}", dir.as_raw_fd()))?;
        let temp_name = temp
            .path()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        let mut staged = Staged {
            dir,
            name,
            temp_name,
            temp,
        };
        let mut digest = Sha256::new();
        let mut count = 0u64;
        let mut buf = [0u8; 65536];
        loop {
            let n = reader.read(&mut buf)?;
            if n == 0 {
                break;
            }
            count += n as u64;
            if count > size {
                return Err(Error::Changed(path.into()));
            }
            if hash.is_some() {
                digest.update(&buf[..n]);
            }
            staged.temp.write_all(&buf[..n])?;
        }
        if count != size {
            return Err(Error::Changed(path.into()));
        }
        if let Some(expected) = hash
            && <[u8; 32]>::from(digest.finalize()) != expected
        {
            return Err(Error::HashMismatch(path.into()));
        }
        if let Some((sec, ns)) = mtime {
            set_mtime(staged.temp.as_file(), sec, ns)?;
        }
        staged.temp.as_file().sync_all()?;
        Ok(staged)
    }
}
pub struct Staged {
    dir: File,
    name: String,
    temp_name: String,
    temp: tempfile::NamedTempFile,
}
impl Drop for Staged {
    fn drop(&mut self) {
        let _ = rx::unlinkat(&self.dir, self.temp_name.as_str(), AtFlags::empty());
    }
}
impl Staged {
    pub fn commit(self) -> Result<()> {
        rx::renameat(
            &self.dir,
            self.temp_name.as_str(),
            &self.dir,
            self.name.as_str(),
        )?;
        self.dir.sync_all()?;
        Ok(())
    }
}
fn set_mtime(file: &File, sec: i64, ns: u32) -> Result<()> {
    if ns >= 1_000_000_000 {
        return Err(Error::Protocol("invalid nanoseconds".into()));
    }
    rx::futimens(
        file,
        &rx::Timestamps {
            last_access: rx::Timespec {
                tv_sec: 0,
                tv_nsec: rx::UTIME_OMIT,
            },
            last_modification: rx::Timespec {
                tv_sec: sec,
                tv_nsec: ns as _,
            },
        },
    )?;
    Ok(())
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stamp {
    pub dev: u64,
    pub ino: u64,
    pub size: u64,
    pub mtime: i64,
    pub mtime_ns: i64,
    pub ctime: i64,
    pub ctime_ns: i64,
}
impl Stamp {
    pub fn of(m: &Metadata) -> Self {
        Self {
            dev: m.dev(),
            ino: m.ino(),
            size: m.len(),
            mtime: m.mtime(),
            mtime_ns: m.mtime_nsec(),
            ctime: m.ctime(),
            ctime_ns: m.ctime_nsec(),
        }
    }
}
pub fn hash_file(file: &mut File) -> Result<[u8; 32]> {
    let mut h = Sha256::new();
    let mut buf = [0; 65536];
    file.rewind()?;
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
    }
    file.rewind()?;
    Ok(h.finalize().into())
}
pub fn snapshot(root: &Root, path: &str, expected: &Stamp) -> Result<File> {
    let mut source = root.open_file(path)?;
    if Stamp::of(&source.metadata()?) != *expected {
        return Err(Error::Changed(path.into()));
    }
    let mut tmp = tempfile::tempfile()?;
    let n = std::io::copy(&mut (&mut source).take(expected.size + 1), &mut tmp)?;
    if n != expected.size
        || Stamp::of(&source.metadata()?) != *expected
        || Stamp::of(&root.metadata(path)?) != *expected
    {
        return Err(Error::Changed(path.into()));
    }
    tmp.rewind()?;
    Ok(tmp)
}
