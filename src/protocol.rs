//! Language-independent, bounded big-endian codec. See SUMMARY.md for the wire specification.
pub mod v2;
use crate::{
    Error, Result,
    sync::{Entry, Kind, MAX_ENTRIES, Manifest, Options},
};
pub const VERSION: u8 = 1;
pub const MAX_CONTROL: usize = 8 * 1024 * 1024;
pub const REQUEST_PATH: &str = "/rrsync/v1";
#[derive(Debug)]
pub enum Message {
    Start {
        push: bool,
        options: Options,
        path: String,
        manifest: Manifest,
    },
    Manifest(Manifest),
    Begin(u32),
    Commit {
        index: u32,
        resource: [u8; 32],
    },
    Get(u32),
    Verified(u32),
    Finish,
    Ok,
    Error {
        code: u8,
        text: String,
    },
}
impl Message {
    pub fn error(e: &Error) -> Self {
        let code = match e {
            Error::PermissionDenied => 1,
            Error::InvalidPath(_) => 2,
            Error::Changed(_) => 3,
            Error::HashMismatch(_) => 4,
            Error::Protocol(_) => 5,
            Error::Io(_) => 6,
            Error::Transport(_) | Error::Connection(_) => 7,
            Error::Config(_) => 8,
            Error::Busy => 9,
            Error::Remote { code, .. } => *code,
        };
        let text = e.to_string().chars().take(240).collect();
        Self::Error { code, text }
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut w = Writer(vec![VERSION]);
        match self {
            Self::Start {
                push,
                options,
                path,
                manifest,
            } => {
                w.u8(1);
                w.u8(u8::from(*push));
                w.u8(u8::from(options.delete)
                    | (u8::from(options.checksum) << 1)
                    | (u8::from(options.dry_run) << 2));
                w.string(path)?;
                w.manifest(manifest)?;
            }
            Self::Manifest(m) => {
                w.u8(2);
                w.manifest(m)?;
            }
            Self::Begin(i) => {
                w.u8(3);
                w.u32(*i);
            }
            Self::Commit { index, resource } => {
                w.u8(4);
                w.u32(*index);
                w.0.extend(resource);
            }
            Self::Get(i) => {
                w.u8(5);
                w.u32(*i);
            }
            Self::Verified(i) => {
                w.u8(6);
                w.u32(*i);
            }
            Self::Finish => w.u8(7),
            Self::Ok => w.u8(8),
            Self::Error { code, text } => {
                w.u8(9);
                w.u8(*code);
                w.string(text)?;
            }
        }
        if w.0.len() > MAX_CONTROL {
            return Err(Error::Protocol("control message limit".into()));
        }
        Ok(w.0)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_CONTROL {
            return Err(Error::Protocol("control message limit".into()));
        }
        let mut r = Reader(bytes);
        if r.u8()? != VERSION {
            return Err(Error::Protocol("version mismatch".into()));
        }
        let message = match r.u8()? {
            1 => {
                let push = r.boolean()?;
                let flags = r.u8()?;
                if flags & !7 != 0 {
                    return Err(Error::Protocol("unknown flags".into()));
                }
                let path = r.string()?;
                let manifest = r.manifest()?;
                Self::Start {
                    push,
                    options: Options {
                        delete: flags & 1 != 0,
                        checksum: flags & 2 != 0,
                        dry_run: flags & 4 != 0,
                    },
                    path,
                    manifest,
                }
            }
            2 => Self::Manifest(r.manifest()?),
            3 => Self::Begin(r.u32()?),
            4 => Self::Commit {
                index: r.u32()?,
                resource: r.take(32)?.try_into().unwrap(),
            },
            5 => Self::Get(r.u32()?),
            6 => Self::Verified(r.u32()?),
            7 => Self::Finish,
            8 => Self::Ok,
            9 => {
                let code = r.u8()?;
                let text = r.string()?;
                if text.len() > 1024 {
                    return Err(Error::Protocol("error text limit".into()));
                }
                Self::Error { code, text }
            }
            _ => return Err(Error::Protocol("unknown message tag".into())),
        };
        if !r.0.is_empty() {
            return Err(Error::Protocol("trailing bytes".into()));
        }
        Ok(message)
    }
}
struct Writer(Vec<u8>);
impl Writer {
    fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    fn u32(&mut self, v: u32) {
        self.0.extend(v.to_be_bytes());
    }
    fn string(&mut self, s: &str) -> Result<()> {
        if s.len() > crate::fs::MAX_PATH {
            return Err(Error::Protocol("string limit".into()));
        }
        self.0.extend((s.len() as u16).to_be_bytes());
        self.0.extend(s.as_bytes());
        Ok(())
    }
    fn manifest(&mut self, m: &Manifest) -> Result<()> {
        m.validate(false)?;
        self.u32(m.entries.len() as u32);
        for e in &m.entries {
            self.string(&e.path)?;
            self.u8(match e.kind {
                Kind::File => 0,
                Kind::Directory => 1,
                Kind::Protected => 2,
            });
            self.0.extend(e.size.to_be_bytes());
            self.0.extend(e.mtime.to_be_bytes());
            self.u32(e.ns);
            self.u8(u8::from(e.hash.is_some()));
            if let Some(h) = e.hash {
                self.0.extend(h);
            }
            if self.0.len() > MAX_CONTROL {
                return Err(Error::Protocol("manifest byte limit".into()));
            }
        }
        Ok(())
    }
}
struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if n > self.0.len() {
            return Err(Error::Protocol("truncated message".into()));
        }
        let (a, b) = self.0.split_at(n);
        self.0 = b;
        Ok(a)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn boolean(&mut self) -> Result<bool> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Error::Protocol("invalid boolean".into())),
        }
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn string(&mut self) -> Result<String> {
        let n = u16::from_be_bytes(self.take(2)?.try_into().unwrap()) as usize;
        if n > crate::fs::MAX_PATH {
            return Err(Error::Protocol("string limit".into()));
        }
        String::from_utf8(self.take(n)?.to_vec())
            .map_err(|_| Error::Protocol("invalid UTF-8".into()))
    }
    fn manifest(&mut self) -> Result<Manifest> {
        let n = self.u32()? as usize;
        if n > MAX_ENTRIES {
            return Err(Error::Protocol("manifest entry limit".into()));
        }
        let mut entries = Vec::new();
        for _ in 0..n {
            let path = self.string()?;
            let kind = match self.u8()? {
                0 => Kind::File,
                1 => Kind::Directory,
                2 => Kind::Protected,
                _ => return Err(Error::Protocol("unknown entry type".into())),
            };
            let size = u64::from_be_bytes(self.take(8)?.try_into().unwrap());
            let mtime = i64::from_be_bytes(self.take(8)?.try_into().unwrap());
            let ns = self.u32()?;
            let hash = if self.boolean()? {
                Some(self.take(32)?.try_into().unwrap())
            } else {
                None
            };
            entries.push(Entry {
                path,
                kind,
                size,
                mtime,
                ns,
                hash,
            });
        }
        let m = Manifest { entries };
        m.validate(false)?;
        Ok(m)
    }
}
