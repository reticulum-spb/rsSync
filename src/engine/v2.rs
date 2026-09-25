//! Chunk synchronization over the same opaque file transport as v1.
use super::{ConnectionId, ReceivedFile, SyncRequest, SyncTransport, check_source, require_file};
use crate::{
    Error, Result,
    chunks::{self, Description, Store},
    config::Permission,
    fs::{Root, snapshot},
    protocol::{
        Message as Common,
        v2::{Message, validate_missing},
    },
    sync::{self, Entry, Plan, Scan},
};
use std::{
    collections::BTreeSet,
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
    time::{Duration, Instant},
};

/// The adapter supplies a private directory outside synced trees and a stable
/// canonical root identifier. Limited caches are created lazily on receive;
/// unrestricted library callers must supply an existing directory.
#[derive(Clone, Debug)]
pub struct Cache {
    pub directory: PathBuf,
    pub root_id: String,
    pub limits: Option<chunks::Limits>,
}
impl Cache {
    fn open(
        &self,
        peer: [u8; 16],
        push: bool,
        prefix: &str,
        entry: &Entry,
        description: Description,
    ) -> Result<Store> {
        let path = if prefix.is_empty() {
            entry.path.clone()
        } else {
            format!("{prefix}/{}", entry.path)
        };
        let scope = chunks::scope(peer, push, &self.root_id, &path)?;
        if let Some(limits) = self.limits {
            use std::os::unix::fs::DirBuilderExt;
            std::fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(&self.directory)?;
            Store::open_limited(&self.directory, scope, description, limits)
        } else {
            Store::open(&self.directory, scope, description)
        }
    }
}
pub enum ServerReply {
    Message(Message),
    File { message: Message, file: File },
}
enum Payload {
    Upload {
        store: Store,
        missing: BTreeSet<usize>,
    },
    Download {
        file: File,
        served: BTreeSet<usize>,
    },
}
struct Transfer {
    index: usize,
    description: Description,
    payload: Payload,
    pending: Option<usize>,
    received: Option<ReceivedFile>,
}
pub struct Server {
    base: super::Server,
    cache: Cache,
    peer: Option<[u8; 16]>,
    path: String,
    transfer: Option<Transfer>,
}
fn invalid(text: &str) -> Error {
    Error::Protocol(text.into())
}
fn validate_description(entry: &Entry, description: &Description) -> Result<()> {
    if entry.size != description.size() || entry.hash.is_some_and(|h| h != description.hash()) {
        return Err(Error::HashMismatch(entry.path.clone()));
    }
    Ok(())
}
/// Produce a bounded file-backed Resource from a stable whole-file snapshot.
fn chunk_file(source: &mut File, description: &Description, chunk: usize) -> Result<File> {
    let size = description.length(chunk)?;
    source.seek(SeekFrom::Start(
        chunk as u64 * u64::from(description.chunk_size()),
    ))?;
    let mut file = tempfile::tempfile()?;
    if std::io::copy(&mut source.take(size), &mut file)? != size {
        return Err(invalid("short chunk snapshot"));
    }
    file.rewind()?;
    Ok(file)
}
impl Server {
    pub fn new(root: Root, cache: Cache) -> Self {
        Self {
            base: super::Server::new(root),
            cache,
            peer: None,
            path: String::new(),
            transfer: None,
        }
    }
    pub fn active(&self) -> bool {
        self.base.session.is_some()
    }
    pub fn disconnect(&mut self, connection: ConnectionId) {
        if self
            .base
            .session
            .as_ref()
            .is_some_and(|s| s.connection == connection)
        {
            self.transfer = None;
            self.peer = None;
            self.path.clear();
            self.base.disconnect(connection);
        }
    }
    pub fn touch(&mut self, connection: ConnectionId, now: Instant) {
        self.base.touch(connection, now);
    }
    pub fn expire(&mut self, now: Instant, timeout: Duration) -> Option<ConnectionId> {
        let expired = self.base.expire(now, timeout);
        if expired.is_some() {
            self.transfer = None;
            self.peer = None;
            self.path.clear();
        }
        expired
    }
    pub fn accepts_file(&self, connection: ConnectionId, size: u64, metadata: bool) -> bool {
        self.base
            .session
            .as_ref()
            .is_some_and(|s| s.connection == connection && s.push && !s.options.dry_run)
            && !metadata
            && self.transfer.as_ref().is_some_and(|t| {
                t.received.is_none()
                    && t.pending
                        .is_some_and(|i| t.description.length(i).ok() == Some(size))
            })
    }
    pub fn receive(&mut self, connection: ConnectionId, file: ReceivedFile) {
        if self.accepts_file(connection, file.size, file.has_metadata) {
            self.transfer.as_mut().unwrap().received = Some(file);
        }
    }
    /// Identity and permission come from the authenticated adapter, not wire fields.
    pub fn handle(
        &mut self,
        connection: ConnectionId,
        peer: [u8; 16],
        permission: Permission,
        message: Message,
    ) -> Result<ServerReply> {
        let result = self.handle_inner(connection, peer, permission, message);
        if result.is_err() {
            self.disconnect(connection);
        }
        result
    }
    fn handle_inner(
        &mut self,
        connection: ConnectionId,
        peer: [u8; 16],
        permission: Permission,
        message: Message,
    ) -> Result<ServerReply> {
        if permission == Permission::Deny {
            return Err(Error::PermissionDenied);
        }
        if let Message::Common(start @ Common::Start { .. }) = message {
            let path = match &start {
                Common::Start { path, .. } => path.clone(),
                _ => unreachable!(),
            };
            let reply = self.base.handle_inner(connection, permission, start)?;
            self.peer = Some(peer);
            self.path = path;
            return match reply {
                super::ServerReply::Message(m) => Ok(ServerReply::Message(Message::Common(m))),
                _ => unreachable!(),
            };
        }
        let s = self
            .base
            .session
            .as_mut()
            .filter(|s| s.connection == connection)
            .ok_or_else(|| invalid("no active session"))?;
        if self.peer != Some(peer) || (s.push && permission != Permission::Full) {
            return Err(Error::PermissionDenied);
        }
        s.activity = Instant::now();
        if s.options.dry_run && !matches!(message, Message::Common(Common::Finish)) {
            return Err(invalid("mutating command in dry-run"));
        }
        match message {
            Message::Description { index, description } => {
                let i = index as usize;
                require_file(s, i, true)?;
                if self.transfer.is_some() {
                    return Err(invalid("file already pending"));
                }
                validate_description(&s.source.entries[i], &description)?;
                let store = self.cache.open(
                    peer,
                    true,
                    &self.path,
                    &s.source.entries[i],
                    description.clone(),
                )?;
                let missing: BTreeSet<_> = store.missing()?.into_iter().collect();
                let reply = Message::Missing {
                    index,
                    chunks: missing.iter().map(|&i| i as u32).collect(),
                };
                self.transfer = Some(Transfer {
                    index: i,
                    description,
                    payload: Payload::Upload { store, missing },
                    pending: None,
                    received: None,
                });
                s.pending = Some(i);
                return Ok(ServerReply::Message(reply));
            }
            Message::Describe { index, chunk_size } => {
                let i = index as usize;
                require_file(s, i, false)?;
                if self.transfer.is_some() {
                    return Err(invalid("file already pending"));
                }
                Description::count(s.source.entries[i].size, chunk_size)?;
                let e = &s.source.entries[i];
                let mut file = snapshot(&s.root, &e.path, &s.scan.stamps[&e.path])?;
                let description = Description::scan(&mut file, chunk_size)?;
                validate_description(e, &description)?;
                self.transfer = Some(Transfer {
                    index: i,
                    description: description.clone(),
                    payload: Payload::Download {
                        file,
                        served: BTreeSet::new(),
                    },
                    pending: None,
                    received: None,
                });
                s.pending = Some(i);
                return Ok(ServerReply::Message(Message::Description {
                    index,
                    description,
                }));
            }
            Message::ChunkBegin { index, chunk } => {
                let t = transfer(&mut self.transfer, index)?;
                let Payload::Upload { missing, .. } = &t.payload else {
                    return Err(invalid("not an upload"));
                };
                if t.pending.is_some() || !missing.contains(&(chunk as usize)) {
                    return Err(invalid("chunk not pending"));
                }
                t.pending = Some(chunk as usize);
            }
            Message::ChunkCommit {
                index,
                chunk,
                resource,
            } => {
                let t = transfer(&mut self.transfer, index)?;
                let Payload::Upload { store, missing } = &mut t.payload else {
                    return Err(invalid("not an upload"));
                };
                if t.pending != Some(chunk as usize) {
                    return Err(invalid("unexpected chunk commit"));
                }
                let mut received = t
                    .received
                    .take()
                    .ok_or_else(|| invalid("chunk Resource not received"))?;
                if received.transfer_id != resource
                    || received.has_metadata
                    || received.size != t.description.length(chunk as usize)?
                {
                    return Err(invalid("unexpected chunk Resource"));
                }
                store.receive(chunk as usize, &mut received.file)?;
                missing.remove(&(chunk as usize));
                t.pending = None;
            }
            Message::ChunkGet { index, chunk } => {
                let t = transfer(&mut self.transfer, index)?;
                let Payload::Download { file, served } = &mut t.payload else {
                    return Err(invalid("not a download"));
                };
                if t.pending.is_some() || served.contains(&(chunk as usize)) {
                    return Err(invalid("chunk already requested"));
                }
                let file = chunk_file(file, &t.description, chunk as usize)?;
                t.pending = Some(chunk as usize);
                return Ok(ServerReply::File {
                    message: Message::Common(Common::Ok),
                    file,
                });
            }
            Message::ChunkVerified { index, chunk } => {
                let t = transfer(&mut self.transfer, index)?;
                let Payload::Download { served, .. } = &mut t.payload else {
                    return Err(invalid("not a download"));
                };
                if t.pending != Some(chunk as usize) {
                    return Err(invalid("unexpected chunk verification"));
                }
                served.insert(chunk as usize);
                t.pending = None;
            }
            Message::FileCommit(index) => {
                let t = transfer(&mut self.transfer, index)?;
                let Payload::Upload { store, missing } = &t.payload else {
                    return Err(invalid("not an upload"));
                };
                if t.pending.is_some() || !missing.is_empty() {
                    return Err(invalid("unfinished chunks"));
                }
                let mut file = store.assemble()?;
                if !s.prepared {
                    sync::prepare(&s.root, &s.source, &s.plan)?;
                    s.prepared = true;
                }
                let mut entry = s.source.entries[t.index].clone();
                entry.hash = Some(t.description.hash());
                sync::install(&s.root, &entry, &s.destination, &s.plan, &mut file)?;
                clear_committed(store);
                s.done.insert(t.index);
                s.pending = None;
                self.transfer = None;
            }
            Message::FileVerified(index) => {
                let t = transfer(&mut self.transfer, index)?;
                if !matches!(t.payload, Payload::Download { .. }) || t.pending.is_some() {
                    return Err(invalid("unexpected file verification"));
                }
                let e = &s.source.entries[t.index];
                sync::ensure_unchanged(&s.root, &e.path, &s.scan.stamps[&e.path])?;
                s.done.insert(t.index);
                s.pending = None;
                self.transfer = None;
            }
            Message::Common(Common::Finish) => {
                if self.transfer.is_some() {
                    return Err(invalid("unfinished file transfer"));
                }
                self.base
                    .handle_inner(connection, permission, Common::Finish)?;
                self.peer = None;
                self.path.clear();
            }
            _ => return Err(invalid("unexpected v2 request")),
        }
        Ok(ServerReply::Message(Message::Common(Common::Ok)))
    }
}
fn transfer(state: &mut Option<Transfer>, index: u32) -> Result<&mut Transfer> {
    state
        .as_mut()
        .filter(|t| t.index == index as usize)
        .ok_or_else(|| invalid("unexpected file index"))
}
fn clear_committed(store: &Store) {
    if let Err(error) = store.clear() {
        tracing::warn!(%error, "could not evict committed chunks");
    }
}
async fn request(
    link: &mut impl SyncTransport,
    message: Message,
    deadline: Duration,
) -> Result<Message> {
    let response = link.request(message.encode()?, deadline).await?;
    let message = Message::decode(&response)?;
    if let Message::Common(Common::Error { code, text }) = message {
        return Err(Error::Remote { code, text });
    }
    Ok(message)
}
async fn ok(link: &mut impl SyncTransport, message: Message, deadline: Duration) -> Result<()> {
    if !matches!(
        request(link, message, deadline).await?,
        Message::Common(Common::Ok)
    ) {
        return Err(invalid("expected v2 OK"));
    }
    Ok(())
}
#[derive(Clone, Debug)]
pub struct Resume {
    pub cache: Cache,
    /// Authenticated remote identity (never a connection ID).
    pub peer: [u8; 16],
    pub chunk_size: u32,
}
pub async fn synchronize(
    link: &mut impl SyncTransport,
    root: &Root,
    scan: Scan,
    job: SyncRequest,
    resume: Resume,
    mut on_plan: impl FnMut(&Plan),
) -> Result<()> {
    let result = synchronize_inner(link, root, scan, job, resume, &mut on_plan).await;
    let _ = link.close().await;
    result
}
async fn synchronize_inner(
    link: &mut impl SyncTransport,
    root: &Root,
    scan: Scan,
    job: SyncRequest,
    resume: Resume,
    on_plan: &mut impl FnMut(&Plan),
) -> Result<()> {
    let SyncRequest {
        push,
        path,
        options,
        deadline,
    } = job;
    Description::count(0, resume.chunk_size)?;
    let Message::Common(Common::Manifest(remote)) = request(
        link,
        Message::Common(Common::Start {
            push,
            options,
            path: path.clone(),
            manifest: scan.manifest.clone(),
        }),
        deadline,
    )
    .await?
    else {
        return Err(invalid("expected manifest"));
    };
    let (source, destination) = if push {
        (&scan.manifest, &remote)
    } else {
        (&remote, &scan.manifest)
    };
    let plan = sync::plan(source, destination, options)?;
    on_plan(&plan);
    if options.dry_run {
        return ok(link, Message::Common(Common::Finish), deadline).await;
    }
    if !push {
        sync::prepare(root, source, &plan)?;
    }
    for &i in &plan.files {
        let index = i as u32;
        let e = &source.entries[i];
        if push {
            let mut file = snapshot(root, &e.path, &scan.stamps[&e.path])?;
            let description = Description::scan(&mut file, resume.chunk_size)?;
            validate_description(e, &description)?;
            let Message::Missing {
                index: actual,
                chunks,
            } = request(
                link,
                Message::Description {
                    index,
                    description: description.clone(),
                },
                deadline,
            )
            .await?
            else {
                return Err(invalid("expected missing chunks"));
            };
            if actual != index {
                return Err(invalid("unexpected file index"));
            }
            validate_missing(&chunks, description.hashes().len())?;
            tracing::info!(path=%e.path, cached_chunks=description.hashes().len()-chunks.len(), missing_chunks=chunks.len(), "resume plan");
            for chunk in chunks {
                let part = chunk_file(&mut file, &description, chunk as usize)?;
                ok(link, Message::ChunkBegin { index, chunk }, deadline).await?;
                let resource = link
                    .send_file(part, description.length(chunk as usize)?, deadline)
                    .await?;
                ok(
                    link,
                    Message::ChunkCommit {
                        index,
                        chunk,
                        resource,
                    },
                    deadline,
                )
                .await?;
            }
            sync::ensure_unchanged(root, &e.path, &scan.stamps[&e.path])?;
            ok(link, Message::FileCommit(index), deadline).await?;
        } else {
            let Message::Description {
                index: actual,
                description,
            } = request(
                link,
                Message::Describe {
                    index,
                    chunk_size: resume.chunk_size,
                },
                deadline,
            )
            .await?
            else {
                return Err(invalid("expected description"));
            };
            if actual != index || description.chunk_size() != resume.chunk_size {
                return Err(invalid("unexpected description"));
            }
            validate_description(e, &description)?;
            let store = resume
                .cache
                .open(resume.peer, false, &path, e, description.clone())?;
            let missing = store.missing()?;
            tracing::info!(path=%e.path, cached_chunks=description.hashes().len()-missing.len(), missing_chunks=missing.len(), "resume plan");
            for chunk in missing {
                ok(
                    link,
                    Message::ChunkGet {
                        index,
                        chunk: chunk as u32,
                    },
                    deadline,
                )
                .await?;
                let size = description.length(chunk)?;
                let mut received = link.receive_file(size, deadline).await?;
                if received.has_metadata || received.size != size {
                    return Err(invalid("unexpected chunk Resource"));
                }
                store.receive(chunk, &mut received.file)?;
                ok(
                    link,
                    Message::ChunkVerified {
                        index,
                        chunk: chunk as u32,
                    },
                    deadline,
                )
                .await?;
            }
            let mut file = store.assemble()?;
            ok(link, Message::FileVerified(index), deadline).await?;
            let mut entry = e.clone();
            entry.hash = Some(description.hash());
            sync::install(root, &entry, destination, &plan, &mut file)?;
            clear_committed(&store);
        }
    }
    if push {
        check_source(root, &scan)?;
    }
    ok(link, Message::Common(Common::Finish), deadline).await?;
    if !push {
        sync::finish(root, source, &plan)?;
    }
    Ok(())
}
