//! Transport-independent synchronization state and orchestration.
use crate::{
    Error, Result,
    config::Permission,
    fs::{Root, snapshot},
    protocol::Message,
    sync::{self, Manifest, Options, Plan, Scan},
};
use std::{
    collections::BTreeSet,
    fs::File,
    time::{Duration, Instant},
};

/// Opaque connection identity supplied by the authenticated transport adapter.
pub type ConnectionId = [u8; 16];
/// Opaque receipt for a completed file transfer, not a content hash.
pub type TransferId = [u8; 32];

pub struct ReceivedFile {
    pub transfer_id: TransferId,
    pub file: File,
    pub size: u64,
    pub has_metadata: bool,
}

pub enum ServerReply {
    Message(Message),
    /// Deliver the control response before beginning the file transfer.
    File {
        message: Message,
        file: File,
    },
}

/// A connection that carries bounded control payloads and file-backed transfers.
/// Implementations own framing, delivery guarantees and deadlines. Engines never
/// retry an uncertain mutation: a new connection must rescan both directories.
#[allow(async_fn_in_trait)]
pub trait SyncTransport {
    async fn request(&mut self, payload: Vec<u8>, deadline: Duration) -> Result<Vec<u8>>;
    async fn send_file(&mut self, file: File, size: u64, deadline: Duration) -> Result<TransferId>;
    async fn receive_file(&mut self, max_size: u64, deadline: Duration) -> Result<ReceivedFile>;
    async fn close(&mut self) -> Result<()>;
}

struct Session {
    connection: [u8; 16],
    push: bool,
    options: Options,
    root: Root,
    source: Manifest,
    destination: Manifest,
    scan: Scan,
    plan: Plan,
    prepared: bool,
    pending: Option<usize>,
    received: Option<ReceivedFile>,
    done: BTreeSet<usize>,
    activity: Instant,
}
pub struct Server {
    root: Root,
    session: Option<Session>,
}
impl Server {
    pub fn new(root: Root) -> Self {
        Self {
            root,
            session: None,
        }
    }

    pub fn disconnect(&mut self, connection: ConnectionId) {
        if self
            .session
            .as_ref()
            .is_some_and(|s| s.connection == connection)
        {
            self.session = None;
        }
    }
    pub fn touch(&mut self, connection: ConnectionId, now: Instant) {
        if let Some(s) = self.session.as_mut().filter(|s| s.connection == connection) {
            s.activity = now;
        }
    }
    pub fn expire(&mut self, now: Instant, timeout: Duration) -> Option<ConnectionId> {
        if self
            .session
            .as_ref()
            .is_some_and(|s| now.saturating_duration_since(s.activity) > timeout)
        {
            self.session.take().map(|s| s.connection)
        } else {
            None
        }
    }
    pub fn accepts_file(&self, connection: ConnectionId, size: u64, has_metadata: bool) -> bool {
        self.session.as_ref().is_some_and(|s| {
            s.connection == connection
                && s.push
                && !s.options.dry_run
                && !has_metadata
                && s.pending
                    .is_some_and(|index| s.source.entries[index].size == size)
        })
    }
    /// Store only a verified file from the expected connection and pending transfer.
    /// Invalid or unsolicited completions are discarded without filesystem changes.
    pub fn receive(&mut self, connection: ConnectionId, file: ReceivedFile) {
        if self.accepts_file(connection, file.size, file.has_metadata)
            && let Some(s) = &mut self.session
            && s.received.is_none()
        {
            s.received = Some(file);
        }
    }
    /// The adapter supplies permission from its authenticated identity, never
    /// from peer-controlled message fields. An error aborts only this connection.
    pub fn handle(
        &mut self,
        connection: ConnectionId,
        permission: Permission,
        request: Message,
    ) -> Result<ServerReply> {
        let result = self.handle_inner(connection, permission, request);
        if result.is_err() {
            self.disconnect(connection);
        }
        result
    }
    fn handle_inner(
        &mut self,
        connection: ConnectionId,
        permission: Permission,
        request: Message,
    ) -> Result<ServerReply> {
        if permission == Permission::Deny {
            return Err(Error::PermissionDenied);
        }
        if let Message::Start {
            push,
            options,
            path,
            manifest,
        } = request
        {
            if push && permission != Permission::Full {
                return Err(Error::PermissionDenied);
            }
            if self.session.is_some() {
                return Err(Error::Protocol("server export is busy".into()));
            }
            let root = self.root.subtree(&path)?;
            if !push && !root.exists("")? {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "source directory does not exist",
                )));
            }
            let scan = sync::scan(&root, options.checksum)?;
            let remote = scan.manifest.clone();
            let (source, destination) = if push {
                (manifest, remote.clone())
            } else {
                (remote.clone(), manifest)
            };
            let plan = sync::plan(&source, &destination, options)?;
            let reply = Message::Manifest(remote);
            self.session = Some(Session {
                connection,
                push,
                options,
                root,
                source,
                destination,
                scan,
                plan,
                prepared: false,
                pending: None,
                received: None,
                done: BTreeSet::new(),
                activity: Instant::now(),
            });
            return Ok(ServerReply::Message(reply));
        }
        let s = self
            .session
            .as_mut()
            .filter(|s| s.connection == connection)
            .ok_or_else(|| Error::Protocol("no active session".into()))?;
        s.activity = Instant::now();
        if s.options.dry_run && !matches!(request, Message::Finish) {
            return Err(Error::Protocol("mutating command in dry-run".into()));
        }
        match request {
            Message::Begin(index) => {
                let i = index as usize;
                require_file(s, i, true)?;
                if s.pending.is_some() {
                    return Err(Error::Protocol("file already pending".into()));
                }
                if !s.prepared {
                    sync::prepare(&s.root, &s.source, &s.plan)?;
                    s.prepared = true;
                }
                s.pending = Some(i);
            }
            Message::Commit { index, resource } => {
                let i = index as usize;
                require_file(s, i, true)?;
                if s.pending != Some(i) {
                    return Err(Error::Protocol("unexpected file commit".into()));
                }
                let mut file = s
                    .received
                    .take()
                    .ok_or_else(|| Error::Protocol("file Resource not received".into()))?;
                let entry = &s.source.entries[i];
                if file.transfer_id != resource || file.size != entry.size || file.has_metadata {
                    return Err(Error::Protocol(
                        "unexpected Resource metadata/hash/size".into(),
                    ));
                }
                sync::install(&s.root, entry, &s.destination, &s.plan, &mut file.file)?;
                s.pending = None;
                s.done.insert(i);
            }
            Message::Get(index) => {
                let i = index as usize;
                require_file(s, i, false)?;
                if s.pending.is_some() {
                    return Err(Error::Protocol("file already pending".into()));
                }
                let e = &s.source.entries[i];
                let stamp = &s.scan.stamps[&e.path];
                let file = snapshot(&s.root, &e.path, stamp)?;
                s.pending = Some(i);
                return Ok(ServerReply::File {
                    message: Message::Ok,
                    file,
                });
            }
            Message::Verified(index) => {
                let i = index as usize;
                require_file(s, i, false)?;
                if s.pending != Some(i) {
                    return Err(Error::Protocol("unexpected verification".into()));
                }
                let e = &s.source.entries[i];
                sync::ensure_unchanged(&s.root, &e.path, &s.scan.stamps[&e.path])?;
                s.pending = None;
                s.done.insert(i);
            }
            Message::Finish => {
                if !s.options.dry_run {
                    if s.pending.is_some() || s.done.len() != s.plan.files.len() {
                        return Err(Error::Protocol("unfinished file transfers".into()));
                    }
                    if s.push {
                        if !s.prepared {
                            sync::prepare(&s.root, &s.source, &s.plan)?;
                        }
                        sync::finish(&s.root, &s.source, &s.plan)?;
                    } else {
                        check_source(&s.root, &s.scan)?;
                    }
                }
                self.session = None;
            }
            _ => return Err(Error::Protocol("unexpected request".into())),
        }
        Ok(ServerReply::Message(Message::Ok))
    }
}
fn require_file(s: &Session, index: usize, push: bool) -> Result<()> {
    if s.push != push || !s.plan.files.contains(&index) || s.done.contains(&index) {
        return Err(Error::Protocol("file not in pending plan".into()));
    }
    Ok(())
}
fn check_source(root: &Root, scan: &Scan) -> Result<()> {
    if let Some(stamp) = scan.stamps.get("") {
        sync::ensure_unchanged(root, "", stamp)?;
    }
    for e in &scan.manifest.entries {
        if e.kind != sync::Kind::Protected {
            sync::ensure_unchanged(root, &e.path, &scan.stamps[&e.path])?;
        }
    }
    Ok(())
}

async fn request(
    link: &mut impl SyncTransport,
    message: Message,
    deadline: Duration,
) -> Result<Message> {
    let bytes = message.encode()?;
    let response = link.request(bytes, deadline).await?;
    let result = Message::decode(&response)?;
    if let Message::Error { code, text } = result {
        return Err(Error::Protocol(format!("remote error {code}: {text}")));
    }
    Ok(result)
}
async fn ok(link: &mut impl SyncTransport, message: Message, deadline: Duration) -> Result<()> {
    if !matches!(request(link, message, deadline).await?, Message::Ok) {
        return Err(Error::Protocol("expected OK".into()));
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct SyncRequest {
    pub push: bool,
    pub path: String,
    pub options: Options,
    pub deadline: Duration,
}

/// Run one synchronization and close the connection on success or failure.
/// Cancellation by dropping this future still requires the caller/adapter to
/// close the connection or let the server inactivity lease expire.
pub async fn synchronize(
    link: &mut impl SyncTransport,
    root: &Root,
    scan: Scan,
    request: SyncRequest,
    mut on_plan: impl FnMut(&Plan),
) -> Result<()> {
    let result = synchronize_inner(link, root, scan, request, &mut on_plan).await;
    // A lost close cannot undo a successful FINISH; preserve the operation result.
    let _ = link.close().await;
    result
}
async fn synchronize_inner(
    link: &mut impl SyncTransport,
    root: &Root,
    scan: Scan,
    job: SyncRequest,
    on_plan: &mut impl FnMut(&Plan),
) -> Result<()> {
    let SyncRequest {
        push,
        path,
        options,
        deadline,
    } = job;
    let reply = request(
        link,
        Message::Start {
            push,
            options,
            path,
            manifest: scan.manifest.clone(),
        },
        deadline,
    )
    .await?;
    let Message::Manifest(remote) = reply else {
        return Err(Error::Protocol("expected manifest".into()));
    };
    let (source, destination) = if push {
        (&scan.manifest, &remote)
    } else {
        (&remote, &scan.manifest)
    };
    let plan = sync::plan(source, destination, options)?;
    on_plan(&plan);
    if options.dry_run {
        return ok(link, Message::Finish, deadline).await;
    }
    if !push {
        sync::prepare(root, source, &plan)?;
    }
    for &index in &plan.files {
        let e = &source.entries[index];
        if push {
            let file = snapshot(root, &e.path, &scan.stamps[&e.path])?;
            ok(link, Message::Begin(index as u32), deadline).await?;
            let resource = link.send_file(file, e.size, deadline).await?;
            sync::ensure_unchanged(root, &e.path, &scan.stamps[&e.path])?;
            ok(
                link,
                Message::Commit {
                    index: index as u32,
                    resource,
                },
                deadline,
            )
            .await?;
        } else {
            ok(link, Message::Get(index as u32), deadline).await?;
            let received = link.receive_file(e.size, deadline).await?;
            if received.size != e.size || received.has_metadata {
                return Err(Error::Protocol("unexpected file transfer".into()));
            }
            let mut file = received.file;
            ok(link, Message::Verified(index as u32), deadline).await?;
            sync::install(root, e, destination, &plan, &mut file)?;
        }
    }
    if push {
        check_source(root, &scan)?;
    }
    ok(link, Message::Finish, deadline).await?;
    if !push {
        sync::finish(root, source, &plan)?;
    }
    Ok(())
}
