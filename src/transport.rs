use crate::{
    Error, Result,
    config::{Config, Permission},
    fs::{Root, snapshot},
    protocol::{MAX_CONTROL, Message, REQUEST_PATH},
    sync::{self, MAX_FILE, Manifest, Options, Plan, Scan},
};
use rns_identity::identity::Identity;
use rns_runtime::{
    lifecycle::ShutdownSignal,
    link_client::LinkSession,
    link_manager::{FileResourceCompletion, LinkManager, RequestOutcome, register_destination},
    reticulum::{self, ReticulumHandle},
};
use std::{
    collections::{BTreeSet, HashMap},
    path::Path,
    sync::{Arc, Mutex, atomic::AtomicBool},
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
pub const APP_NAME: &str = "rrsync.sync";
fn transport(e: impl std::fmt::Display) -> Error {
    Error::Transport(e.to_string())
}

pub fn identity(config: &Config, read_only: bool) -> Result<Identity> {
    if config.identity.exists() {
        return Identity::from_file(&config.identity).map_err(transport);
    }
    if read_only {
        return Err(Error::Config(
            "dry-run requires an existing identity; run 'rrsync identity' first".into(),
        ));
    }
    let id = Identity::new();
    id.to_file(&config.identity).map_err(transport)?;
    Ok(id)
}
pub async fn runtime(
    config: &Config,
    read_only: bool,
) -> Result<(ReticulumHandle, ShutdownSignal)> {
    if read_only {
        let dir = rns_runtime::platform::resolve_config_dir(config.reticulum_config.as_deref());
        let p = rns_runtime::platform::StoragePaths::from_config_dir(&dir);
        if !dir.join("config.yaml").is_file()
            || [
                &p.config_dir,
                &p.interface_dir,
                &p.storage_dir,
                &p.cache_dir,
                &p.resource_dir,
                &p.identity_dir,
                &p.blackhole_dir,
                &p.announce_cache_dir,
            ]
            .iter()
            .any(|p| !p.is_dir())
        {
            return Err(Error::Config(
                "dry-run requires an initialized Reticulum configuration and storage directories"
                    .into(),
            ));
        }
    }
    let shutdown = ShutdownSignal::new();
    let handle = reticulum::init_with_options(
        config.reticulum_config.as_deref(),
        None,
        shutdown.clone(),
        Arc::new(AtomicBool::new(true)),
        rns_runtime::client_options::ClientOptions::server(),
    )
    .await
    .map_err(transport)?;
    Ok((handle, shutdown))
}

struct Session {
    link: [u8; 16],
    push: bool,
    options: Options,
    root: Root,
    source: Manifest,
    destination: Manifest,
    scan: Scan,
    plan: Plan,
    prepared: bool,
    pending: Option<usize>,
    received: Option<FileResourceCompletion>,
    done: BTreeSet<usize>,
    activity: Instant,
}
struct Service {
    root: Root,
    session: Option<Session>,
    incoming: mpsc::Receiver<FileResourceCompletion>,
    identities: Arc<Mutex<HashMap<[u8; 16], [u8; 16]>>>,
    config: Config,
}
impl Service {
    fn receive(&mut self) {
        while let Ok(file) = self.incoming.try_recv() {
            if let Some(s) = &mut self.session
                && s.push
                && s.link == file.link_id
                && s.pending.is_some()
                && s.received.is_none()
            {
                s.received = Some(file);
            }
        }
    }
    fn request(&mut self, link: [u8; 16], path: [u8; 16], data: Vec<u8>) -> Result<RequestOutcome> {
        if path != rns_crypto::sha::truncated_hash(REQUEST_PATH.as_bytes()) {
            return Err(Error::Protocol("unknown request path".into()));
        }
        let authorized = self
            .identities
            .lock()
            .unwrap()
            .get(&link)
            .is_some_and(|id| self.config.permits(id));
        if !authorized {
            return Err(Error::PermissionDenied);
        }
        self.receive();
        let request = Message::decode(&data)?;
        if let Message::Start {
            push,
            options,
            path,
            manifest,
        } = request
        {
            if push
                && self
                    .identities
                    .lock()
                    .unwrap()
                    .get(&link)
                    .is_none_or(|id| self.config.permission(id) != Permission::Full)
            {
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
            let reply = Message::Manifest(remote).encode()?;
            self.session = Some(Session {
                link,
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
            return Ok(RequestOutcome::Reply(reply));
        }
        let s = self
            .session
            .as_mut()
            .filter(|s| s.link == link)
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
                if file.resource_hash != resource
                    || file.data_size as u64 != entry.size
                    || file.metadata.is_some()
                {
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
                return Ok(RequestOutcome::ReplyWithFile {
                    ack: Message::Ok.encode()?,
                    file: Arc::new(file),
                    metadata: None,
                    auto_compress: false,
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
        Ok(RequestOutcome::Reply(Message::Ok.encode()?))
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

pub async fn serve(config: Config, root: Root) -> Result<()> {
    let id = identity(&config, false)?;
    let (runtime, shutdown) = runtime(&config, false).await?;
    let result = serve_on(&runtime, &id, config, root).await;
    shutdown.trigger();
    result
}
pub async fn serve_on(
    runtime: &ReticulumHandle,
    id: &Identity,
    config: Config,
    root: Root,
) -> Result<()> {
    let dest = rns_identity::destination::Destination::hash_from_name_and_identity(
        APP_NAME,
        Some(&id.hash),
    );
    let mut ingress = register_destination(&runtime.transport_tx, dest, APP_NAME);
    let (event_tx, event_rx) = mpsc::channel(1);
    let mut manager = LinkManager::with_destination(
        runtime.transport_tx.clone(),
        event_rx,
        id,
        APP_NAME,
        id.get_signing_key(),
    );
    manager.set_max_request_size(MAX_CONTROL + 1024);
    let (file_tx, file_rx) = mpsc::channel(2);
    manager.set_file_resource_completion_channel(file_tx, MAX_FILE as usize);
    let identities = manager.link_identities_handle();
    let gate = config.clone();
    manager.set_link_identity_gate(move |_, id| gate.permits(&id));
    let service = Arc::new(Mutex::new(Service {
        root,
        session: None,
        incoming: file_rx,
        identities,
        config: config.clone(),
    }));
    let handler = service.clone();
    manager.set_request_handler_ex(move |link, path, data| {
        let mut state = handler.lock().unwrap();
        match state.request(link, path, data) {
            Ok(reply) => reply,
            Err(e) => {
                tracing::warn!(error=%e,"sync request failed");
                if state.session.as_ref().is_some_and(|s| s.link == link) {
                    state.session = None;
                }
                RequestOutcome::Reply(Message::error(&e).encode().expect("bounded error"))
            }
        }
    });
    let (closed_tx, mut closed_rx) = mpsc::channel(64);
    manager.set_link_closed_channel(closed_tx);

    println!("Destination: {}", hex::encode(dest));
    let mut announce = tokio::time::interval(Duration::from_secs(config.announce_seconds));
    let mut maintenance = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            _=tokio::signal::ctrl_c()=>break,
            _=runtime.shutdown.wait()=>break,
            _=announce.tick()=>{
                event_tx.send(rns_transport::link_messages::DestinationEvent::AnnounceRequested(rns_transport::link_messages::AnnounceRequest::normal(APP_NAME.into()))).await.map_err(transport)?;
                manager.try_step();
            },
            Some(event)=ingress.recv()=>{
                if admit(&event,&manager,&service,&config) {
                    event_tx.send(event).await.map_err(transport)?; manager.try_step();
                }
            },
            _=maintenance.tick()=>{
                manager.tick();
                let expired={
                    let mut state=service.lock().unwrap(); state.receive();
                    if state.session.as_ref().is_some_and(|s| s.activity.elapsed() > Duration::from_secs(config.timeout_seconds)) {
                        state.session.take().map(|s| s.link)
                    } else { None }
                };
                if let Some(link_id)=expired {
                    event_tx.send(rns_transport::link_messages::DestinationEvent::LinkClosed {link_id}).await.map_err(transport)?;
                    manager.try_step();
                }
            },
            Some(link)=closed_rx.recv()=>{ let mut s=service.lock().unwrap(); if s.session.as_ref().is_some_and(|s| s.link == link) {s.session=None;} },
        }
    }
    Ok(())
}

/// Admission uses the existing Reticulum decoders; reliability remains in LinkManager.
fn admit(
    event: &rns_transport::link_messages::DestinationEvent,
    manager: &LinkManager,
    service: &Arc<Mutex<Service>>,
    config: &Config,
) -> bool {
    use rns_transport::link_messages::DestinationEvent;
    use rns_wire::{context::PacketContext as C, flags::PacketType};
    if matches!(event, DestinationEvent::LinkRequest { .. }) {
        return manager.active_link_count() < 16;
    }
    let DestinationEvent::InboundPacket { raw, .. } = event else {
        return true;
    };
    let Ok((header, offset)) = rns_wire::header::PacketHeader::unpack(raw) else {
        return false;
    };
    if header.flags.packet_type != PacketType::Data {
        return true;
    }
    if matches!(
        header.context,
        C::LinkIdentify | C::Lrrtt | C::Keepalive | C::LinkClose
    ) {
        return true;
    }
    let mut state = service.lock().unwrap();
    state.receive();
    let authorized = state
        .identities
        .lock()
        .unwrap()
        .get(&header.destination_hash)
        .is_some_and(|id| config.permits(id));
    if !authorized {
        return false;
    }
    if let Some(s) = state
        .session
        .as_mut()
        .filter(|s| s.link == header.destination_hash)
    {
        s.activity = Instant::now();
    }
    if header.context == C::ResourceAdv {
        let Some(link) = manager.get_link(&header.destination_hash) else {
            return false;
        };
        let Ok(plain) = link.decrypt(&raw[offset..]) else {
            return false;
        };
        let Ok(adv) = rns_protocol::resource_adv::ResourceAdvertisement::unpack(&plain) else {
            return false;
        };
        if adv.flags.is_request {
            return adv.data_size <= MAX_CONTROL + 1024;
        }
        if adv.flags.is_response {
            return false;
        }
        let Some(s) = &state.session else {
            return false;
        };
        let Some(index) = s.pending else {
            return false;
        };
        return s.link == header.destination_hash
            && s.push
            && !s.options.dry_run
            && adv.data_size == s.source.entries[index].size as usize
            && !adv.flags.has_metadata;
    }
    true
}

async fn request(link: &mut LinkSession, message: Message, deadline: Duration) -> Result<Message> {
    let bytes = message.encode()?;
    let response = link
        .request_with_metadata_limit(REQUEST_PATH, Some(&bytes), deadline, MAX_CONTROL + 1024)
        .await
        .map_err(transport)?;
    let result = Message::decode(&response.data)?;
    if let Message::Error { code, text } = result {
        return Err(Error::Protocol(format!("remote error {code}: {text}")));
    }
    Ok(result)
}
async fn ok(link: &mut LinkSession, message: Message, deadline: Duration) -> Result<()> {
    if !matches!(request(link, message, deadline).await?, Message::Ok) {
        return Err(Error::Protocol("expected OK".into()));
    }
    Ok(())
}

pub async fn client(
    config: &Config,
    push: bool,
    local: &Path,
    remote: [u8; 16],
    path: String,
    options: Options,
) -> Result<()> {
    let local_root = if push {
        Root::open(local)?
    } else {
        Root::destination(local)?
    };
    let scan = sync::scan(&local_root, options.checksum)?;
    let id = identity(config, options.dry_run)?;
    let (runtime, shutdown) = runtime(config, options.dry_run).await?;
    let deadline = Duration::from_secs(config.timeout_seconds);
    let result = async {
        runtime
            .await_path(remote, deadline)
            .await
            .map_err(transport)?;
        let mut link = LinkSession::open(&runtime, id, remote, 1, deadline)
            .await
            .map_err(transport)?;
        link.identify().await.map_err(transport)?;
        let result = sync_link(&mut link, push, &local_root, scan, path, options, deadline).await;
        let _ = link.close().await;
        result
    }
    .await;
    shutdown.trigger();
    result
}
pub async fn sync_link(
    link: &mut LinkSession,
    push: bool,
    root: &Root,
    scan: Scan,
    path: String,
    options: Options,
    deadline: Duration,
) -> Result<()> {
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
    for op in &plan.operations {
        println!("{:?}\t{}", op.action, op.path);
    }
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
            let mut reader = tokio::fs::File::from_std(file);
            let resource = link
                .send_resource_reader(&mut reader, e.size as usize, None, false, deadline)
                .await
                .map_err(transport)?;
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
            let received = link
                .recv_resource_file(e.size as usize, deadline)
                .await
                .map_err(transport)?;
            if received.data_size as u64 != e.size || received.metadata.is_some() {
                return Err(Error::Protocol("unexpected file Resource".into()));
            }
            let mut file = received.file.into_std().await;
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

#[cfg(test)]
mod tests {
    use super::*;
    fn service(root: Root) -> (Service, mpsc::Sender<FileResourceCompletion>) {
        let (tx, rx) = mpsc::channel(2);
        let link = [1; 16];
        let id = [2; 16];
        let config = Config {
            permits: vec![std::collections::BTreeMap::from([(
                hex::encode(id),
                Permission::Full,
            )])],
            ..Default::default()
        };
        let identities = Arc::new(Mutex::new(HashMap::from([(link, id)])));
        (
            Service {
                root,
                session: None,
                incoming: rx,
                identities,
                config,
            },
            tx,
        )
    }
    fn call(s: &mut Service, message: Message) -> Result<Message> {
        let response = s.request(
            [1; 16],
            rns_crypto::sha::truncated_hash(REQUEST_PATH.as_bytes()),
            message.encode()?,
        )?;
        let RequestOutcome::Reply(bytes) = response else {
            panic!("unexpected file response")
        };
        Message::decode(&bytes)
    }
    #[test]
    fn dry_run_refuses_mutations_and_preserves_missing_root() {
        let temp = tempfile::tempdir().unwrap();
        let (mut s, _) = service(Root::open(temp.path()).unwrap());
        assert!(matches!(
            call(
                &mut s,
                Message::Start {
                    push: true,
                    options: Options {
                        dry_run: true,
                        ..Default::default()
                    },
                    path: "missing".into(),
                    manifest: Manifest::default()
                }
            )
            .unwrap(),
            Message::Manifest(_)
        ));
        assert!(call(&mut s, Message::Begin(0)).is_err());
        call(&mut s, Message::Finish).unwrap();
        assert!(!temp.path().join("missing").exists());
    }
    #[test]
    fn partial_upload_requires_resource_and_commit_before_delete() {
        let source = tempfile::tempdir().unwrap();
        let target = tempfile::tempdir().unwrap();
        std::fs::write(source.path().join("file"), b"new data").unwrap();
        std::fs::write(target.path().join("file"), b"old").unwrap();
        std::fs::write(target.path().join("extra"), b"extra").unwrap();
        let m = sync::scan(&Root::open(source.path()).unwrap(), false)
            .unwrap()
            .manifest;
        let (mut s, tx) = service(Root::open(target.path()).unwrap());
        call(
            &mut s,
            Message::Start {
                push: true,
                options: Options {
                    delete: true,
                    ..Default::default()
                },
                path: String::new(),
                manifest: m,
            },
        )
        .unwrap();
        assert!(call(&mut s, Message::Finish).is_err());
        call(&mut s, Message::Begin(0)).unwrap();
        assert!(
            call(
                &mut s,
                Message::Commit {
                    index: 0,
                    resource: [3; 32]
                }
            )
            .is_err()
        );
        assert_eq!(std::fs::read(target.path().join("file")).unwrap(), b"old");
        assert!(target.path().join("extra").exists());
        tx.try_send(FileResourceCompletion {
            link_id: [1; 16],
            resource_hash: [3; 32],
            file: std::fs::File::open(source.path().join("file")).unwrap(),
            data_size: 8,
            metadata: None,
        })
        .unwrap();
        call(
            &mut s,
            Message::Commit {
                index: 0,
                resource: [3; 32],
            },
        )
        .unwrap();
        assert_eq!(
            std::fs::read(target.path().join("file")).unwrap(),
            b"new data"
        );
        assert!(target.path().join("extra").exists());
        assert!(call(&mut s, Message::Begin(0)).is_err());
        call(&mut s, Message::Finish).unwrap();
        assert!(!target.path().join("extra").exists());
    }
    #[test]
    fn read_only_identity_can_pull_but_cannot_push() {
        let temp = tempfile::tempdir().unwrap();
        let (mut s, _) = service(Root::open(temp.path()).unwrap());
        s.config.permits = vec![std::collections::BTreeMap::from([(
            "others".into(),
            Permission::Read,
        )])];
        for dry_run in [false, true] {
            let result = call(
                &mut s,
                Message::Start {
                    push: true,
                    options: Options {
                        dry_run,
                        ..Default::default()
                    },
                    path: "missing".into(),
                    manifest: Manifest::default(),
                },
            );
            assert!(matches!(result, Err(Error::PermissionDenied)));
            assert!(s.session.is_none());
        }
        call(
            &mut s,
            Message::Start {
                push: false,
                options: Options::default(),
                path: String::new(),
                manifest: Manifest::default(),
            },
        )
        .unwrap();
        call(&mut s, Message::Finish).unwrap();
        assert!(!temp.path().join("missing").exists());
    }
    #[test]
    fn unauthorized_and_busy_sessions_do_not_access_export() {
        let temp = tempfile::tempdir().unwrap();
        let (mut s, _) = service(Root::open(temp.path()).unwrap());
        let start = || Message::Start {
            push: true,
            options: Options::default(),
            path: "new".into(),
            manifest: Manifest::default(),
        };
        assert!(matches!(
            s.request(
                [9; 16],
                rns_crypto::sha::truncated_hash(REQUEST_PATH.as_bytes()),
                start().encode().unwrap()
            ),
            Err(Error::PermissionDenied)
        ));
        call(&mut s, start()).unwrap();
        assert!(call(&mut s, start()).is_err());
        assert!(!temp.path().join("new").exists());
    }
}
