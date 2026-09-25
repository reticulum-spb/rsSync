use crate::{
    Error, Result,
    config::{Config, Permission},
    engine::{self, SyncTransport, v2},
    fs::Root,
    protocol::{MAX_CONTROL, Message, REQUEST_PATH},
    sync::{self, MAX_FILE, Options},
};
use rns_identity::identity::Identity;
use rns_runtime::{
    lifecycle::ShutdownSignal,
    link_client::LinkSession,
    link_manager::{FileResourceCompletion, LinkManager, RequestOutcome, register_destination},
    reticulum::{self, ReticulumHandle},
};
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex, atomic::AtomicBool},
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
pub const APP_NAME: &str = "rrsync.sync";
fn transport(e: impl std::fmt::Display) -> Error {
    Error::Transport(e.to_string())
}

fn link_error(error: rns_runtime::link_client::LinkClientError) -> Error {
    use rns_runtime::link_client::LinkClientError as E;
    let retryable = matches!(
        &error,
        E::TransportUnavailable | E::Timeout(_) | E::PubkeyNotDiscovered | E::HandshakeFailed(_)
    ) || matches!(&error, E::Resource(text) if text == "resource sender retries exhausted");
    if retryable {
        Error::Connection(error.to_string())
    } else {
        transport(error)
    }
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

struct Service {
    engine: engine::Server,
    v2: Option<v2::Server>,
    versions: HashMap<[u8; 16], u8>,
    incoming: mpsc::Receiver<FileResourceCompletion>,
    identities: Arc<Mutex<HashMap<[u8; 16], [u8; 16]>>>,
    config: Config,
}
fn is_v2(path: [u8; 16]) -> bool {
    path == rns_crypto::sha::truncated_hash(crate::protocol::v2::REQUEST_PATH.as_bytes())
}
fn cache(config: &Config, root: &Root) -> Result<Option<v2::Cache>> {
    let Some(resume) = &config.resume else {
        return Ok(None);
    };
    let root_path = root.resolved_path()?;
    let directory = Root::destination(&resume.directory)?.resolved_path()?;
    if directory.starts_with(&root_path) || root_path.starts_with(&directory) {
        return Err(Error::Config(
            "resume directory must be outside the synchronized tree".into(),
        ));
    }
    Ok(Some(v2::Cache {
        directory,
        root_id: root_path
            .to_str()
            .ok_or_else(|| Error::Config("non-UTF-8 root".into()))?
            .into(),
        limits: Some(crate::chunks::Limits {
            max_bytes: resume.max_bytes,
            max_transfers: resume.max_transfers,
        }),
    }))
}
impl Service {
    fn disconnect(&mut self, link: [u8; 16]) {
        self.engine.disconnect(link);
        if let Some(engine) = &mut self.v2 {
            engine.disconnect(link);
        }
    }
    fn touch(&mut self, link: [u8; 16], now: Instant) {
        self.engine.touch(link, now);
        if let Some(engine) = &mut self.v2 {
            engine.touch(link, now);
        }
    }
    fn expire(&mut self, now: Instant, timeout: Duration) -> Option<[u8; 16]> {
        let v1 = self.engine.expire(now, timeout);
        let v2 = self.v2.as_mut().and_then(|e| e.expire(now, timeout));
        v1.or(v2)
    }
    fn accepts_file(&self, link: [u8; 16], size: u64, metadata: bool) -> bool {
        self.engine.accepts_file(link, size, metadata)
            || self
                .v2
                .as_ref()
                .is_some_and(|e| e.accepts_file(link, size, metadata))
    }
    fn receive(&mut self) {
        while let Ok(file) = self.incoming.try_recv() {
            let link = file.link_id;
            let received = engine::ReceivedFile {
                transfer_id: file.resource_hash,
                file: file.file,
                size: file.data_size as u64,
                has_metadata: file.metadata.is_some(),
            };
            if self.versions.get(&link) == Some(&2) {
                if let Some(engine) = &mut self.v2 {
                    engine.receive(link, received);
                }
            } else {
                self.engine.receive(link, received);
            }
        }
    }
    fn request(&mut self, link: [u8; 16], path: [u8; 16], data: Vec<u8>) -> Result<RequestOutcome> {
        let v2 = is_v2(path);
        if !v2 && path != rns_crypto::sha::truncated_hash(REQUEST_PATH.as_bytes()) {
            return Err(Error::Protocol("unknown request path".into()));
        }
        let peer = self
            .identities
            .lock()
            .unwrap()
            .get(&link)
            .copied()
            .ok_or(Error::PermissionDenied)?;
        let permission = self.config.permission(&peer);
        if permission == Permission::Deny {
            return Err(Error::PermissionDenied);
        }
        let version = if v2 { 2 } else { 1 };
        if self.versions.get(&link).is_some_and(|&v| v != version) {
            return Err(Error::Protocol("cannot change protocol on a Link".into()));
        }
        self.versions.insert(link, version);
        self.receive();
        if v2 {
            if self.engine.active() {
                return Err(Error::Busy);
            }
            let engine = self.v2.as_mut().ok_or_else(|| {
                Error::Config("v2 is disabled; configure resume on the server".into())
            })?;
            match engine.handle(
                link,
                peer,
                permission,
                crate::protocol::v2::Message::decode(&data)?,
            )? {
                v2::ServerReply::Message(message) => Ok(RequestOutcome::Reply(message.encode()?)),
                v2::ServerReply::File { message, file } => Ok(RequestOutcome::ReplyWithFile {
                    ack: message.encode()?,
                    file: Arc::new(file),
                    metadata: None,
                    auto_compress: false,
                }),
            }
        } else {
            if self.v2.as_ref().is_some_and(|e| e.active()) {
                return Err(Error::Busy);
            }
            match self
                .engine
                .handle(link, permission, Message::decode(&data)?)?
            {
                engine::ServerReply::Message(message) => {
                    Ok(RequestOutcome::Reply(message.encode()?))
                }
                engine::ServerReply::File { message, file } => Ok(RequestOutcome::ReplyWithFile {
                    ack: message.encode()?,
                    file: Arc::new(file),
                    metadata: None,
                    auto_compress: false,
                }),
            }
        }
    }
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
    let v2 = cache(&config, &root)?.map(|cache| v2::Server::new(root.clone(), cache));
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
        engine: engine::Server::new(root),
        v2,
        versions: HashMap::new(),
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
                state.disconnect(link);
                let error = Message::error(&e);
                let bytes = if is_v2(path) {
                    crate::protocol::v2::Message::Common(error).encode()
                } else {
                    error.encode()
                };
                RequestOutcome::Reply(bytes.expect("bounded error"))
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
                    state.expire(Instant::now(), Duration::from_secs(config.timeout_seconds))
                };
                if let Some(link_id)=expired {
                    event_tx.send(rns_transport::link_messages::DestinationEvent::LinkClosed {link_id}).await.map_err(transport)?;
                    manager.try_step();
                }
            },
            Some(link)=closed_rx.recv()=>{ let mut s=service.lock().unwrap(); s.disconnect(link); s.versions.remove(&link); },
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
    state.touch(header.destination_hash, Instant::now());
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
        return state.accepts_file(
            header.destination_hash,
            adv.data_size as u64,
            adv.flags.has_metadata,
        );
    }
    true
}

pub async fn client(
    config: &Config,
    push: bool,
    local: &Path,
    remote: [u8; 16],
    path: String,
    options: Options,
) -> Result<()> {
    let id = identity(config, options.dry_run)?;
    let (runtime, shutdown) = runtime(config, options.dry_run).await?;
    let deadline = Duration::from_secs(config.timeout_seconds);
    let result = crate::reconnect::run(&config.reconnect, |_| {
        let id = id.clone();
        let path = path.clone();
        let runtime = &runtime;
        async move {
            let local_root = if push {
                Root::open(local)?
            } else {
                Root::destination(local)?
            };
            let resume_cache = if config.protocol == 2 {
                cache(config, &local_root)?
            } else {
                None
            };

            let scan = sync::scan(&local_root, options.checksum)?;
            runtime
                .await_path(remote, deadline)
                .await
                .map_err(|e| Error::Connection(e.to_string()))?;
            let (mut link, peer) = if config.protocol == 2 {
                use rns_transport::messages::{TransportQuery, TransportQueryResponse};
                let key = match runtime
                    .query_control(TransportQuery::Recall {
                        destination_hash: remote,
                    })
                    .await
                {
                    Some(TransportQueryResponse::Announce(Some(entry))) => entry.public_key,
                    _ => None,
                }
                .ok_or_else(|| Error::Connection("remote identity not discovered".into()))?;
                let peer = rns_crypto::sha::truncated_hash(&key);
                if rns_identity::destination::Destination::hash_from_name_and_identity(
                    APP_NAME,
                    Some(&peer),
                ) != remote
                {
                    return Err(Error::Transport(
                        "remote destination identity mismatch".into(),
                    ));
                }
                (
                    LinkSession::open_with_public_key(runtime, id, remote, key, 1, deadline)
                        .await
                        .map_err(link_error)?,
                    peer,
                )
            } else {
                (
                    LinkSession::open(runtime, id, remote, 1, deadline)
                        .await
                        .map_err(link_error)?,
                    [0; 16],
                )
            };
            link.identify().await.map_err(link_error)?;
            let job = engine::SyncRequest {
                push,
                path,
                options,
                deadline,
            };
            let on_plan = |plan: &sync::Plan| {
                for op in &plan.operations {
                    println!("{:?}\t{}", op.action, op.path);
                }
            };
            if let Some(cache) = resume_cache {
                v2::synchronize(
                    &mut ReticulumTransport(&mut link, crate::protocol::v2::REQUEST_PATH),
                    &local_root,
                    scan,
                    job,
                    v2::Resume {
                        cache,
                        peer,
                        chunk_size: config.resume.as_ref().unwrap().chunk_size,
                    },
                    on_plan,
                )
                .await
            } else {
                engine::synchronize(
                    &mut ReticulumTransport(&mut link, REQUEST_PATH),
                    &local_root,
                    scan,
                    job,
                    on_plan,
                )
                .await
            }
        }
    })
    .await;
    shutdown.trigger();
    result
}

struct ReticulumTransport<'a>(&'a mut LinkSession, &'static str);
impl SyncTransport for ReticulumTransport<'_> {
    async fn request(&mut self, payload: Vec<u8>, deadline: Duration) -> Result<Vec<u8>> {
        let response = self
            .0
            .request_with_metadata_limit(self.1, Some(&payload), deadline, MAX_CONTROL + 1024)
            .await
            .map_err(link_error)?;
        Ok(response.data)
    }
    async fn send_file(
        &mut self,
        file: std::fs::File,
        size: u64,
        deadline: Duration,
    ) -> Result<engine::TransferId> {
        let mut reader = tokio::fs::File::from_std(file);
        self.0
            .send_resource_reader(&mut reader, size as usize, None, false, deadline)
            .await
            .map_err(link_error)
    }
    async fn receive_file(
        &mut self,
        max_size: u64,
        deadline: Duration,
    ) -> Result<engine::ReceivedFile> {
        let received = self
            .0
            .recv_resource_file(max_size as usize, deadline)
            .await
            .map_err(link_error)?;
        Ok(engine::ReceivedFile {
            transfer_id: received.resource_hash,
            file: received.file.into_std().await,
            size: received.data_size as u64,
            has_metadata: received.metadata.is_some(),
        })
    }
    async fn close(&mut self) -> Result<()> {
        self.0.close().await.map_err(link_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::ResumeConfig, protocol::v2::Message as V2};
    fn config(directory: &Path) -> Config {
        Config {
            permits: vec![std::collections::BTreeMap::from([(
                "others".into(),
                Permission::Full,
            )])],
            resume: Some(ResumeConfig {
                chunk_size: 4096,
                directory: directory.into(),
                max_bytes: 1_000_000,
                max_transfers: 10,
            }),
            ..Config::default()
        }
    }
    fn start() -> Message {
        Message::Start {
            push: true,
            options: Options::default(),
            path: String::new(),
            manifest: sync::Manifest::default(),
        }
    }
    #[test]
    fn versions_share_export_ownership_and_cannot_mix_on_a_link() {
        let tmp = tempfile::tempdir().unwrap();
        let export = tmp.path().join("export");
        std::fs::create_dir(&export).unwrap();
        let root = Root::open(&export).unwrap();
        let config = config(&tmp.path().join("cache"));
        let (_, rx) = mpsc::channel(2);
        let mut service = Service {
            engine: engine::Server::new(root.clone()),
            v2: Some(v2::Server::new(
                root.clone(),
                cache(&config, &root).unwrap().unwrap(),
            )),
            versions: HashMap::new(),
            incoming: rx,
            identities: Arc::new(Mutex::new(HashMap::from([
                ([1; 16], [7; 16]),
                ([2; 16], [8; 16]),
            ]))),
            config,
        };
        let p1 = rns_crypto::sha::truncated_hash(REQUEST_PATH.as_bytes());
        let p2 = rns_crypto::sha::truncated_hash(crate::protocol::v2::REQUEST_PATH.as_bytes());
        service
            .request([1; 16], p1, start().encode().unwrap())
            .unwrap();
        assert!(
            service
                .request([2; 16], p2, V2::Common(start()).encode().unwrap())
                .is_err()
        );
        assert!(service.engine.active());
        service
            .request([1; 16], p1, Message::Finish.encode().unwrap())
            .unwrap();
        assert!(
            service
                .request([1; 16], p2, V2::Common(start()).encode().unwrap())
                .is_err()
        );
        service
            .request([2; 16], p2, V2::Common(start()).encode().unwrap())
            .unwrap();
        assert!(
            service
                .request([1; 16], p1, start().encode().unwrap())
                .is_err()
        );
        assert!(service.v2.as_ref().unwrap().active());
        service
            .request([2; 16], p2, V2::Common(Message::Finish).encode().unwrap())
            .unwrap();
        assert!(!tmp.path().join("cache").exists()); // planning does not create state
    }
    #[test]
    fn native_errors_only_retry_connection_failures() {
        use rns_runtime::link_client::LinkClientError as E;
        for error in [
            E::Timeout("test"),
            E::HandshakeFailed("link closed".into()),
            E::Resource("resource sender retries exhausted".into()),
        ] {
            assert!(crate::reconnect::retryable(&link_error(error)));
        }
        for error in [
            E::ProofInvalid("test".into()),
            E::LinkCrypto("test".into()),
            E::NoSigningKey,
            E::Resource("resource file write: disk full".into()),
            E::UnexpectedResponse("malformed".into()),
        ] {
            assert!(!crate::reconnect::retryable(&link_error(error)));
        }
    }
    #[test]
    fn cache_location_is_canonical_and_outside_tree() {
        let tmp = tempfile::tempdir().unwrap();
        let export = tmp.path().join("export");
        std::fs::create_dir(&export).unwrap();
        let root = Root::open(&export).unwrap();
        assert!(cache(&config(&export.join("state")), &root).is_err());
        assert!(cache(&config(tmp.path()), &root).is_err());
        let state = tmp.path().join("state");
        let c = cache(&config(&state), &root).unwrap().unwrap();
        assert_eq!(c.directory, state);
        assert!(!state.exists());
        let missing = Root::destination(&export.join("missing")).unwrap();
        assert!(cache(&config(&export.join("missing/state")), &missing).is_err());
    }
}
