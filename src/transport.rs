use crate::{
    Error, Result,
    config::{Config, Permission},
    engine::{self, SyncTransport},
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
    incoming: mpsc::Receiver<FileResourceCompletion>,
    identities: Arc<Mutex<HashMap<[u8; 16], [u8; 16]>>>,
    config: Config,
}
impl Service {
    fn receive(&mut self) {
        while let Ok(file) = self.incoming.try_recv() {
            self.engine.receive(
                file.link_id,
                engine::ReceivedFile {
                    transfer_id: file.resource_hash,
                    file: file.file,
                    size: file.data_size as u64,
                    has_metadata: file.metadata.is_some(),
                },
            );
        }
    }
    fn request(&mut self, link: [u8; 16], path: [u8; 16], data: Vec<u8>) -> Result<RequestOutcome> {
        if path != rns_crypto::sha::truncated_hash(REQUEST_PATH.as_bytes()) {
            return Err(Error::Protocol("unknown request path".into()));
        }
        let permission = self
            .identities
            .lock()
            .unwrap()
            .get(&link)
            .map(|id| self.config.permission(id))
            .unwrap_or(Permission::Deny);
        self.receive();
        match self
            .engine
            .handle(link, permission, Message::decode(&data)?)?
        {
            engine::ServerReply::Message(message) => Ok(RequestOutcome::Reply(message.encode()?)),
            engine::ServerReply::File { message, file } => Ok(RequestOutcome::ReplyWithFile {
                ack: message.encode()?,
                file: Arc::new(file),
                metadata: None,
                auto_compress: false,
            }),
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
                state.engine.disconnect(link);
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
                    state.engine.expire(Instant::now(), Duration::from_secs(config.timeout_seconds))
                };
                if let Some(link_id)=expired {
                    event_tx.send(rns_transport::link_messages::DestinationEvent::LinkClosed {link_id}).await.map_err(transport)?;
                    manager.try_step();
                }
            },
            Some(link)=closed_rx.recv()=>{ let mut s=service.lock().unwrap(); s.engine.disconnect(link); },
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
    state.engine.touch(header.destination_hash, Instant::now());
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
        return state.engine.accepts_file(
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
        engine::synchronize(
            &mut ReticulumTransport(&mut link),
            &local_root,
            scan,
            engine::SyncRequest {
                push,
                path,
                options,
                deadline,
            },
            |plan| {
                for op in &plan.operations {
                    println!("{:?}\t{}", op.action, op.path);
                }
            },
        )
        .await
    }
    .await;
    shutdown.trigger();
    result
}

struct ReticulumTransport<'a>(&'a mut LinkSession);
impl SyncTransport for ReticulumTransport<'_> {
    async fn request(&mut self, payload: Vec<u8>, deadline: Duration) -> Result<Vec<u8>> {
        let response = self
            .0
            .request_with_metadata_limit(REQUEST_PATH, Some(&payload), deadline, MAX_CONTROL + 1024)
            .await
            .map_err(transport)?;
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
            .map_err(transport)
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
            .map_err(transport)?;
        Ok(engine::ReceivedFile {
            transfer_id: received.resource_hash,
            file: received.file.into_std().await,
            size: received.data_size as u64,
            has_metadata: received.metadata.is_some(),
        })
    }
    async fn close(&mut self) -> Result<()> {
        self.0.close().await.map_err(transport)
    }
}
