//! Application-level transport simulator. It delivers encoded control messages
//! and file streams, not Reticulum packets; packet retransmission remains native.
use rrsync::{
    Error, Result,
    config::Permission,
    engine::{ConnectionId, ReceivedFile, Server, ServerReply, SyncTransport, TransferId},
    fs::Root,
    protocol::Message,
};
use std::{
    fs::File,
    io::{Read, Seek, Write},
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    DropRequest(u8),
    DropResponse(u8),
    DuplicateRequest(u8),
    FinishBeforeCommit,
    DelayResponse(u8, Duration),
    InterruptUpload(usize),
    InterruptDownload(usize),
    CorruptUpload(usize),
    RestartServerAfterUpload(usize),
}
#[derive(Default)]
pub struct Stats {
    pub requests: Vec<u8>,
    pub uploads: usize,
    pub downloads: usize,
    pub upload_bytes: u64,
    pub download_bytes: u64,
    pub closed: bool,
}
pub struct SimulatedTransport {
    pub server: Arc<Mutex<Server>>,
    pub connection: ConnectionId,
    pub permission: Permission,
    pub stats: Stats,
    pub fault: Option<Fault>,
    pub close_is_lost: bool,
    root: Root,
    download: Option<File>,
}
impl SimulatedTransport {
    pub fn new(root: Root, server: Arc<Mutex<Server>>, connection: ConnectionId) -> Self {
        Self {
            server,
            connection,
            permission: Permission::Full,
            stats: Stats::default(),
            fault: None,
            close_is_lost: false,
            root,
            download: None,
        }
    }
    fn take_fault(&mut self, fault: Fault) -> bool {
        if self.fault == Some(fault) {
            self.fault = None;
            true
        } else {
            false
        }
    }
    fn dispatch(&mut self, payload: &[u8]) -> Result<Vec<u8>> {
        let mut server = self.server.lock().unwrap();
        let outcome = Message::decode(payload)
            .and_then(|message| server.handle(self.connection, self.permission, message));
        let response = match outcome {
            Ok(ServerReply::Message(message)) => message.encode(),
            Ok(ServerReply::File { message, file }) => {
                self.download = Some(file);
                message.encode()
            }
            Err(error) => {
                server.disconnect(self.connection);
                Message::error(&error).encode()
            }
        };
        if response.is_err() {
            server.disconnect(self.connection);
        }
        response
    }
    async fn request_inner(&mut self, payload: Vec<u8>, deadline: Duration) -> Result<Vec<u8>> {
        let tag = payload[1];
        self.stats.requests.push(tag);
        if self.take_fault(Fault::DropRequest(tag)) {
            tokio::time::sleep(deadline + Duration::from_secs(1)).await;
            unreachable!("request deadline must fire")
        }
        if tag == 4 && self.take_fault(Fault::FinishBeforeCommit) {
            return self.dispatch(&Message::Finish.encode()?);
        }
        let response = self.dispatch(&payload)?;
        if self.take_fault(Fault::DropResponse(tag)) {
            tokio::time::sleep(deadline + Duration::from_secs(1)).await;
            unreachable!("response deadline must fire")
        }
        if let Some(Fault::DelayResponse(target, delay)) = self.fault
            && target == tag
        {
            self.fault = None;
            tokio::time::sleep(delay).await;
        }
        if self.take_fault(Fault::DuplicateRequest(tag)) {
            return self.dispatch(&payload);
        }
        Ok(response)
    }
}
fn lost() -> Error {
    Error::Transport("simulated connection interrupted".into())
}
impl SyncTransport for SimulatedTransport {
    async fn request(&mut self, payload: Vec<u8>, deadline: Duration) -> Result<Vec<u8>> {
        tokio::time::timeout(deadline, self.request_inner(payload, deadline))
            .await
            .map_err(|_| Error::Transport("simulated request timeout".into()))?
    }
    async fn send_file(
        &mut self,
        mut source: File,
        size: u64,
        _deadline: Duration,
    ) -> Result<TransferId> {
        self.stats.uploads += 1;
        let index = self.stats.uploads;
        let mut file = tempfile::tempfile()?;
        let interrupt = self.take_fault(Fault::InterruptUpload(index));
        let count = std::io::copy(
            &mut (&mut source).take(if interrupt { size / 2 } else { size }),
            &mut file,
        )?;
        self.stats.upload_bytes += count;
        if interrupt {
            return Err(lost());
        }
        if count != size {
            return Err(lost());
        }
        if self.take_fault(Fault::CorruptUpload(index)) && size != 0 {
            file.rewind()?;
            let mut byte = [0];
            file.read_exact(&mut byte)?;
            byte[0] ^= 0xff;
            file.rewind()?;
            file.write_all(&byte)?;
        }
        file.rewind()?;
        let mut transfer_id = [0; 32];
        transfer_id[..8].copy_from_slice(&(index as u64).to_be_bytes());
        self.server.lock().unwrap().receive(
            self.connection,
            ReceivedFile {
                transfer_id,
                file,
                size,
                has_metadata: false,
            },
        );
        if self.take_fault(Fault::RestartServerAfterUpload(index)) {
            *self.server.lock().unwrap() = Server::new(self.root.clone());
            return Err(lost());
        }
        Ok(transfer_id)
    }
    async fn receive_file(&mut self, max_size: u64, _deadline: Duration) -> Result<ReceivedFile> {
        self.stats.downloads += 1;
        let mut source = self
            .download
            .take()
            .ok_or_else(|| Error::Protocol("no simulated file pending".into()))?;
        let size = source.metadata()?.len();
        if size > max_size {
            return Err(Error::Protocol("simulated file exceeds bound".into()));
        }
        let mut file = tempfile::tempfile()?;
        let interrupt = self.take_fault(Fault::InterruptDownload(self.stats.downloads));
        let count = std::io::copy(
            &mut (&mut source).take(if interrupt { size / 2 } else { size }),
            &mut file,
        )?;
        self.stats.download_bytes += count;
        if interrupt || count != size {
            return Err(lost());
        }
        file.rewind()?;
        Ok(ReceivedFile {
            transfer_id: [0; 32],
            file,
            size,
            has_metadata: false,
        })
    }
    async fn close(&mut self) -> Result<()> {
        self.stats.closed = true;
        self.download = None;
        if self.close_is_lost {
            return Err(lost());
        }
        self.server.lock().unwrap().disconnect(self.connection);
        Ok(())
    }
}
