#![cfg(target_os = "linux")]
pub mod chunks;
pub mod config;
pub mod engine;
pub mod fs;
pub mod protocol;
pub mod reconnect;
pub mod sync;
pub mod transport;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("filesystem: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid path: {0}")]
    InvalidPath(String),
    #[error("protocol: {0}")]
    Protocol(String),
    #[error("permission denied")]
    PermissionDenied,
    #[error("source or destination changed: {0}; run synchronization again")]
    Changed(String),
    #[error("hash mismatch: {0}")]
    HashMismatch(String),
    #[error("transport: {0}")]
    Transport(String),
    #[error("connection: {0}")]
    Connection(String),
    #[error("server export is busy")]
    Busy,
    #[error("remote error {code}: {text}")]
    Remote { code: u8, text: String },
    #[error("configuration: {0}")]
    Config(String),
}
pub type Result<T> = std::result::Result<T, Error>;
impl From<rustix::io::Errno> for Error {
    fn from(e: rustix::io::Errno) -> Self {
        Self::Io(e.into())
    }
}
