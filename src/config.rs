use crate::{Error, Result};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

pub const MINIMAL_CONFIG: &str = "permits:\n  - others: deny\n";
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResumeConfig {
    pub chunk_size: u32,
    #[serde(default = "cache_directory")]
    pub directory: PathBuf,
    #[serde(default = "cache_bytes")]
    pub max_bytes: u64,
    #[serde(default = "cache_transfers")]
    pub max_transfers: usize,
}
fn cache_directory() -> PathBuf {
    "transfers".into()
}
fn cache_bytes() -> u64 {
    512 * 1024 * 1024
}
fn cache_transfers() -> usize {
    128
}
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Permission {
    Full,
    Read,
    Deny,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Each list item contains exactly one address and its permission.
    pub permits: Vec<BTreeMap<String, Permission>>,
    #[serde(skip)]
    pub identity: PathBuf,
    pub reticulum_config: Option<String>,
    pub timeout_seconds: u64,
    pub announce_seconds: u64,
    pub protocol: u8,
    pub resume: Option<ResumeConfig>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            permits: vec![],
            identity: "identity".into(),
            reticulum_config: None,
            timeout_seconds: 600,
            announce_seconds: 600,
            protocol: 1,
            resume: None,
        }
    }
}
impl Config {
    pub fn default_directory() -> Result<PathBuf> {
        std::env::var_os("HOME")
            .filter(|s| !s.is_empty())
            .map(|s| PathBuf::from(s).join(".rsSync"))
            .ok_or_else(|| Error::Config("HOME is unset; specify --config DIRECTORY".into()))
    }
    /// Only the implicit default directory is bootstrapped. Explicit directories
    /// must contain config.yaml, so a typo cannot silently create a new identity.
    pub fn load_directory(directory: &Path, initialize: bool) -> Result<Self> {
        if initialize {
            std::fs::create_dir_all(directory)?;
            let path = directory.join("config.yaml");
            if !path.try_exists()? {
                use std::io::Write;
                let mut file = tempfile::NamedTempFile::new_in(directory)?;
                file.write_all(MINIMAL_CONFIG.as_bytes())?;
                file.as_file().sync_all()?;
                match file.persist_noclobber(&path) {
                    Ok(_) => {}
                    Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => {}
                    Err(e) => return Err(e.error.into()),
                }
            }
        }
        let text = std::fs::read_to_string(directory.join("config.yaml"))?;
        let mut config: Self =
            serde_saphyr::from_str(&text).map_err(|e| Error::Config(e.to_string()))?;
        for rule in &config.permits {
            if rule.len() != 1 {
                return Err(Error::Config(
                    "each permits item must contain exactly one address".into(),
                ));
            }
            let key = rule.keys().next().unwrap();
            if key != "others" {
                address(key)?;
            }
        }
        if config.timeout_seconds == 0 || config.announce_seconds == 0 {
            return Err(Error::Config("timeouts must be positive".into()));
        }
        config.identity = directory.join("identity");
        if !matches!(config.protocol, 1 | 2) || (config.protocol == 2 && config.resume.is_none()) {
            return Err(Error::Config(
                "protocol must be 1 or 2; protocol 2 requires resume configuration".into(),
            ));
        }
        if let Some(resume) = &mut config.resume {
            crate::chunks::Description::count(0, resume.chunk_size)
                .map_err(|e| Error::Config(e.to_string()))?;
            if resume.max_bytes == 0
                || resume.max_transfers == 0
                || resume.max_transfers >= crate::sync::MAX_ENTRIES
            {
                return Err(Error::Config("invalid resume cache limits".into()));
            }
            if resume.directory.is_relative() {
                resume.directory = directory.join(&resume.directory);
            }
        }
        Ok(config)
    }
    pub fn permission(&self, id: &[u8; 16]) -> Permission {
        let mut fallback = None;
        for rule in &self.permits {
            for (key, value) in rule {
                if key == "others" {
                    fallback.get_or_insert(*value);
                } else if address(key).ok().as_ref() == Some(id) {
                    return *value;
                }
            }
        }
        fallback.unwrap_or(Permission::Deny)
    }
    pub fn permits(&self, id: &[u8; 16]) -> bool {
        self.permission(id) != Permission::Deny
    }
}
pub fn address(s: &str) -> Result<[u8; 16]> {
    hex::decode(s)
        .ok()
        .and_then(|b| b.try_into().ok())
        .ok_or_else(|| Error::Config("expected 32 hexadecimal characters".into()))
}
#[cfg(test)]
mod tests {
    use super::*;
    fn rule(key: String, value: Permission) -> BTreeMap<String, Permission> {
        BTreeMap::from([(key, value)])
    }
    #[test]
    fn ordered_permissions_and_fallback() {
        let id = [7; 16];
        let mut c = Config::default();
        assert_eq!(c.permission(&id), Permission::Deny);
        c.permits = vec![
            rule("others".into(), Permission::Full),
            rule(hex::encode(id), Permission::Read),
            rule(hex::encode(id), Permission::Deny),
        ];
        assert_eq!(c.permission(&id), Permission::Read);
        assert_eq!(c.permission(&[8; 16]), Permission::Full);
        c.permits
            .insert(0, rule(hex::encode_upper(id), Permission::Deny));
        assert!(!c.permits(&id));
        c.permits = vec![
            rule("others".into(), Permission::Read),
            rule("others".into(), Permission::Full),
        ];
        assert_eq!(c.permission(&id), Permission::Read);
    }
    #[test]
    fn bootstrap_and_preserve_configuration() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("new");
        assert!(Config::load_directory(&dir, false).is_err());
        assert!(!dir.exists());
        let c = Config::load_directory(&dir, true).unwrap();
        assert_eq!(c.identity, dir.join("identity"));
        let first = crate::transport::identity(&c, false).unwrap();
        assert!(dir.join("identity").is_file());
        assert_eq!(
            crate::transport::identity(&c, false).unwrap().hash,
            first.hash
        );
        std::fs::write(dir.join("config.yaml"), "permits:\n  - others: read\n").unwrap();
        assert_eq!(
            Config::load_directory(&dir, true)
                .unwrap()
                .permission(&[0; 16]),
            Permission::Read
        );
    }
    #[test]
    fn resume_config_validates_explicit_version_geometry_and_limits() {
        let tmp = tempfile::tempdir().unwrap();
        for text in [
            "protocol: 3",
            "protocol: 2",
            "resume: {}",
            "resume: {chunk_size: 1}",
            "resume: {chunk_size: 4096, max_bytes: 0}",
            "resume: {chunk_size: 4096, max_transfers: 0}",
            "resume: {chunk_size: 4096, max_transfers: 16384}",
        ] {
            std::fs::write(tmp.path().join("config.yaml"), text).unwrap();
            assert!(Config::load_directory(tmp.path(), false).is_err(), "{text}");
        }
        std::fs::write(
            tmp.path().join("config.yaml"),
            "protocol: 2\nresume: {chunk_size: 262144}",
        )
        .unwrap();
        let config = Config::load_directory(tmp.path(), false).unwrap();
        let resume = config.resume.unwrap();
        assert_eq!(resume.directory, tmp.path().join("transfers"));
        assert_eq!(resume.max_bytes, 512 * 1024 * 1024);
        assert!(!resume.directory.exists());
    }
    #[test]
    fn invalid_permissions_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        for text in [
            "allow: all",
            "permits: {others: full}",
            "permits:\n - others: write",
            "permits:\n - bad: read",
            "permits:\n - {}",
            "permits:\n - others: full\n   bad: deny",
        ] {
            std::fs::write(tmp.path().join("config.yaml"), text).unwrap();
            assert!(Config::load_directory(tmp.path(), false).is_err(), "{text}");
        }
    }
}
