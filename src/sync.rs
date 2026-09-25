use crate::{
    Error, Result,
    fs::{Root, Stamp, hash_file},
};
use std::{collections::BTreeMap, os::unix::fs::MetadataExt};
pub const MAX_ENTRIES: usize = 16384;
pub const MAX_FILE: u64 = 134_217_727;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    File,
    Directory,
    Protected,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub path: String,
    pub kind: Kind,
    pub size: u64,
    pub mtime: i64,
    pub ns: u32,
    pub hash: Option<[u8; 32]>,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Manifest {
    pub entries: Vec<Entry>,
}
#[derive(Debug)]
pub struct Scan {
    pub manifest: Manifest,
    pub stamps: BTreeMap<String, Stamp>,
}
impl Manifest {
    pub fn validate(&self, checksum: bool) -> Result<()> {
        if self.entries.len() > MAX_ENTRIES {
            return Err(Error::Protocol("manifest entry limit".into()));
        }
        let mut seen = BTreeMap::new();
        for e in &self.entries {
            crate::fs::validate(&e.path)?;
            if e.path.split('/').any(|part| part.starts_with(".rrsync-"))
                && e.kind != Kind::Protected
            {
                return Err(Error::InvalidPath(e.path.clone()));
            }
            if e.kind != Kind::File && (e.size != 0 || e.hash.is_some()) {
                return Err(Error::Protocol("invalid non-file metadata".into()));
            }
            if e.ns >= 1_000_000_000
                || (e.kind == Kind::File && (e.size > MAX_FILE || (checksum && e.hash.is_none())))
            {
                return Err(Error::Protocol(format!(
                    "invalid metadata or file too large: {}",
                    e.path
                )));
            }
            if let Some(previous) = seen.last_key_value()
                && previous.0 >= &e.path
            {
                return Err(Error::Protocol(
                    "manifest paths must be sorted and unique".into(),
                ));
            }
            if let Some((parent, _)) = e.path.rsplit_once('/')
                && seen.get(parent) != Some(&Kind::Directory)
            {
                return Err(Error::Protocol(
                    "missing or invalid parent directory".into(),
                ));
            }
            seen.insert(e.path.clone(), e.kind);
        }
        Ok(())
    }
}
pub fn scan(root: &Root, checksum: bool) -> Result<Scan> {
    let mut result = Scan {
        manifest: Manifest::default(),
        stamps: BTreeMap::new(),
    };
    if !root.exists("")? {
        return Ok(result);
    }
    walk(root, "", checksum, &mut result)?;
    result
        .stamps
        .insert(String::new(), Stamp::of(&root.metadata("")?));
    result.manifest.entries.sort_by(|a, b| a.path.cmp(&b.path));
    result.manifest.validate(checksum)?;
    Ok(result)
}
fn walk(root: &Root, path: &str, checksum: bool, result: &mut Scan) -> Result<()> {
    if path.split('/').count() > 128 {
        return Err(Error::Protocol("directory nesting exceeds 128".into()));
    }
    let before = Stamp::of(&root.metadata(path)?);
    for (name, meta) in root.children(path)? {
        let child = if path.is_empty() {
            name
        } else {
            format!("{path}/{name}")
        };
        crate::fs::validate(&child)?;
        let kind = if child.rsplit('/').next().unwrap().starts_with(".rrsync-") {
            Kind::Protected
        } else if meta.is_file() {
            Kind::File
        } else if meta.is_dir() {
            Kind::Directory
        } else {
            Kind::Protected
        };
        let stamp = Stamp::of(&meta);
        let hash = if kind == Kind::File && checksum {
            let mut file = root.open_file(&child)?;
            let hash = hash_file(&mut file)?;
            if Stamp::of(&file.metadata()?) != stamp {
                return Err(Error::Changed(child));
            }
            Some(hash)
        } else {
            None
        };
        if kind == Kind::Protected {
            tracing::warn!(path = %child, "skipping unsupported filesystem object");
        }
        result.manifest.entries.push(Entry {
            path: child.clone(),
            kind,
            size: if kind == Kind::File { meta.len() } else { 0 },
            mtime: meta.mtime(),
            ns: meta.mtime_nsec() as u32,
            hash,
        });
        result.stamps.insert(child.clone(), stamp);
        if result.manifest.entries.len() > MAX_ENTRIES {
            return Err(Error::Protocol("manifest entry limit".into()));
        }
        if kind == Kind::Directory {
            walk(root, &child, checksum, result)?;
        }
    }
    if Stamp::of(&root.metadata(path)?) != before {
        return Err(Error::Changed(path.into()));
    }
    Ok(())
}
#[derive(Clone, Copy, Debug, Default)]
pub struct Options {
    pub delete: bool,
    pub checksum: bool,
    pub dry_run: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    Skip,
    Mkdir,
    Create,
    Update,
    Delete,
}
#[derive(Clone, Debug)]
pub struct Operation {
    pub action: Action,
    pub path: String,
}
#[derive(Clone, Debug)]
pub struct Plan {
    pub operations: Vec<Operation>,
    pub files: Vec<usize>,
    pub directories: Vec<usize>,
    pub removals: Vec<Entry>,
    pub conflicts: BTreeMap<String, Vec<Entry>>,
}
fn below(path: &str, ancestor: &str) -> bool {
    path == ancestor
        || path
            .strip_prefix(ancestor)
            .is_some_and(|rest| rest.starts_with('/'))
}
pub fn plan(source: &Manifest, destination: &Manifest, options: Options) -> Result<Plan> {
    source.validate(options.checksum)?;
    destination.validate(options.checksum)?;
    let sm: BTreeMap<_, _> = source
        .entries
        .iter()
        .map(|e| (e.path.as_str(), e))
        .collect();
    let dm: BTreeMap<_, _> = destination
        .entries
        .iter()
        .map(|e| (e.path.as_str(), e))
        .collect();
    let protected: Vec<_> = source
        .entries
        .iter()
        .chain(destination.entries.iter())
        .filter(|e| e.kind == Kind::Protected)
        .map(|e| e.path.as_str())
        .collect();
    let mut p = Plan {
        operations: vec![],
        files: vec![],
        directories: vec![],
        removals: vec![],
        conflicts: BTreeMap::new(),
    };
    for (index, e) in source.entries.iter().enumerate() {
        if protected.iter().any(|s| below(&e.path, s)) {
            p.operations.push(Operation {
                action: Action::Skip,
                path: e.path.clone(),
            });
            continue;
        }
        let old = dm.get(e.path.as_str());
        if let Some(old) = old
            && old.kind != e.kind
        {
            if !options.delete {
                return Err(Error::Protocol(format!(
                    "type conflict at {}; requires --delete",
                    e.path
                )));
            }
            if protected.iter().any(|s| below(s, &e.path)) {
                return Err(Error::Protocol(format!(
                    "type conflict contains protected object: {}",
                    e.path
                )));
            }
            let mut conflicts: Vec<_> = destination
                .entries
                .iter()
                .filter(|x| below(&x.path, &e.path))
                .cloned()
                .collect();
            conflicts.reverse();
            p.conflicts.insert(e.path.clone(), conflicts);
        }
        let action = match e.kind {
            Kind::Directory => {
                p.directories.push(index);
                if old.is_some_and(|x| x.kind == Kind::Directory) {
                    Action::Skip
                } else {
                    Action::Mkdir
                }
            }
            Kind::File => {
                if old.is_some_and(|x| {
                    x.kind == Kind::File
                        && x.size == e.size
                        && x.mtime == e.mtime
                        && x.ns == e.ns
                        && (!options.checksum || x.hash == e.hash)
                }) {
                    Action::Skip
                } else {
                    p.files.push(index);
                    if old.is_some() {
                        Action::Update
                    } else {
                        Action::Create
                    }
                }
            }
            Kind::Protected => Action::Skip,
        };
        p.operations.push(Operation {
            action,
            path: e.path.clone(),
        });
    }
    if options.delete {
        for e in destination.entries.iter().rev() {
            if sm.contains_key(e.path.as_str())
                || protected
                    .iter()
                    .any(|s| below(&e.path, s) || below(s, &e.path))
                || p.conflicts.keys().any(|s| below(&e.path, s))
            {
                continue;
            }
            p.removals.push(e.clone());
            p.operations.push(Operation {
                action: Action::Delete,
                path: e.path.clone(),
            });
        }
    }
    Ok(p)
}
pub fn ensure_unchanged(root: &Root, path: &str, stamp: &Stamp) -> Result<()> {
    if Stamp::of(&root.metadata(path)?) != *stamp {
        return Err(Error::Changed(path.into()));
    }
    Ok(())
}
pub fn prepare(root: &Root, source: &Manifest, plan: &Plan) -> Result<()> {
    root.mkdir("")?;
    for &index in &plan.directories {
        let e = &source.entries[index];
        if let Some(entries) = plan.conflicts.get(&e.path) {
            for old in entries {
                check_entry(root, old)?;
                root.remove(&old.path, old.kind == Kind::Directory)?;
            }
        }
        root.mkdir(&e.path)?;
    }
    Ok(())
}
pub fn check_entry(root: &Root, entry: &Entry) -> Result<()> {
    let m = root.metadata(&entry.path)?;
    let valid = match entry.kind {
        Kind::File => {
            m.is_file()
                && m.len() == entry.size
                && m.mtime() == entry.mtime
                && m.mtime_nsec() == entry.ns as i64
        }
        Kind::Directory => m.is_dir(),
        Kind::Protected => false,
    };
    if !valid {
        return Err(Error::Changed(entry.path.clone()));
    }
    Ok(())
}
pub fn install(
    root: &Root,
    source: &Entry,
    destination: &Manifest,
    plan: &Plan,
    file: &mut std::fs::File,
) -> Result<()> {
    let staged = root.stage(
        &source.path,
        file,
        source.size,
        source.hash,
        source.mtime,
        source.ns,
    )?;
    if let Some(old) = destination.entries.iter().find(|e| e.path == source.path) {
        check_entry(root, old)?;
    } else if root.exists(&source.path)? {
        return Err(Error::Changed(source.path.clone()));
    }
    if let Some(entries) = plan.conflicts.get(&source.path) {
        for old in entries {
            check_entry(root, old)?;
            root.remove(&old.path, old.kind == Kind::Directory)?;
        }
    }
    staged.commit()
}
pub fn finish(root: &Root, source: &Manifest, plan: &Plan) -> Result<()> {
    // Preflight all removals before the first unlink. Never follow symlinks.
    for old in &plan.removals {
        check_entry(root, old)?;
    }
    for old in &plan.removals {
        root.remove(&old.path, old.kind == Kind::Directory)?;
    }
    for &index in plan.directories.iter().rev() {
        let e = &source.entries[index];
        root.set_mtime(&e.path, e.mtime, e.ns)?;
    }
    Ok(())
}
