//! Component benchmark: real storage and codec, no radio/Reticulum timing model.
//! Build: cargo build --offline --release --example cache_bench
//! Matrix: python3 tests/benchmark.py target/release/examples/cache_bench /tmp
//!         --output /tmp/cache-bench.jsonl
//! Report: python3 tests/benchmark_report.py /tmp/cache-bench.jsonl
//! RSS is process-wide; disk figures are checkpoints, not transient peaks.
use clap::Parser;
use rrsync::{
    Result,
    chunks::{Description, Limits, PAGE_CHUNKS, Store},
    protocol::{Message as Common, v2::Message},
};
use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::{Read, Write},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    time::Instant,
};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    directory: PathBuf,
    #[arg(long, default_value_t = 1_048_576)]
    size: u64,
    #[arg(long)]
    chunk_size: u32,
}
fn proc_value(file: &str, key: &str) -> u64 {
    fs::read_to_string(file)
        .unwrap()
        .lines()
        .find_map(|line| {
            let mut fields = line.split_whitespace();
            (fields.next() == Some(key)).then(|| fields.next().unwrap().parse().unwrap())
        })
        .unwrap()
}
fn disk(path: &Path) -> std::io::Result<(u64, u64)> {
    let mut logical = 0;
    let mut allocated = 0;
    for e in fs::read_dir(path)? {
        let e = e?;
        let meta = e.metadata()?;
        allocated += meta.blocks() * 512;
        if meta.is_dir() {
            let (l, a) = disk(&e.path())?;
            logical += l;
            allocated += a;
        } else {
            logical += meta.len();
        }
    }
    Ok((logical, allocated))
}
fn open_file_allocation(fixture: &Path) -> std::io::Result<u64> {
    let mut seen = BTreeSet::new();
    let mut bytes = 0;
    for fd in fs::read_dir("/proc/self/fd")? {
        let path = fd?.path();
        let Ok(target) = fs::read_link(&path) else {
            continue;
        };
        if !target.starts_with(fixture)
            && !target
                .as_os_str()
                .as_encoded_bytes()
                .ends_with(b" (deleted)")
        {
            continue;
        }
        let Ok(meta) = fs::metadata(&path) else {
            continue;
        };
        if meta.is_file() && seen.insert((meta.dev(), meta.ino())) {
            bytes += meta.blocks() * 512;
        }
    }
    Ok(bytes)
}
fn phase<T>(
    name: &str,
    cache: &Path,
    fixture: &Path,
    action: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let written = proc_value("/proc/self/io", "write_bytes:");
    let start = Instant::now();
    let result = action()?;
    let ms = start.elapsed().as_secs_f64() * 1000.;
    let written = proc_value("/proc/self/io", "write_bytes:").saturating_sub(written);
    let rss = proc_value("/proc/self/status", "VmRSS:");
    let peak = proc_value("/proc/self/status", "VmHWM:");
    let (logical, allocated) = disk(cache)?;
    let open = open_file_allocation(fixture)?;
    println!(
        "{{\"kind\":\"phase\",\"phase\":\"{name}\",\"elapsed_ms\":{ms},\"rss_kib\":{rss},\"process_peak_rss_kib\":{peak},\"write_bytes\":{written},\"cache_logical_bytes\":{logical},\"cache_allocated_bytes\":{allocated},\"open_file_allocated_bytes\":{open}}}"
    );
    std::io::stdout().flush()?;
    Ok(result)
}
fn controls(d: &Description, push: bool, cached: bool) -> Result<()> {
    let ok = Message::Common(Common::Ok).encode()?.len();
    let header = Message::Description {
        index: 0,
        description: d.clone(),
    }
    .encode()?
    .len();
    let mut bytes = header
        + if push {
            ok
        } else {
            Message::Describe {
                index: 0,
                chunk_size: d.chunk_size(),
            }
            .encode()?
            .len()
        };
    let mut requests = 1;
    for start in (0..d.chunks()).step_by(PAGE_CHUNKS) {
        let hashes = d.page(start)?;
        let count = hashes.len();
        bytes += Message::Hashes {
            index: 0,
            start: start as u32,
            hashes,
        }
        .encode()?
        .len();
        bytes += if push {
            Message::Missing {
                index: 0,
                start: start as u32,
                count: count as u32,
                chunks: if cached {
                    vec![]
                } else {
                    (start..start + count).map(|i| i as u32).collect()
                },
            }
            .encode()?
            .len()
        } else {
            Message::HashesGet {
                index: 0,
                start: start as u32,
            }
            .encode()?
            .len()
        };
        requests += 1;
        if !cached {
            for i in start..start + count {
                let chunk = i as u32;
                let a = if push {
                    Message::ChunkBegin { index: 0, chunk }
                } else {
                    Message::ChunkGet { index: 0, chunk }
                };
                let b = if push {
                    Message::ChunkCommit {
                        index: 0,
                        chunk,
                        resource: [0; 32],
                    }
                } else {
                    Message::ChunkVerified { index: 0, chunk }
                };
                bytes += a.encode()?.len() + b.encode()?.len() + 2 * ok;
                requests += 2;
            }
        }
    }
    bytes += if push {
        Message::FileCommit(0)
    } else {
        Message::FileVerified(0)
    }
    .encode()?
    .len()
        + ok;
    requests += 1;
    let direction = if push { "push" } else { "pull" };
    let payload = if cached { 0 } else { d.size() };
    println!(
        "{{\"kind\":\"control\",\"direction\":\"{direction}\",\"cached\":{cached},\"bytes\":{bytes},\"requests\":{requests},\"payload_bytes\":{payload}}}"
    );
    Ok(())
}
fn main() -> Result<()> {
    let args = Args::parse();
    Description::count(args.size, args.chunk_size)?;
    let fixture = tempfile::Builder::new()
        .prefix("rrsync-bench-")
        .tempdir_in(fs::canonicalize(&args.directory)?)?;
    let root = fixture.path();
    let cache = root.join("cache");
    fs::create_dir(&cache)?;
    let mut source = phase("source", &cache, root, || {
        let mut file = File::options()
            .create_new(true)
            .read(true)
            .write(true)
            .open(root.join("source"))?;
        let mut buffer = [0u8; 65536];
        let mut seed = 0x12345678u32;
        let mut left = args.size;
        while left > 0 {
            let n = left.min(buffer.len() as u64) as usize;
            for byte in &mut buffer[..n] {
                seed ^= seed << 13;
                seed ^= seed >> 17;
                seed ^= seed << 5;
                *byte = seed as u8;
            }
            file.write_all(&buffer[..n])?;
            left -= n as u64;
        }
        file.sync_all()?;
        Ok(file)
    })?;
    let description = phase("describe", &cache, root, || {
        Description::scan(&mut source, args.chunk_size)
    })?;
    let limits = Limits {
        max_bytes: args.size.max(1),
        max_transfers: 1,
        retention_seconds: 604800,
    };
    let receiver = Description::header(args.size, args.chunk_size, description.hash())?;
    let store = phase("open_cold", &cache, root, || {
        Store::open_limited(&cache, [1; 32], receiver.clone(), limits)
    })?;
    phase("receive", &cache, root, || {
        for start in (0..description.chunks()).step_by(PAGE_CHUNKS) {
            receiver.set_page(start, &description.page(start)?)?;
            let missing = store.missing(start)?;
            assert_eq!(missing.len(), description.page_len(start)?);
            for i in missing {
                store.receive(i, &mut (&mut source).take(description.length(i)?))?;
            }
        }
        Ok(())
    })?;
    let assembled = phase("assemble", &cache, root, || store.assemble())?;
    assert_eq!(assembled.metadata()?.len(), args.size);
    drop(assembled);
    phase("close_cold", &cache, root, || {
        drop(store);
        Ok(())
    })?;
    drop(receiver);
    let receiver = Description::header(args.size, args.chunk_size, description.hash())?;
    let store = phase("open_warm", &cache, root, || {
        Store::open_limited(&cache, [1; 32], receiver.clone(), limits)
    })?;
    phase("rehash", &cache, root, || {
        for start in (0..description.chunks()).step_by(PAGE_CHUNKS) {
            receiver.set_page(start, &description.page(start)?)?;
            assert!(store.missing(start)?.is_empty());
        }
        Ok(())
    })?;
    phase("clear", &cache, root, || store.clear())?;
    phase("close_warm", &cache, root, || {
        drop(store);
        Ok(())
    })?;
    for push in [true, false] {
        for cached in [false, true] {
            controls(&description, push, cached)?;
        }
    }
    Ok(())
}
