#!/usr/bin/env python3
"""Benchmark two local clients of an existing rnsd-rs; never manage the daemon."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import tempfile
import time


def digest(path):
    value = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(65536), b''):
            value.update(block)
    return value.hexdigest()


def snapshot(directory):
    result = {}
    for path in directory.rglob('*'):
        info = path.stat()
        result[str(path.relative_to(directory))] = (
            ('file', info.st_size, info.st_mtime_ns, digest(path)) if path.is_file()
            else ('directory', info.st_mtime_ns))
    return result


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def rss(pid, field="VmRSS"):
    try:
        for line in Path(f'/proc/{pid}/status').read_text().splitlines():
            if line.startswith(field + ':'):
                return int(line.split()[1])
    except FileNotFoundError:
        pass
    return 0


def allocated(root, processes, misses):
    # Deduplicate named files and open fds. Include anonymous regular temporary
    # files owned by these children, even when TMPDIR is outside the fixture.
    seen = set()
    total = 0
    paths = [p for p in root.rglob('*') if p.suffix != '.log']
    for process in processes:
        try:
            paths.extend(Path(f'/proc/{process.pid}/fd').iterdir())
        except FileNotFoundError:
            pass
        except PermissionError:
            # Linux can revoke proc fd access while a process execs/exits.
            misses['proc_fd'] += 1
    for path in paths:
        try:
            if not path.is_file():
                continue
            if '/proc/' in str(path):
                target = os.readlink(path)
                if target.endswith('.log') or not (target.startswith(str(root) + '/') or
                                                   target.endswith(' (deleted)')):
                    continue
            info = path.stat()
            key = (info.st_dev, info.st_ino)
            if key not in seen:
                seen.add(key)
                total += info.st_blocks * 512
        except FileNotFoundError:
            pass
        except PermissionError:
            if not str(path).startswith('/proc/'):
                raise
            misses['proc_fd'] += 1
    return total


def run_case(a, binary, rns, chunk, repeat, output, metadata):
    with tempfile.TemporaryDirectory(prefix='rrsync-native-bench-', dir=a.directory) as temp:
        root = Path(temp)
        source, export, download = (root / name for name in ('source', 'export', 'download'))
        for directory in (source, export, download):
            directory.mkdir()
        # Deterministic, bounded generation; no highly compressible repeating block.
        for index in range(a.files):
            parent = source if not a.files_per_directory else source / f'd{index // a.files_per_directory:06}'
            parent.mkdir(exist_ok=True)
            with (parent / f'{index:06}.bin').open('wb') as stream:
                remaining = a.size
                counter = 0
                while remaining:
                    block = hashlib.shake_256(f'{index}:{counter}'.encode()).digest(min(65536, remaining))
                    stream.write(block)
                    remaining -= len(block)
                    counter += 1
                stream.flush()
                if a.scenario == 'full':
                    os.fsync(stream.fileno())
        if a.scenario == 'unchanged':
            shutil.copytree(source, export / 'backup')
            shutil.copytree(source, download, dirs_exist_ok=True)
        expected = snapshot(source)
        client_config, server_config = root / 'client', root / 'server'
        client_config.mkdir(); server_config.mkdir()
        common = (f'reticulum_config: {json.dumps(str(rns))}\ntimeout_seconds: 60\n'
                  f'protocol: 2\nresume:\n  chunk_size: {chunk}\n'
                  f'  max_transfers: {max(128, a.files)}\n')
        (client_config / 'config.yaml').write_text(common)
        identity = subprocess.check_output([str(binary), '--config', str(client_config), 'identity'],
                                           text=True, timeout=30).split()[-1]
        (server_config / 'config.yaml').write_text(
            common + f'announce_seconds: 30\npermits:\n  - "{identity}": full\n  - others: deny\n')
        env = dict(os.environ, RUST_LOG='rrsync=info,rrsync::transport=debug,rns_runtime=warn,rns_transport=warn')
        with (root / 'server.log').open('w') as server_log:
            server = subprocess.Popen([str(binary), '--config', str(server_config), 'serve', str(export)],
                                      stdout=server_log, stderr=server_log, env=env)
            try:
                deadline = time.monotonic() + 30
                while True:
                    log = (root / 'server.log').read_text()
                    match = re.search(r'^Destination: ([0-9a-f]{32})$', log, re.M)
                    if match:
                        remote = match[1] + ':/backup'
                        break
                    if server.poll() is not None or time.monotonic() > deadline:
                        raise RuntimeError('server not ready: ' + log[-4000:])
                    time.sleep(.05)
                phases = ([('push', True), ('pull', True)] if a.scenario == 'unchanged' else
                          [('push', False), ('push', True), ('pull', False), ('pull', True)])
                for direction, warm in phases:
                    phase = direction + ('_unchanged' if warm else '_cold')
                    arguments = [str(source), remote] if direction == 'push' else [remote, str(download)]
                    log_path = root / (phase + '.log')
                    misses = dict(proc_fd=0)
                    baseline = allocated(root, [server], misses)
                    peaks = dict(client_rss_kib=0, server_rss_kib=0, allocated_bytes=baseline)
                    samples = 0
                    sampling_seconds = 0.0
                    started = time.monotonic()
                    with log_path.open('w') as log_file:
                        client = subprocess.Popen([str(binary), '--config', str(client_config), direction,
                                                   '--checksum', *arguments], stdout=log_file, stderr=log_file, env=env)
                        try:
                            while client.poll() is None:
                                if server.poll() is not None:
                                    raise RuntimeError('server exited during benchmark')
                                if time.monotonic() - started > a.timeout:
                                    raise RuntimeError('client timed out')
                                sample_started = time.monotonic()
                                peaks['client_rss_kib'] = max(peaks['client_rss_kib'], rss(client.pid))
                                peaks['server_rss_kib'] = max(peaks['server_rss_kib'], rss(server.pid))
                                if a.disk_sampling == 'periodic':
                                    peaks['allocated_bytes'] = max(peaks['allocated_bytes'], allocated(root, [client, server], misses))
                                sampling_seconds += time.monotonic() - sample_started
                                samples += 1
                                time.sleep(a.sample_interval)
                            elapsed = time.monotonic() - started
                        finally:
                            stop(client)
                    server_peak = rss(server.pid, 'VmHWM')
                    if a.disk_sampling == 'endpoints':
                        peaks['allocated_bytes'] = max(peaks['allocated_bytes'], allocated(root, [server], misses))
                    log = re.sub(r'\x1b\[[0-9;]*m', '', log_path.read_text())
                    if client.returncode != 0:
                        raise RuntimeError(f'{phase} failed: {log[-4000:]}')
                    if snapshot(export / 'backup' if direction == 'push' else download) != expected:
                        raise RuntimeError(f'{phase}: content/mtime mismatch')
                    planned = len(re.findall(r'^(?:Create|Update)\t', log, re.M))
                    if planned != (0 if warm else a.files):
                        raise RuntimeError(f'{phase}: unexpected transfer plan: {log[-4000:]}')
                    counters = re.findall(r'client local interface counters before shutdown.*?rx_bytes=(\d+) tx_bytes=(\d+)', log)
                    if len(counters) != 1:
                        raise RuntimeError(f'expected exactly one local interface counter record: {log[-4000:]}')
                    rx, tx = map(int, counters[0])
                    memory = re.findall(r'client peak resident memory.*?peak_rss_kib=(\d+)', log)
                    if len(memory) != 1:
                        raise RuntimeError('expected one client kernel RSS peak diagnostic')
                    process_peak = dict(client=int(memory[0]), server=server_peak)
                    record = dict(metadata, repeat=repeat, chunk_size=chunk, phase=phase,
                                  elapsed_seconds=elapsed, samples=samples, sampled_peak=peaks,
                                  sampling_misses=misses, sampling_seconds=sampling_seconds,
                                  process_peak_rss_kib=process_peak,
                                  allocated_baseline_bytes=baseline, client_local_rx_bytes=rx,
                                  client_local_tx_bytes=tx, planned_files=planned)
                    output.write(json.dumps(record) + '\n'); output.flush()
                    print(f'{phase}: {elapsed:.3f}s, local rx/tx {rx}/{tx}', flush=True)
            finally:
                stop(server)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('binary', type=Path)
    p.add_argument('reticulum_config', type=Path)
    p.add_argument('--directory', type=Path, default=Path('/tmp'))
    p.add_argument('--size', type=int, default=1048576, help='bytes per source file')
    p.add_argument('--files', type=int, default=1)
    p.add_argument('--files-per-directory', type=int, default=0, help='0 for a flat tree; otherwise group files in subdirectories')
    p.add_argument('--scenario', choices=['full', 'unchanged'], default='full',
                   help='full transfer/recheck, or compare preseeded identical trees')
    p.add_argument('--chunks', type=int, nargs='+', default=[4096, 65536, 1048576])
    p.add_argument('--repeats', type=int, default=3)
    p.add_argument('--sample-interval', type=float, default=.05)
    p.add_argument('--disk-sampling', choices=['periodic', 'endpoints'], default='periodic',
                   help='endpoints avoids repeated tree walks but misses transient disk use')
    p.add_argument('--timeout', type=float, default=300)
    p.add_argument('--output', type=Path, required=True)
    a = p.parse_args()
    if not (0 <= a.size <= 134217727 and 1 <= a.files <= 16383 and a.repeats > 0 and a.files_per_directory >= 0 and
            .01 <= a.sample_interval <= 10 and 0 < a.timeout <= 86400 and
            all(4096 <= c <= 16777216 for c in a.chunks)):
        p.error('invalid size, files, repeats, chunk size, sampling interval or timeout')
    directories = (a.files + a.files_per_directory - 1) // a.files_per_directory if a.files_per_directory else 0
    if a.files + directories > 16384:
        p.error('files plus subdirectories exceed the 16384-entry manifest limit')
    binary = a.binary.resolve(strict=True)
    rns = a.reticulum_config.expanduser().resolve(strict=True)
    a.directory = a.directory.resolve(strict=True)
    metadata = dict(binary=str(binary), binary_sha256=digest(binary), platform=platform.platform(),
                    size=a.size, files=a.files, directory=str(a.directory),
                    filesystem=subprocess.check_output(['findmnt', '-T', str(a.directory), '-n', '-o', 'FSTYPE'], text=True).strip(),
                    tmpdir=os.environ.get('TMPDIR', '/tmp'), sample_interval=a.sample_interval,
                    protocol=2, checksum=True, scenario=a.scenario,
                    files_per_directory=a.files_per_directory, manifest_entries=a.files + directories,
                    disk_sampling=a.disk_sampling)
    with a.output.open('x') as output:
        for repeat in range(a.repeats):
            for chunk in a.chunks:
                print(f'Run {repeat + 1}/{a.repeats}, chunk {chunk}', flush=True)
                run_case(a, binary, rns, chunk, repeat, output, metadata)


if __name__ == '__main__':
    main()
