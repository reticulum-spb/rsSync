#!/usr/bin/env python3
"""Two reticulum-client application processes through an already running rnsd-rs.
Usage: python3 tests/reticulum_e2e.py [rrsync-binary] [reticulum-config-directory]
"""
import os
import pathlib
import subprocess
import sys
import tempfile
import time

binary = str(pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else 'target/debug/rrsync').resolve())
reticulum_config = str(pathlib.Path(sys.argv[2] if len(sys.argv) > 2 else '~/.rsReticulum').expanduser().resolve())

def wait_for(predicate, timeout=15):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        value = predicate()
        if value:
            return value
        time.sleep(.05)
    raise RuntimeError('timed out waiting for process readiness')

def contents(root):
    return {str(p.relative_to(root)): ('dir' if p.is_dir() else p.read_bytes(), p.stat().st_mtime_ns)
            for p in root.rglob('*') if not p.is_symlink()}

with tempfile.TemporaryDirectory(prefix='rrsync-e2e-') as temp:
    root = pathlib.Path(temp)
    rns = reticulum_config
    server_config = root/'server-config'
    client_config = root/'client-config'
    server_config.mkdir(); client_config.mkdir()
    (client_config/'config.yaml').write_text(f'reticulum_config: {rns}\ntimeout_seconds: 30\n')
    def cli(*args, expect=0):
        run = subprocess.run([binary, '--config', str(client_config), *map(str,args)], capture_output=True, text=True, timeout=150)
        if run.returncode != expect:
            raise AssertionError(f'{args}: exit {run.returncode}\n{run.stdout}\n{run.stderr}')
        return run.stdout
    client_id = cli('identity').strip().split()[-1]
    read_config = root/'read-config'; read_config.mkdir()
    (read_config/'config.yaml').write_text(f'reticulum_config: {rns}\ntimeout_seconds: 30\n')
    reader_id = subprocess.check_output([binary,'--config',str(read_config),'identity'],text=True).strip().split()[-1]
    (server_config/'config.yaml').write_text(f'reticulum_config: {rns}\npermits:\n  - others: deny\n  - "{client_id}": full\n  - "{reader_id}": read\ntimeout_seconds: 8\nannounce_seconds: 5\n')
    export = root/'export'; export.mkdir()
    source = root/'source'; source.mkdir()
    (source/'empty-dir').mkdir(); (source/'nested').mkdir()
    (source/'empty-file').write_bytes(b'')
    (source/'nested'/'данные.txt').write_bytes(b'hello reticulum')
    (source/'large').write_bytes(bytes(range(256))*4300)
    processes = []
    logs = []
    try:
        slog = open(root/'server.log','w+'); logs.append(slog)
        processes.append(subprocess.Popen([binary,'--config',str(server_config),'serve',str(export)],stdout=slog,stderr=slog))
        def destination():
            text=(root/'server.log').read_text()
            for line in text.splitlines():
                if line.startswith('Destination: '): return line.split()[-1]
        dest = wait_for(destination)
        remote = dest+':/backup'
        cli('push','--checksum',source,remote)
        assert contents(source)==contents(export/'backup'), 'initial push mismatch'
        repeat = cli('push','--checksum',source,remote)
        assert 'Create\t' not in repeat and 'Update\t' not in repeat, repeat
        (source/'nested'/'данные.txt').write_bytes(b'new content')
        output = cli('push',source,remote)
        assert output.count('Update\t')==1, output
        (export/'backup'/'extra').write_bytes(b'preserved until delete')
        before = contents(export)
        cli('push','--dry-run','--delete',source,remote)
        assert contents(export)==before, 'dry-run changed destination'
        cli('push','--delete',source,remote)
        assert contents(source)==contents(export/'backup'), 'delete mismatch'
        local = root/'download'/'nested'
        cli('pull','--checksum',remote,local)
        assert contents(source)==contents(local), 'pull mismatch'
        (local/'extra-local').write_bytes(b'local extra')
        local_before=contents(local)
        cli('pull','--dry-run','--delete',remote,local)
        assert contents(local)==local_before
        cli('pull','--delete',remote,local)
        assert contents(source)==contents(local), 'pull delete mismatch'

        cli('pull','--dry-run',remote,root/'never-created')
        assert not (root/'never-created').exists()
        # A killed sender must not replace its unfinished file or delete extras.
        interrupted = root/'interrupted'; interrupted.mkdir()
        (interrupted/'a-first').write_bytes(b'first committed')
        (interrupted/'z-large').write_bytes(bytes(range(256))*32768)
        interrupted_target = export/'interrupted'; interrupted_target.mkdir()
        (interrupted_target/'z-large').write_bytes(b'old file must survive')
        (interrupted_target/'extra').write_bytes(b'delete only on success')
        ilog = open(root/'interrupted.log','w+'); logs.append(ilog)
        transfer = subprocess.Popen([binary,'--config',str(client_config),'push','--delete',str(interrupted),dest+':/interrupted'],stdout=ilog,stderr=ilog)
        processes.append(transfer)
        wait_for(lambda: (interrupted_target/'a-first').exists(), 30)
        transfer.kill(); transfer.wait()
        assert (interrupted_target/'z-large').read_bytes()==b'old file must survive'
        assert (interrupted_target/'extra').exists()
        # The server's short test inactivity lease releases the abandoned session.
        time.sleep(10)
        recovered = cli('push','--delete',interrupted,dest+':/interrupted')
        assert 'Skip\ta-first' in recovered, recovered
        assert contents(interrupted)==contents(interrupted_target)
        # A read-only identity may pull, but cannot push, even in dry-run.
        read_target = root/'read-download'
        subprocess.run([binary,'--config',str(read_config),'pull',remote,str(read_target)],check=True,capture_output=True,text=True,timeout=150)
        assert contents(source)==contents(read_target)
        before_read_push=contents(export)
        for flags in [[],['--dry-run']]:
            denied_push=subprocess.run([binary,'--config',str(read_config),'push',*flags,str(source),dest+':/read-must-not-create'],capture_output=True,text=True,timeout=40)
            assert denied_push.returncode != 0 and 'permission denied' in denied_push.stderr, denied_push.stderr
        assert contents(export)==before_read_push
        # Source exclusions must protect existing destination data from --delete.
        os.symlink('/etc', source/'excluded')
        (export/'backup'/'excluded').mkdir()
        (export/'backup'/'excluded'/'keep').write_bytes(b'protected')
        cli('push','--delete',source,remote)
        assert (export/'backup'/'excluded'/'keep').read_bytes()==b'protected'
        # A separate, untrusted identity must not read or mutate the export.
        denied_config = root/'denied-config'; denied_config.mkdir()
        (denied_config/'config.yaml').write_text(f'reticulum_config: {rns}\ntimeout_seconds: 3\n')
        denied = subprocess.run([binary,'--config',str(denied_config),'pull',remote,str(root/'denied')],capture_output=True,text=True,timeout=15)
        assert denied.returncode != 0
        assert not (root/'denied').exists()
        print('PASS: push, pull, checksum, repeated sync, one-file update, delete, dry-run, empty files/directories, Unicode, segmented Resource, interruption/recovery, protected exclusions, read-only permissions, unauthorized identity',flush=True)
    except BaseException:
        for log in logs:
            log.flush(); log.seek(0); print(log.read()[-10000:],file=sys.stderr)
        raise
    finally:
        for p in reversed(processes):
            p.terminate()
            try: p.wait(timeout=5)
            except subprocess.TimeoutExpired: p.kill(); p.wait()
        for log in logs: log.close()
