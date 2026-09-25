#!/usr/bin/env python3
"""Run isolated cache/codec benchmarks; save JSONL, never alter existing files."""
import argparse
import hashlib
import json
import os
import platform
import subprocess
from pathlib import Path

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('binary', type=Path)
p.add_argument('directory', type=Path, help='existing parent for temporary source/cache fixtures')
p.add_argument('--size', type=int, default=1048576)
p.add_argument('--chunks', type=int, nargs='+', default=[4096, 16384, 65536, 262144, 1048576])
p.add_argument('--repeats', type=int, default=3)
p.add_argument('--output', type=Path, required=True, help='new JSONL file; existing files are refused')
a = p.parse_args()
if a.repeats < 1:
    p.error('--repeats must be positive')
binary = a.binary.resolve(strict=True)
directory = a.directory.resolve(strict=True)
with binary.open('rb') as executable:
    digest = hashlib.sha256()
    for block in iter(lambda: executable.read(1048576), b''):
        digest.update(block)
binary_sha256 = digest.hexdigest()
filesystem = subprocess.check_output(['findmnt', '-T', str(directory), '-n', '-o', 'FSTYPE'], text=True).strip()
with a.output.open('x') as output:
    # Round-robin cases help expose time/order variance; each child has its own HWM.
    for repeat in range(a.repeats):
        for chunk in a.chunks:
            print(f'Run {repeat + 1}/{a.repeats}: {a.size} bytes, chunk {chunk}, {filesystem}', flush=True)
            metadata = dict(size=a.size, chunk_size=chunk, repeat=repeat, filesystem=filesystem,
                            platform=platform.platform(), binary=str(binary), binary_sha256=binary_sha256, directory=str(directory),
                            tmpdir=os.environ.get('TMPDIR', '/tmp'))
            with subprocess.Popen([str(binary), '--directory', str(directory), '--size', str(a.size),
                                   '--chunk-size', str(chunk)], stdout=subprocess.PIPE, text=True) as process:
                for line in process.stdout:
                    record = json.loads(line)
                    output.write(json.dumps(dict(metadata, **record)) + '\n')
                    output.flush()
                if process.wait() != 0:
                    raise SystemExit('Benchmark failed; partial results retained in output')
print(f'Results: {a.output}', flush=True)
