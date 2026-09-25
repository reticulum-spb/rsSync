#!/usr/bin/env python3
"""Summarize complete local Reticulum benchmark cases from JSONL."""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import statistics

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('results', type=Path)
a = p.parse_args()
groups = defaultdict(list)
cases = defaultdict(set)
with a.results.open() as stream:
    for line in stream:
        r = json.loads(line)
        metadata = tuple(r[key] for key in ('binary_sha256', 'platform', 'directory', 'filesystem',
                                            'tmpdir', 'size', 'files', 'chunk_size', 'sample_interval',
                                            'protocol', 'checksum'))
        case = metadata + (r['repeat'],)
        phase = r['phase']
        if phase not in {'push_cold', 'push_unchanged', 'pull_cold', 'pull_unchanged'} or phase in cases[case]:
            p.error('invalid or duplicate phase')
        cases[case].add(phase)
        groups[(metadata, phase)].append(r)
if not cases or any(len(phases) != 4 for phases in cases.values()):
    p.error('empty results or incomplete case; preserve partial data but do not summarize it')


def spread(values):
    return f'{statistics.median(values):.3f} ({min(values):.3f}–{max(values):.3f})'


previous = None
for (metadata, phase), rows in sorted(groups.items()):
    if metadata != previous:
        sha, platform, directory, fs, tmpdir, size, files, chunk, interval, protocol, checksum = metadata
        print(f'\n## {fs}: {files} × {size} bytes, chunk {chunk}\n')
        print(f'Binary SHA-256: `{sha}`. {platform}. Directory `{directory}`, TMPDIR `{tmpdir}`. '
              f'Protocol {protocol}, checksum={checksum}, requested sampling interval {interval}s.\n')
        print('| Phase | Runs | Seconds median (range) | Local RX median (range), KiB | Local TX median (range), KiB | Client/server max sampled RSS, MiB | Max sampled allocated files, MiB | Recorded FD access misses |')
        print('| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |')
        previous = metadata
    peak = lambda key: max(row['sampled_peak'][key] for row in rows)
    print(f"| {phase} | {len(rows)} | {spread([r['elapsed_seconds'] for r in rows])} | "
          f"{spread([r['client_local_rx_bytes']/1024 for r in rows])} | "
          f"{spread([r['client_local_tx_bytes']/1024 for r in rows])} | "
          f"{peak('client_rss_kib')/1024:.2f} / {peak('server_rss_kib')/1024:.2f} | "
          f"{peak('allocated_bytes')/1048576:.2f} | "
          f"{sum(r.get('sampling_misses', {}).get('proc_fd', 0) for r in rows)} |")
print('\nLocal counters include shared-instance framing and possible unrelated incoming announces; '
      'they are not radio traffic. Counters stop before runtime shutdown. '
      'RSS/disk are sampled lower bounds on peaks, excluding daemon/kernel memory and logs. '
      'Allocated files include source, destinations, caches and open anonymous files, '
      'but exclude directory metadata. The server persists across the four phases of a case.')
