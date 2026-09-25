#!/usr/bin/env python3
"""Summarize complete benchmark JSONL cases as Markdown (median and range)."""
import argparse
import json
import statistics
from collections import defaultdict

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('results', nargs='+')
a = p.parse_args()
cases = defaultdict(list)
for filename in a.results:
    with open(filename) as source:
        for line in source:
            r = json.loads(line)
            cases[(filename, r['directory'], r['filesystem'], r['size'], r['chunk_size'], r['repeat'], r.get('binary_sha256', 'unrecorded'), r['platform'])].append(r)
if not cases:
    p.error('No benchmark records')
required = {'source', 'describe', 'open_cold', 'receive', 'assemble', 'close_cold',
            'open_warm', 'rehash', 'clear', 'close_warm'}
groups = defaultdict(list)
for key, records in cases.items():
    phases = [r for r in records if r['kind'] == 'phase']
    controls = [r for r in records if r['kind'] == 'control']
    if len(phases) != len(required) or {r['phase'] for r in phases} != required or len(controls) != 4:
        p.error('Incomplete or duplicate case: ' + str(key))
    control_map = {(r['direction'], r['cached']): r for r in controls}
    if set(control_map) != {('push', False), ('push', True), ('pull', False), ('pull', True)}:
        p.error('Missing control scenario: ' + str(key))
    groups[key[1:5] + key[6:]].append(({r['phase']: r for r in phases}, control_map))

def timing(cases, phase):
    values = [phases[phase]['elapsed_ms'] / 1000 for phases, _ in cases]
    return f'{statistics.median(values):.3f} ({min(values):.3f}–{max(values):.3f})'

for (directory, filesystem, size, chunk, binary_sha256, system), runs in sorted(groups.items()):
    print(f'## {filesystem}: {size} bytes, chunk {chunk} bytes, {len(runs)} runs\n')
    print(f'Fixture parent: `{directory}`. Times in seconds: median (min–max).\n')
    print(f'Platform: {system}. Binary SHA-256: `{binary_sha256}`.\n')
    print('| Phase | Seconds |\n| --- | --- |')
    for phase in ['source', 'describe', 'open_cold', 'receive', 'assemble', 'close_cold',
                  'open_warm', 'rehash', 'clear', 'close_warm']:
        print(f'| {phase} | {timing(runs, phase)} |')
    peak = max(r['process_peak_rss_kib'] for phases, _ in runs for r in phases.values())
    cache = max(r['cache_allocated_bytes'] for phases, _ in runs for r in phases.values())
    print(f'\nReported process peak RSS: {peak} KiB. Cache allocation checkpoint maximum: {cache} bytes.\n')
    print('| Direction | Cached | Control bytes | Exchanges | Payload bytes |\n| --- | --- | ---: | ---: | ---: |')
    first = runs[0][1]
    for scenario, r in sorted(first.items()):
        for _, controls in runs:
            if any(controls[scenario][field] != r[field] for field in ['bytes', 'requests', 'payload_bytes']):
                p.error('Control measurements differ across repeats')
        print(f"| {r['direction']} | {r['cached']} | {r['bytes']} | {r['requests']} | {r['payload_bytes']} |")
    print()
