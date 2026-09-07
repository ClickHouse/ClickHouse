#!/usr/bin/env python3
"""Compile and check a generated adapter in isolated processes; no server or timing workload."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import struct
import subprocess

FIELDS = '''magic version ready mode hot page segment event_count counters_wrapper_bytes
created destroyed live_objects peak_live_objects initial_requested_sum initial_usable_sum
initial_allocations_sum final_requested_sum final_usable_sum final_allocations_sum
final_requested_max final_usable_max final_backend_wrapper_requested_sum
final_backend_wrapper_usable_sum published_cold_allocations_sum'''.split()


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--repo', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--compiler', default='clang++')
    p.add_argument('--sanitizers', default='address,undefined', choices=['address,undefined', 'thread', 'none'])
    a = p.parse_args()
    stage = a.repo.resolve(); out = a.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    source = Path(__file__).with_suffix('.cpp')
    source_names = ['src/Common/ProfileEventsPagedExperiment/' + n for n in ['adapter.h', 'common.h', 'hot_paged.h', 'catalogue.h']]
    proof = {'files_sha256': {n: digest(stage/n) for n in source_names}}
    required = list(range(10))  # Matches the explicitly synthetic standalone reservation stub.
    for name, expected in proof['files_sha256'].items():
        if digest(stage / name) != expected:
            raise ValueError(f'Staged source hash differs: {name}')
    binary = out / 'validate_adapter'
    command = [a.compiler, '-std=c++20', '-O1', '-g', '-pthread', '-fno-omit-frame-pointer', '-I', str(stage/'src'), str(source), '-o', str(binary)]
    if a.sanitizers != 'none':
        command.append('-fsanitize=' + a.sanitizers)
    with (out/'build.log').open('w') as log:
        subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True)
    permutation = required + [i for i in range(1562) if i not in required]
    layout = out/'layout.txt'; layout.write_text(''.join(f'{i}\n' for i in permutation))
    duplicate = out/'duplicate.txt'; duplicate.write_text('0\n' * 1562)
    unprotected = out/'unprotected.txt'
    unprotected.write_text(''.join(f'{i}\n' for i in list(reversed(permutation))))
    receipt = {'schema_version': 1, 'scope': 'Standalone adapter and modeled caller; not production Counters move, allocator hooks, or actual signals',
               'generated_headers_sha256': proof['files_sha256'], 'source_sha256': digest(source),
               'driver_sha256': digest(Path(__file__)), 'binary_sha256': digest(binary), 'compile_command': command, 'tests': []}

    def run(name, backend='paged', hot=128, test='normal', code=0, overrides=None, message=None):
        env = {k: v for k, v in os.environ.items() if not k.startswith('CH_COUNTER_')}
        env.update(CH_COUNTER_STORAGE=backend, CH_COUNTER_LAYOUT=str(layout), CH_COUNTER_HOT=str(hot), CH_COUNTER_PAGE='32')
        diagnostics = out/(name + '.bin')
        env['CH_COUNTER_DIAGNOSTICS'] = str(diagnostics)
        env.update(overrides or {})
        with (out/(name + '.log')).open('w') as log:
            process = subprocess.run([str(binary), test], env=env, stdout=log, stderr=subprocess.STDOUT)
        text = (out/(name + '.log')).read_text()
        if process.returncode != code or (message and message not in text):
            raise ValueError(f'{name}: exit {process.returncode}, expected {code}; inspect log')
        item = {'name': name, 'returncode': process.returncode, 'log_sha256': digest(out/(name+'.log'))}
        if test == 'normal':
            data = diagnostics.read_bytes()
            d = dict(zip(FIELDS, struct.unpack('<24Q', data)))
            assert d['magic'] == 0x4350455850455231 and d['version'] == 1 and d['ready'] == 1
            assert d['segment'] == 0 and d['hot'] == hot and d['page'] == 32
            assert d['created'] == d['destroyed'] == 2 and d['live_objects'] == 0 and d['peak_live_objects'] == 2
            base = 2 if backend == 'paged' and hot < 1562 else 1
            assert d['initial_allocations_sum'] == 2 * base
            assert d['published_cold_allocations_sum'] == d['final_allocations_sum'] - 2 * base
            assert d['published_cold_allocations_sum'] == 0 if hot == 1562 or backend == 'dense' else d['published_cold_allocations_sum'] > 0
            assert d['final_requested_sum'] >= d['initial_requested_sum']
            assert d['final_usable_sum'] >= d['final_requested_sum']
            if backend == 'dense':
                assert d['final_backend_wrapper_requested_sum'] == d['final_backend_wrapper_usable_sum'] == 0
            else:
                assert d['final_backend_wrapper_usable_sum'] >= d['final_backend_wrapper_requested_sum'] > 0
            item['diagnostics'] = d
        receipt['tests'].append(item)

    for backend in ['dense', 'paged']:
        for hot in [128, 1562]:
            run(f'{backend}_hot{hot}', backend=backend, hot=hot)
    for backend in ['paged']:
        run(backend+'_cold_signal', backend=backend, test='cold_signal', code=80, message='cold signal event is unsupported')
        run(backend+'_recursive_cold', backend=backend, test='recursive_cold', code=79, message='recursive cold update is unsupported')
    invalid = [
        ('hot_zero', {'CH_COUNTER_HOT': '0'}), ('hot_large', {'CH_COUNTER_HOT': '1563'}),
        ('mode', {'CH_COUNTER_STORAGE': 'unknown'}), ('page', {'CH_COUNTER_PAGE': '3'}),
        ('duplicate', {'CH_COUNTER_LAYOUT': str(duplicate)}), ('unprotected', {'CH_COUNTER_LAYOUT': str(unprotected)}),
        ('missing_layout', {'CH_COUNTER_LAYOUT': str(out/'absent')}),
    ]
    for name, overrides in invalid:
        run('invalid_'+name, test='configuration', code=78, overrides=overrides, message='Counter experiment:')
    for name, expected in proof['files_sha256'].items():
        assert digest(stage/name) == expected
    (out/'receipt.json').write_text(json.dumps(receipt, indent=2)+'\n')
    print(json.dumps({'status': 'PASS', 'tests': len(receipt['tests']), 'receipt': str(out/'receipt.json')}))


if __name__ == '__main__':
    main()
