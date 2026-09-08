#!/usr/bin/env python3
"""Compile and check a generated adapter in isolated processes; no server or timing workload."""
import argparse
import hashlib
import json
import os
import re
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


def actual_catalogue(stage):
    """Resolve this pinned public macro format; reject unsupported catalogue changes."""
    files = ['src/Common/ProfileEvents.cpp', 'src/Common/ProfileEventsNonAllocatingEventList.h',
             'utils/profile-events-paged-experiment/layout_calls128.txt']
    hashes = {name: digest(stage / name) for name in files}
    text = (stage / files[0]).read_text()
    marker = '#define APPLY_FOR_BUILTIN_EVENTS(M)'
    if text.count(marker) != 1:
        raise ValueError('Expected one builtin event macro')
    prefix, body = text.split(marker)
    body = body.split('#ifdef APPLY_FOR_EXTERNAL_EVENTS', 1)[0]
    names = re.findall(r'^\s*M\((\w+),', body, re.MULTILINE)
    if len(names) != 1562 or len(set(names)) != 1562:
        raise ValueError('Expected exactly 1562 unique public builtin events')
    if '__COUNTER__' in prefix or text.count('__COUNTER__') != 2 or '#define M(NAME, DOCUMENTATION, VALUE_TYPE) extern const Event NAME = Event(__COUNTER__);' not in text:
        raise ValueError('Unsupported builtin numeric ID assignment')
    required_text = (stage / files[1]).read_text()
    required = re.findall(r'^\s*M\((\w+)\)', required_text, re.MULTILINE)
    if len(required) != 40 or len(set(required)) != 40 or set(required) - set(names):
        raise ValueError('Expected exactly 40 unique known mandatory events')
    cpu = {'ConcurrencyControl' + suffix for suffix in ['WaitMicroseconds', 'PreemptedMicroseconds',
           'SlotsAcquired', 'SlotsAcquiredNonCompeting', 'Upscales', 'Downscales', 'Preemptions']}
    memory = {'MemoryReservation' + suffix for suffix in ['AdmitMicroseconds', 'IncreaseMicroseconds',
              'Increases', 'Decreases', 'Failed', 'Killed']}
    io = {'SchedulerIO' + direction + suffix for direction in ['Read', 'Write']
          for suffix in ['Requests', 'Bytes', 'WaitMicroseconds']}
    if not (cpu | memory | io) <= set(required):
        raise ValueError('Mandatory catalogue omits a required CPU/memory/IO scheduler event')
    ids = {name: i for i, name in enumerate(names)}
    rank = [int(line) for line in (stage / files[2]).read_text().splitlines()]
    if len(rank) != 1562 or set(rank) != set(range(1562)):
        raise ValueError('Shipped layout is not the public catalogue permutation')
    return {'input_sha256': hashes, 'required_name_ids': {name: ids[name] for name in required},
            'scheduler_names': sorted(cpu | memory | io), 'builtin_count': len(names), 'rank': rank}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--repo', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--compiler', default='clang++')
    p.add_argument('--actual-catalogue', action='store_true', help='Use the pinned public 40-event catalogue and shipped layout')
    p.add_argument('--sanitizers', default='address,undefined', choices=['address,undefined', 'thread', 'none'])
    a = p.parse_args()
    stage = a.repo.resolve(); out = a.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    source = Path(__file__).with_suffix('.cpp')
    source_names = ['src/Common/ProfileEventsPagedExperiment/' + n for n in ['adapter.h', 'common.h', 'hot_paged.h', 'catalogue.h']]
    proof = {'files_sha256': {n: digest(stage/n) for n in source_names}}
    catalogue = actual_catalogue(stage) if a.actual_catalogue else None
    required = list(catalogue['required_name_ids'].values()) if catalogue else list(range(10))
    if catalogue:
        proof['files_sha256'].update(catalogue['input_sha256'])
        generated = out / 'required_events.h'
        generated.write_text('#pragma once\ninline constexpr std::array<uint16_t, 40> actual_required_events{' +
                             ', '.join(map(str, required)) + '};\n')
    for name, expected in proof['files_sha256'].items():
        if digest(stage / name) != expected:
            raise ValueError(f'Staged source hash differs: {name}')
    binary = out / 'validate_adapter'
    command = [a.compiler, '-std=c++20', '-O1', '-g', '-pthread', '-fno-omit-frame-pointer', '-I', str(stage/'src'), str(source), '-o', str(binary)]
    if catalogue:
        command.extend(['-DVALIDATE_ACTUAL_CATALOGUE=1', '-I', str(out)])
    if a.sanitizers != 'none':
        command.append('-fsanitize=' + a.sanitizers)
    with (out/'build.log').open('w') as log:
        subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True)
    permutation = catalogue['rank'] if catalogue else required + [i for i in range(1562) if i not in required]
    layout = out/'layout.txt'; layout.write_text(''.join(f'{i}\n' for i in permutation))
    duplicate = out/'duplicate.txt'; duplicate.write_text('0\n' * 1562)
    unprotected = out/'unprotected.txt'
    unprotected.write_text(''.join(f'{i}\n' for i in list(reversed(permutation))))
    receipt = {'schema_version': 1, 'scope': 'Standalone adapter and modeled caller; not production Counters move, allocator hooks, or actual signals',
               'generated_headers_sha256': proof['files_sha256'], 'source_sha256': digest(source),
               'driver_sha256': digest(Path(__file__)), 'binary_sha256': digest(binary), 'compile_command': command, 'tests': [], 'catalogue': catalogue,
               'generated_required_header_sha256': digest(generated) if catalogue else None}

    def run(name, backend='paged', hot=128, test='normal', code=0, overrides=None, message=None):
        env = {k: v for k, v in os.environ.items() if not k.startswith('CH_COUNTER_')}
        env.update(CH_COUNTER_STORAGE=backend, CH_COUNTER_LAYOUT=str(layout), CH_COUNTER_HOT=str(hot), CH_COUNTER_PAGE='32')
        diagnostics = out/(name + '.bin')
        env['CH_COUNTER_DIAGNOSTICS'] = str(diagnostics)
        for key, value in (overrides or {}).items():
            if value is None:
                env.pop(key, None)
            else:
                env[key] = value
        with (out/(name + '.log')).open('w') as log:
            process = subprocess.run([str(binary), test], env=env, stdout=log, stderr=subprocess.STDOUT)
        text = (out/(name + '.log')).read_text()
        if process.returncode != code or (message and message not in text):
            raise ValueError(f'{name}: exit {process.returncode}, expected {code}; inspect log')
        if test == 'catalogue_layout':
            expected = [event for event in permutation if event in required] + [event for event in permutation if event not in required]
            assert [int(line) for line in text.splitlines()] == expected
            assert set(required) <= set(expected[:hot])
        item = {'name': name, 'returncode': process.returncode, 'log_sha256': digest(out/(name+'.log'))}
        if test in ['normal', 'reserved_updates', 'allocation_failure']:
            data = diagnostics.read_bytes()
            d = dict(zip(FIELDS, struct.unpack('<24Q', data)))
            assert d['magic'] == 0x4350455850455231 and d['version'] == 1 and d['ready'] == 1
            assert d['segment'] == 0 and d['hot'] == hot and d['page'] == 32
            assert d['created'] == d['destroyed'] == 2 and d['live_objects'] == 0 and d['peak_live_objects'] == 2
            base = 2 if backend == 'paged' and hot < 1562 else 1
            assert d['initial_allocations_sum'] == 2 * base
            assert d['published_cold_allocations_sum'] == d['final_allocations_sum'] - 2 * base
            if test == 'reserved_updates':
                assert d['published_cold_allocations_sum'] == 0
                assert d['final_requested_sum'] == d['initial_requested_sum']
                assert d['final_usable_sum'] == d['initial_usable_sum']
            elif test == 'allocation_failure':
                assert d['published_cold_allocations_sum'] == 1
            else:
                assert d['published_cold_allocations_sum'] == 0 if hot == 1562 or backend == 'dense' else d['published_cold_allocations_sum'] > 0
            assert d['final_requested_sum'] >= d['initial_requested_sum']
            assert d['final_usable_sum'] >= d['final_requested_sum']
            if backend == 'dense':
                assert d['final_backend_wrapper_requested_sum'] == d['final_backend_wrapper_usable_sum'] == 0
            else:
                assert d['final_backend_wrapper_usable_sum'] >= d['final_backend_wrapper_requested_sum'] > 0
            item['diagnostics'] = d
        receipt['tests'].append(item)

    if catalogue:
        for hot in [40, 128]:
            run(f'actual_layout_hot{hot}', hot=hot, test='catalogue_layout')
            run(f'actual_reserved_hot{hot}', hot=hot, test='reserved_updates')
            run(f'actual_fault_hot{hot}', hot=hot, test='allocation_failure')
    run('process_dense', backend='dense', test='process_storage')
    run('process_default_dense', test='process_storage', overrides={'CH_COUNTER_STORAGE': None})
    for hot in [128, 1562]:
        run(f'process_paged_rejected_hot{hot}', hot=hot, test='process_storage', code=78,
            message='paged process counters are disabled pending the nonallocating publisher audit')
    run('reservation_layout', test='reservations')
    for backend in ['dense', 'paged']:
        for hot in [128, 1562]:
            run(f'{backend}_reserved_hot{hot}', backend=backend, hot=hot, test='reserved_updates',
                overrides={'CH_COUNTER_LAYOUT': str(unprotected)})
    run('paged_cold_allocation_failure', test='allocation_failure', overrides={'CH_COUNTER_LAYOUT': str(unprotected)})
    for backend in ['dense', 'paged']:
        for hot in [128, 1562]:
            run(f'{backend}_hot{hot}', backend=backend, hot=hot)
    run('reserved_from_reverse_rank', overrides={'CH_COUNTER_LAYOUT': str(unprotected)})
    for backend in ['paged']:
        run(backend+'_cold_signal', backend=backend, test='cold_signal', code=80, message='cold signal event is unsupported')
        run(backend+'_recursive_cold', backend=backend, test='recursive_cold', code=79, message='recursive cold update is unsupported')
    invalid = [
        ('hot_zero', {'CH_COUNTER_HOT': '0'}), ('hot_large', {'CH_COUNTER_HOT': '1563'}),
        ('mode', {'CH_COUNTER_STORAGE': 'unknown'}), ('page', {'CH_COUNTER_PAGE': '3'}),
        ('duplicate', {'CH_COUNTER_LAYOUT': str(duplicate)}), ('hot_too_small', {'CH_COUNTER_HOT': str(len(required) - 1)}),
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
