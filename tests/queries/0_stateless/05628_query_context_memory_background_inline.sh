#!/usr/bin/env bash
# Tags: no-sanitizers
# Requires the allocation interceptors that sanitizer builds replace.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e
python3 - <<'PYTHON'
import os
import shlex
import socket
import struct
import subprocess
import time
import uuid

client = shlex.split(os.environ['CLICKHOUSE_CLIENT'])
suffix = uuid.uuid4().hex
user = 'background_inline_' + suffix
table = 'default.background_inline_' + suffix
payload = ('x' * 8191 + '\n') * 1024
revision = 54452


def admin(query, allow_nothing_to_kill=False):
    result = subprocess.run(client + ['--log_queries=0'], input=query, text=True, capture_output=True, timeout=60)
    assert result.returncode == 0 or (allow_nothing_to_kill and 'NOTHING_TO_KILL' in result.stderr), result.stderr
    return result.stdout.strip()


def varuint(value):
    result = bytearray()
    while value >= 128:
        result.append((value & 127) | 128)
        value >>= 7
    return bytes(result) + bytes([value])


def string(value):
    data = value.encode()
    return varuint(len(data)) + data


def read_uint(stream):
    value = shift = 0
    while True:
        byte = stream.read(1)
        assert byte, 'Unexpected end of Native response'
        value |= (byte[0] & 127) << shift
        if byte[0] < 128:
            return value
        shift += 7


def read_string(stream):
    size = read_uint(stream)
    data = stream.read(size)
    assert len(data) == size, 'Truncated Native string'
    return data.decode()


def background(query, query_id, **settings):
    # Send the complete inline payload in a Query packet. clickhouse-client
    # parses INSERT data locally and would exercise a different ownership path.
    settings.update(run_query_in_background=1, async_insert=0, log_queries=0, query_metric_log_interval=0,
                    send_logs_level='none', send_profile_events=0, max_threads=1,
                    max_query_size=32 * 1024 * 1024, max_insert_block_size=16,
                    input_format_parallel_parsing=0, function_sleep_max_microseconds_per_block=6_000_000)
    settings.setdefault('max_untracked_memory', 0)
    with socket.create_connection((os.environ['CLICKHOUSE_HOST'], int(os.environ['CLICKHOUSE_PORT_TCP'])), timeout=60) as sock:
        with sock.makefile('rb') as stream:
            sock.sendall(varuint(0) + string('background-inline-test') + varuint(22) + varuint(3)
                         + varuint(revision) + string('default') + string(user) + string(''))
            assert read_uint(stream) == 0, 'Expected Server::Hello'
            read_string(stream)
            for _ in range(3):
                read_uint(stream)
            read_string(stream)  # timezone
            read_string(stream)  # display name
            read_uint(stream)  # version patch

            info = (b'\x01' + string(user) + string(query_id) + string('127.0.0.1:9000')
                    + struct.pack('<Q', int(time.time() * 1_000_000)) + b'\x01'
                    + string('test') + string('localhost') + string('background-inline-test')
                    + varuint(22) + varuint(3) + varuint(revision) + string('')
                    + varuint(0) + varuint(1) + b'\x00')
            encoded_settings = b''.join(string(name) + varuint(1) + string(str(value))
                                        for name, value in settings.items())
            sock.sendall(varuint(1) + string(query_id) + info + encoded_settings + string('')
                         + string('') + varuint(2) + varuint(0) + string(query))
            # Required trailing empty Data block, with compression disabled.
            sock.sendall(b'\x02\x00\x00\x00\x00')
            response = read_uint(stream)
            if response == 2:  # Server::Exception
                code = struct.unpack('<i', stream.read(4))[0]
                read_string(stream)  # name
                message = read_string(stream)
                read_string(stream)  # stack trace
                assert stream.read(1) == b'\x00', 'Unexpected nested exception'
                return code, message
            assert response == 5, response  # Server::EndOfStream
            return 0, ''


def wait_finished(query_id):
    deadline = time.monotonic() + 30
    while admin(f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'") != '0':
        assert time.monotonic() < deadline, query_id
        time.sleep(0.05)


holder_id = uuid.uuid4().hex
admin(f'CREATE USER {user}')
try:
    # Small input blocks keep execution memory below the low admission limits.
    # Sleeping per row keeps the accepted query observable without retaining data in storage.
    # The materialized expression can receive all 1024 rows together, requiring 5.12 seconds.
    admin(f'CREATE TABLE {table} (s String, pause UInt8 MATERIALIZED sleepEachRow(0.005)) ENGINE=Null')
    admin(f'GRANT SELECT, INSERT ON *.* TO {user}')
    query = f'INSERT INTO {table} FORMAT TSV\n' + payload
    for limit, diagnostic in (
        ('max_memory_usage', 'Query memory limit exceeded during query setup'),
        ('max_memory_usage_for_user', 'already allocated'),
    ):
        query_id = uuid.uuid4().hex
        code, message = background(query, query_id, **{limit: len(payload) // 2})
        assert code == 241 and diagnostic in message, (limit, code, message)
        wait_finished(query_id)
        print(limit, 'rejects inline payload at admission')

    code, message = background('SELECT sleepEachRow(0.1) FROM numbers(6000) SETTINGS max_block_size=1 FORMAT Null', holder_id)
    assert code == 0, message
    for batching_limit in (0, 4 * 1024 * 1024):
        query_id = uuid.uuid4().hex
        code, message = background(query, query_id, max_untracked_memory=batching_limit,
                                   max_memory_usage=128 * 1024 * 1024, max_memory_usage_for_user=128 * 1024 * 1024)
        assert code == 0, message
        memory = admin(f"SELECT memory_usage FROM system.processes WHERE query_id = '{query_id}'")
        assert memory and int(memory) >= len(payload), (batching_limit, memory)
        wait_finished(query_id)
        deadline = time.monotonic() + 30
        while True:
            balance = int(admin('SELECT memory_usage - (SELECT memory_usage FROM system.processes '
                                f"WHERE query_id = '{holder_id}') FROM system.user_processes WHERE user = '{user}'"))
            if abs(balance) < 65536:
                break
            assert time.monotonic() < deadline, (batching_limit, balance)
            time.sleep(0.05)
        print(batching_limit, 'reports and releases inline payload with another query active')
finally:
    admin(f"KILL QUERY WHERE user = '{user}' SYNC", allow_nothing_to_kill=True)
    admin(f'DROP TABLE IF EXISTS {table}')
    admin(f'DROP USER {user}')
PYTHON
