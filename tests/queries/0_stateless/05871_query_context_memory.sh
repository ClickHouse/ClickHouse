#!/usr/bin/env bash
# Tags: no-sanitizers
# Requires the allocation interceptors that sanitizer builds replace.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e
python3 - <<'PYTHON'
import http.client
import os
import shlex
import subprocess
import time
import urllib.parse
import uuid

client = shlex.split(os.environ['CLICKHOUSE_CLIENT'])
user = 'query_context_' + uuid.uuid4().hex
payload_size = 8 * 1024 * 1024


def native(query, **settings):
    args = client + [f'--{key}={value}' for key, value in settings.items()]
    result = subprocess.run(args, input=query, text=True, capture_output=True, timeout=60)
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


def admin(query):
    return native(query, max_query_size=128 * 1024 * 1024, log_queries=0)


def http_request(query, expect_error=False, **settings):
    settings['database'] = os.environ['CLICKHOUSE_DATABASE']
    connection = http.client.HTTPConnection(os.environ['CLICKHOUSE_HOST'], int(os.environ['CLICKHOUSE_PORT_HTTP']), timeout=60)
    try:
        connection.request('POST', '/?' + urllib.parse.urlencode(settings), query, headers={'Connection': 'close'})
        with connection.sock.dup() as peer:
            response = connection.getresponse()
            output = response.read().decode()
            assert (response.status != 200) == expect_error, (response.status, output)
            # The response body can finish before the handler releases its query context.
            assert peer.recv(1) == b'', 'HTTP handler did not finish closing the connection'
        return output.strip()
    finally:
        connection.close()


def wait_for(query, expected):
    deadline = time.monotonic() + 30
    while True:
        result = admin(query)
        if result == expected:
            return
        assert time.monotonic() < deadline, (query, result, expected)
        time.sleep(0.05)


def check_peak(query_id):
    admin('SYSTEM FLUSH LOGS')
    row = admin("SELECT memory_usage, length(log_comment) FROM system.query_log "
                f"WHERE current_database = currentDatabase() AND query_id = '{query_id}' AND type = 'QueryFinish'").split('\t')
    assert int(row[0]) >= payload_size and int(row[1]) == 0, row


def open_session(size):
    session_id = uuid.uuid4().hex
    http_request("SET log_comment = '" + 'x' * size + "'", user=user, session_id=session_id,
         max_query_size=2 * size, log_queries=0)
    return session_id


def close_session(session_id):
    http_request('SELECT 1', user=user, session_id=session_id, log_comment='', close_session=1, log_queries=0)


def start_sentinel(retain_payload=False, **settings):
    query_id = uuid.uuid4().hex
    query = ('SELECT repeat(\'ssssssssssssssssssssssssssssssss\', 524288), sleep(600) '
             if retain_payload else 'SELECT sleepEachRow(1) FROM numbers(600) ')
    query += 'SETTINGS max_block_size=1, function_sleep_max_microseconds_per_block=10000000000 FORMAT Null'
    args = client + ['--user', user, '--query_id', query_id, '--max_untracked_memory=0', '--log_queries=0', '--max_threads=1']
    args += [f'--{key}={value}' for key, value in settings.items()]
    process = subprocess.Popen(args + ['--query', query], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        wait_for(f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'", '1')
    except BaseException:
        stop_sentinel(query_id, process)
        raise
    return query_id, process


def stop_sentinel(query_id, process):
    admin(f"KILL QUERY WHERE query_id = '{query_id}' SYNC")
    process.communicate(timeout=30)


admin(f"CREATE USER {user} SETTINGS log_comment='" + 'x' * payload_size + "'")
admin(f'GRANT SELECT ON *.* TO {user}')
try:
    for protocol, request in (('native', native), ('http', http_request)):
        for batching in (0, 4 * 1024 * 1024):
            query_id = uuid.uuid4().hex
            assert request('SELECT 1', user=user, query_id=query_id, log_comment='',
                           max_untracked_memory=batching, log_queries=1) == '1'
            # The initial copy predates the protocol override, which retains its capacity.
            check_peak(query_id)
        print(protocol, 'records inherited context before override')

    admin(f"ALTER USER {user} SETTINGS log_comment=''")
    session_id = open_session(payload_size)
    try:
        for batching in (0, 4 * 1024 * 1024):
            query_id = uuid.uuid4().hex
            assert http_request('SELECT 1', user=user, session_id=session_id, query_id=query_id,
                        log_comment='', max_untracked_memory=batching, log_queries=1) == '1'
            check_peak(query_id)
            assert http_request("SELECT length(getSetting('log_comment'))", user=user, session_id=session_id,
                        log_queries=0) == str(payload_size)
            error = http_request('SELECT 1', expect_error=True, user=user, session_id=session_id,
                         log_comment='', max_memory_usage=payload_size // 2, max_untracked_memory=batching)
            assert 'Query memory limit exceeded during query setup' in error, error
            assert http_request('SELECT 1', user=user, session_id=session_id, log_queries=0) == '1'
        print('http session preserves peak, settings and query admission')
    finally:
        close_session(session_id)

    user_limit = 16 * 1024 * 1024
    session_id = open_session(2 * user_limit)
    sentinel_id, sentinel = start_sentinel(max_memory_usage_for_user=user_limit)
    try:
        assert http_request('SELECT 1', user=user, max_memory_usage_for_user=user_limit) == '1'
        before = int(admin(f"SELECT memory_usage FROM system.user_processes WHERE user = '{user}'"))
        for _ in range(10):
            error = http_request('SELECT 1', expect_error=True, user=user, session_id=session_id,
                         max_memory_usage=0, max_memory_usage_for_user=user_limit,
                         max_untracked_memory=0, log_queries=0)
            assert 'MEMORY_LIMIT_EXCEEDED' in error and 'already allocated' in error, error
        after = int(admin(f"SELECT memory_usage FROM system.user_processes WHERE user = '{user}'"))
        # The live sentinel prevents a last-query reset from hiding a leaked admission charge.
        assert abs(after - before) < 64 * 1024, (before, after)
        assert http_request('SELECT 1', user=user, max_memory_usage_for_user=user_limit) == '1'
        print('rejected admission balances with another active query')
    finally:
        stop_sentinel(sentinel_id, sentinel)
        close_session(session_id)

    session_id = open_session(256 * 1024)
    sentinel_id, sentinel = start_sentinel(retain_payload=True)
    try:
        def setup_balance():
            deadline = time.monotonic() + 30
            previous = None
            stable_samples = 0
            while True:
                row = admin("SELECT memory_usage, (SELECT memory_usage FROM system.processes "
                            f"WHERE query_id = '{sentinel_id}') FROM system.user_processes WHERE user = '{user}'")
                pair = tuple(map(int, row.split('\t')))
                # Both tables sample independently; require a quiet interval from the live sentinel.
                stable_samples = stable_samples + 1 if pair == previous else 1
                previous = pair
                # A positive floor prevents saturation from hiding excess credits.
                if min(pair) >= 16 * 1024 * 1024 and stable_samples >= 3:
                    return pair[0] - pair[1]
                assert time.monotonic() < deadline, pair
                time.sleep(0.01)

        # `SET` completes on the request thread, isolating setup cleanup from executor allocations.
        query = 'SET max_block_size = 65536 /*' + 'q' * 8192 + '*/'
        for batching in (0, 4 * 1024 * 1024):
            def small_query():
                assert http_request(query, user=user, session_id=session_id, log_comment='',
                            log_queries=0, max_untracked_memory=batching) == ''

            for _ in range(8):
                small_query()
            before = setup_balance()
            samples = []
            # 144 post-baseline queries expose even a persistent 16-byte per-query residue.
            for _ in range(3):
                for _ in range(48):
                    small_query()
                samples.append(setup_balance())
            assert admin(f"SELECT count() FROM system.processes WHERE query_id = '{sentinel_id}'") == '1'
            assert max(abs(sample - before) for sample in samples) < 2048, (batching, before, samples)
        print('synchronous setup cleanup balances in both batching modes')
    finally:
        stop_sentinel(sentinel_id, sentinel)
        close_session(session_id)
finally:
    admin(f'DROP USER {user}')
PYTHON
