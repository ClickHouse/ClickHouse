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
user = 'background_context_' + uuid.uuid4().hex
payload_size = 8 * 1024 * 1024


def admin(query):
    result = subprocess.run(client + ['--max_query_size=33554432', '--log_queries=0'], input=query, text=True, capture_output=True, timeout=60)
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


def request(protocol, query, query_id, **settings):
    settings.update(run_query_in_background=1, log_queries=0, query_metric_log_interval=0, max_threads=1)
    settings.setdefault('max_untracked_memory', 0)
    if protocol == 'native':
        args = client + ['--user', user, '--query_id', query_id]
        args += [f'--{key}={value}' for key, value in settings.items()]
        result = subprocess.run(args, input=query, text=True, capture_output=True, timeout=60)
        return result.returncode == 0, result.stdout + result.stderr
    connection = http.client.HTTPConnection(os.environ['CLICKHOUSE_HOST'], int(os.environ['CLICKHOUSE_PORT_HTTP']), timeout=60)
    try:
        settings.update(user=user, query_id=query_id)
        connection.request('POST', '/?' + urllib.parse.urlencode(settings), query)
        response = connection.getresponse()
        return response.status == 200, response.read().decode()
    finally:
        connection.close()


def wait_until_finished(query_id):
    deadline = time.monotonic() + 30
    while admin(f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'") != '0':
        assert time.monotonic() < deadline, query_id
        time.sleep(0.05)


# Use a setting the query does not consume, and disable query-log copies of settings.
admin(f"CREATE USER {user} SETTINGS format_csv_null_representation='" + 'x' * payload_size + "', log_queries=0, log_query_settings=0")
admin(f'GRANT SELECT ON *.* TO {user}')
try:
    for protocol in ('native', 'http'):
        for limit, diagnostic in (
            ('max_memory_usage', 'Query memory limit exceeded during query setup'),
            ('max_memory_usage_for_user', 'already allocated'),
        ):
            query_id = uuid.uuid4().hex
            success, output = request(protocol, 'SELECT 1', query_id, **{limit: payload_size // 2})
            assert not success and 'MEMORY_LIMIT_EXCEEDED' in output, (protocol, limit, output)
            # A later execution allocation exceeding a limit does not prove setup admission.
            assert diagnostic in output, (protocol, limit, output)
            wait_until_finished(query_id)
            print(protocol, limit, 'rejects retained context at admission')

        query_id = uuid.uuid4().hex
        try:
            success, output = request(
                protocol,
                'SELECT sleepEachRow(0.1) FROM numbers(6000) SETTINGS max_block_size=1 FORMAT Null',
                query_id,
                max_memory_usage=128 * 1024 * 1024,
                max_memory_usage_for_user=128 * 1024 * 1024,
            )
            assert success, (protocol, output)
            memory = admin(f"SELECT memory_usage FROM system.processes WHERE query_id = '{query_id}'")
            assert memory and int(memory) >= payload_size, (protocol, memory)
            print(protocol, 'reports retained context while running')

            # Keep a live query so a user-tracker reset cannot hide cleanup under the wrong tracker.
            for batching_limit in (0, 4 * 1024 * 1024):
                for _ in range(4):
                    finished_id = uuid.uuid4().hex
                    success, output = request(protocol, 'SELECT 1', finished_id, max_untracked_memory=batching_limit)
                    assert success, (protocol, output)
                    wait_until_finished(finished_id)
                deadline = time.monotonic() + 30
                while True:
                    balance = int(admin(
                        "SELECT memory_usage - (SELECT memory_usage FROM system.processes "
                        f"WHERE query_id = '{query_id}') FROM system.user_processes WHERE user = '{user}'"
                    ))
                    if abs(balance) < 65536:
                        break
                    assert time.monotonic() < deadline, (protocol, batching_limit, balance)
                    time.sleep(0.05)
            print(protocol, 'balances context cleanup with another live query')
        finally:
            admin(f"KILL QUERY WHERE query_id = '{query_id}' SYNC")
            wait_until_finished(query_id)
finally:
    admin(f'DROP USER {user}')
PYTHON
