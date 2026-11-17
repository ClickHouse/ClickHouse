#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e
python3 - <<'PYTHON'
import http.client
import json
import os
import shlex
import subprocess
import urllib.parse
import uuid

client = shlex.split(os.environ['CLICKHOUSE_CLIENT'])
user = 'keep_alive_context_' + uuid.uuid4().hex
context_bytes = 8 * 1024 * 1024


def admin(query):
    result = subprocess.run(client + ['--max_query_size=33554432', '--log_queries=0'], input=query, text=True, capture_output=True, timeout=60)
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


def request(connection, agent, **settings):
    settings.update(user=user)
    connection.request('POST', '/?' + urllib.parse.urlencode(settings), 'SELECT 1', {'User-Agent': agent})
    response = connection.getresponse()
    output = response.read().decode()
    assert response.status == 200, output
    assert output == '1\n', output
    assert not response.will_close, output
    return int(json.loads(response.getheader('X-ClickHouse-Summary'))['memory_usage'])


# The stateless server configuration enables `collect_connection_metrics`.
admin(f"CREATE USER {user} SETTINGS format_csv_null_representation='" + 'x' * context_bytes + "', log_queries=0, log_query_settings=0")
try:
    for batching in (0, 4 * 1024 * 1024):
        connection = http.client.HTTPConnection(os.environ['CLICKHOUSE_HOST'], int(os.environ['CLICKHOUSE_PORT_HTTP']), timeout=60)
        try:
            settings = dict(max_untracked_memory=batching, max_threads=1, wait_end_of_query=1)
            # Warm the connection before measuring retained query memory.
            request(connection, 'y' * 64, **settings)
            baseline = request(connection, 'y' * 64, **settings)
            request(connection, 'x' * (120 * 1024), **settings)
            reused = request(connection, 'y' * 64, **settings)
            # Allow small per-request differences, but not the old 128 KiB header allocation.
            assert reused >= baseline - 64 * 1024, (batching, baseline, reused)
            print(batching, 'retains context accounting')
        finally:
            connection.close()
finally:
    admin(f'DROP USER {user}')
PYTHON
