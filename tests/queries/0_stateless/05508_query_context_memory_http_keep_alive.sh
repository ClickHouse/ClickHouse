#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e
python3 - <<'PYTHON'
import http.client
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


def request(connection, query, agent, **settings):
    settings.update(user=user)
    connection.request('POST', '/?' + urllib.parse.urlencode(settings), query, {'User-Agent': agent})
    response = connection.getresponse()
    output = response.read().decode()
    assert not response.will_close, output
    return response.status, output


# The stateless server configuration enables `collect_connection_metrics`.
admin(f"CREATE USER {user} SETTINGS format_csv_null_representation='" + 'x' * context_bytes + "', log_queries=0, log_query_settings=0")
try:
    admin(f'GRANT SELECT ON *.* TO {user}')
    for batching in (0, 4 * 1024 * 1024):
        connection = http.client.HTTPConnection(os.environ['CLICKHOUSE_HOST'], int(os.environ['CLICKHOUSE_PORT_HTTP']), timeout=60)
        try:
            settings = dict(max_untracked_memory=batching)
            query = 'SELECT memory_usage FROM system.processes WHERE query_id=currentQueryID()'
            status, output = request(connection, query, 'y' * 64, **settings)
            assert status == 200, output
            baseline = int(output)
            status, output = request(connection, query, 'x' * (120 * 1024), **settings)
            assert status == 200, output
            status, output = request(connection, query, 'y' * 64, **settings)
            assert status == 200, output
            # Allow small per-request differences, but not the old 128 KiB header allocation.
            assert int(output) >= baseline - 64 * 1024, (batching, baseline, output)
            print(batching, 'retains context accounting')
        finally:
            connection.close()
finally:
    admin(f'DROP USER {user}')
PYTHON
