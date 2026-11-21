#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

query="SELECT value FROM system.server_settings WHERE name = 'memory_worker_use_cgroup'"
test_dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$test_dir"
config_file="${test_dir}/config.xml"
trap 'rm -rf "$test_dir"' EXIT

# Check the local default and the implicit local invocation.
${CLICKHOUSE_LOCAL} --query "$query"
${CLICKHOUSE_BINARY} --tmp --query "$query"
ln -s "$(readlink -f "$(command -v "${CLICKHOUSE_BINARY}")")" "$test_dir/clickhouse-client"
"$test_dir/clickhouse-client" --local --tmp --query "$query"

# The long worker period keeps these configuration checks independent of the
# test runner's cgroup usage when explicitly enabling the cgroup source.
${CLICKHOUSE_LOCAL} --query "$query" -- --memory_worker_use_cgroup=1 --memory_worker_period_ms=60000
${CLICKHOUSE_LOCAL} --query "$query" -- --memory_worker_use_cgroup=0

echo '<clickhouse><memory_worker_use_cgroup>1</memory_worker_use_cgroup><memory_worker_period_ms>60000</memory_worker_period_ms></clickhouse>' > "$config_file"
${CLICKHOUSE_LOCAL} --config-file "$config_file" --query "$query"
# XML keeps its existing precedence over server configuration arguments after --.
${CLICKHOUSE_LOCAL} --config-file "$config_file" --query "$query" -- --memory_worker_use_cgroup=0

echo '<clickhouse><memory_worker_use_cgroup>0</memory_worker_use_cgroup><memory_worker_period_ms>60000</memory_worker_period_ms></clickhouse>' > "$config_file"
${CLICKHOUSE_LOCAL} --config-file "$config_file" --query "$query"
${CLICKHOUSE_LOCAL} --config-file "$config_file" --query "$query" -- --memory_worker_use_cgroup=1
