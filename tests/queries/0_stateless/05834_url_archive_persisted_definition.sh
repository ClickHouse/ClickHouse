#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

# Two `clickhouse local` invocations against the same `--path`: the second reloads the metadata the first
# persisted, which is the path a server takes at startup. The host is unreachable on purpose, and `EXPLAIN`
# stops before the pipeline, so no arm needs a live server.
WORKING_FOLDER="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${WORKING_FOLDER}"
trap 'rm -rf "${WORKING_FOLDER}"' EXIT

# The arms that reach the unreachable host would spend the default ten attempts with backoff.
NO_RETRY="SET http_max_tries = 1;"

run_local() {
    local out
    out=$(${CLICKHOUSE_LOCAL} --path="$1" --query "${NO_RETRY} $2" \
        -- --max_server_memory_usage=10G --memory_worker_use_cgroup=0 2>&1)
    echo "$out" | grep -E '^(created|reloaded|plan)\b' || true
}

# Both candidate storages report engine `URL` in `system.tables`, so the engine name cannot say which one a
# reload picked. The read step names them apart: `ReadFromURL` is the plain storage that keeps `::` literal,
# `ReadFromObjectStorage` is the archive reader. This is the migration the `url` documentation promises: a
# definition stored while `::` was literal is read as archive syntax once the setting is on again.
show_table_and_plan() {
    echo "SELECT 'reloaded', engine FROM system.tables WHERE database = 'd' AND name = 't';
          SELECT 'plan', countIf(explain ILIKE '%ReadFromURL%') > 0,
                         countIf(explain ILIKE '%ReadFromObjectStorage%') > 0
          FROM (EXPLAIN SELECT count() FROM d.t);"
}

echo '--- a wildcard archive definition loads without the experimental setting'
# The gate is a session setting, so on a reload it would be the server default; a stored definition must not
# be re-validated against it, or the table would disappear from the catalog on startup.
D="${WORKING_FOLDER}/wildcard"; mkdir -p "$D"
run_local "$D" "
    SET allow_url_wildcard_from_index_pages = 1;
    CREATE DATABASE d;
    CREATE TABLE d.t (x Int32) AS url('http://127.0.0.1:1/{a,b}.zip::m.csv', CSV, 'x Int32');
    SELECT 'created', engine FROM system.tables WHERE database = 'd' AND name = 't';"
run_local "$D" "$(show_table_and_plan)"

echo '--- a definition stored with `::` literal is read as archive syntax after the reload'
D="${WORKING_FOLDER}/archive"; mkdir -p "$D"
run_local "$D" "
    SET allow_archive_path_syntax = 0;
    CREATE DATABASE d;
    CREATE TABLE d.t (x Int32) AS url('http://127.0.0.1:1/a.zip::m.csv', CSV, 'x Int32');
    SELECT 'created', engine FROM system.tables WHERE database = 'd' AND name = 't';"
run_local "$D" "$(show_table_and_plan)"
