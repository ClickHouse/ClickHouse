#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

# A source this version parses as archive syntax is rejected together with a failover group or a custom HTTP
# method. A definition stored by an earlier version, which kept the `::` literal, has to keep loading instead:
# refusing it on startup would drop the table from the catalog. Both carriers are covered, `ENGINE = URL` and
# the `url` table function.

REPLAY_PATH=$(mktemp -d "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.replay.XXXXXX")
trap 'rm -rf "$REPLAY_PATH"' EXIT
MEMORY="-- --max_server_memory_usage=10G --memory_worker_use_cgroup=0"

# With archive syntax off, every source below is an ordinary URL, so an earlier version accepted it.
${CLICKHOUSE_LOCAL} --path="$REPLAY_PATH" --allow_archive_path_syntax=0 --allow_url_wildcard_from_index_pages=1 --multiquery --query "
    CREATE NAMED COLLECTION replayed_method AS
        url = 'http://127.0.0.1:1/method.zip::m.csv',
        format = 'CSV', structure = 'x UInt64', method = 'POST';

    CREATE TABLE engine_literal_member (x UInt64) ENGINE = URL('http://127.0.0.1:1/a.zip::m.csv', 'CSV');
    CREATE TABLE engine_failover_archive (x UInt64) ENGINE = URL('http://{127.0.0.1:1|127.0.0.1:2}/*.zip::m.csv', 'CSV');
    CREATE TABLE function_failover_archive (x UInt64) AS url('http://127.0.0.1:1/a.zip|http://127.0.0.1:2/b.zip::m.csv', 'CSV', 'x UInt64');
    CREATE TABLE function_custom_method (x UInt64) AS url(replayed_method);
    CREATE TABLE function_query_template (x UInt64) AS url('http://127.0.0.1:1/a.zip?x={0|1}::m.csv', 'CSV', 'x UInt64');
" $MEMORY

# Loading them again re-reads the definitions with the default `allow_archive_path_syntax`, so all of them
# would be rejected as fresh definitions and only the replay escape keeps them in the catalog. The index-page
# setting is left on so that no arm depends on the gate being skipped for a replayed definition.
echo "--- a stored definition keeps loading"
${CLICKHOUSE_LOCAL} --path="$REPLAY_PATH" --allow_url_wildcard_from_index_pages=1 --query \
    "SELECT name, engine FROM system.tables WHERE database = 'default' ORDER BY name" $MEMORY

# For the table-function carrier the exemption keeps the meaning the definition was written with: a failover
# group next to `::` stays a literal URL read on every reload, not an archive read that the group would make
# unusable. The `ENGINE = URL` carrier above is only covered by the catalog assertion: its path carries a glob
# character, so the engine reads it through the object storage and splits `::` with the server default either
# way, as the engine page documents.
echo "--- a replayed failover carrier keeps its literal meaning"
${CLICKHOUSE_LOCAL} --path="$REPLAY_PATH" --query \
    "SELECT 'plan', countIf(explain ILIKE '%ReadFromURL%') > 0,
                   countIf(explain ILIKE '%ReadFromObjectStorage%') > 0
     FROM (EXPLAIN SELECT count() FROM function_failover_archive)" $MEMORY

# A source without a glob keeps the plain storage on reload: `ENGINE = URL` reads `::` literally when the path
# carries no glob character, and the engine name cannot tell the two candidate storages apart, so the read plan
# is what pins it.
echo "--- and one without a glob keeps the plain storage"
${CLICKHOUSE_LOCAL} --path="$REPLAY_PATH" --query \
    "SELECT 'plan', countIf(explain ILIKE '%ReadFromURL%') > 0,
                   countIf(explain ILIKE '%ReadFromObjectStorage%') > 0
     FROM (EXPLAIN SELECT count() FROM engine_literal_member)" $MEMORY

# A stored definition that carried a custom HTTP method kept `::` literal, so it must not be reinterpreted as
# an archive read: the method would be dropped and the container read over `GET` instead.
echo "--- and one with a custom method keeps its literal meaning"
${CLICKHOUSE_LOCAL} --path="$REPLAY_PATH" --query \
    "SELECT 'plan', countIf(explain ILIKE '%ReadFromURL%') > 0,
                   countIf(explain ILIKE '%ReadFromObjectStorage%') > 0
     FROM (EXPLAIN SELECT count() FROM function_custom_method)" $MEMORY

# A `{a|b}` template in the query is a failover option of the same shard, which a fresh definition refuses, so
# the definition that was written with `::` literal keeps that meaning on reload instead of turning into an
# archive read that the group would make unusable.
echo "--- a query failover group keeps the literal meaning the definition was written with"
${CLICKHOUSE_LOCAL} --path="$REPLAY_PATH" --query \
    "SELECT 'plan', countIf(explain ILIKE '%ReadFromURL%') > 0
     FROM (EXPLAIN SELECT count() FROM function_query_template)" $MEMORY

# A fresh `cluster()` call is not a stored definition, so its inner function runs the same checks as the
# function written directly: the experimental gate is not skipped.
echo "--- while a fresh cluster() call still runs the checks of its inner function"
${CLICKHOUSE_LOCAL} --query \
    "SELECT count() FROM cluster('test_shard_localhost', url('http://127.0.0.1:1/data/**/part*.tsv', 'CSV', 'id UInt64'))" $MEMORY 2>&1 \
    | grep -o 'Wildcard expansion for .url. from HTTP index pages is experimental' | head -1

echo "--- and a fresh definition of the same source is still rejected"
${CLICKHOUSE_LOCAL} --query \
    "CREATE TABLE fresh_member (x UInt64) ENGINE = URL('http://127.0.0.1:1/a.zip::m.csv', 'CSV')" $MEMORY 2>&1 \
    | grep -o 'Archive path syntax is not supported by the .URL. table engine' | head -1
${CLICKHOUSE_LOCAL} --query \
    "CREATE TABLE fresh_failover (x UInt64) AS url('http://127.0.0.1:1/a.zip|http://127.0.0.1:2/b.zip::m.csv', 'CSV', 'x UInt64')" $MEMORY 2>&1 \
    | grep -o 'Archive reads do not support failover patterns' | head -1
