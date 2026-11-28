#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree, no-replicated-database, no-ordinary-database, no-parallel
# Tag no-parallel: `/replicas_status` loads every deferred replicated table on the server, which breaks the other tests
# that check that their replicated tables in databases with `lazy_load_tables` are still stand-ins.
# Tag zookeeper: the page under test reports on replicated tables.
# Tag no-shared-merge-tree: a replica left read-only by an empty Keeper path is a ReplicatedMergeTree state; the private CI rewrites the engine.
# Tag no-replicated-database: a Replicated database assigns table UUIDs itself, so the ATTACH below cannot name one.
# Tag no-ordinary-database: ATTACH TABLE with an explicit UUID needs an Atomic database.

# `/replicas_status` reports the replication lag of every replicated table of the server, and
# `verbose=1` asks for the per-table lines rather than a bare "Ok."
# (docs/guides/oss/deployment-and-scaling/monitoring/monitoring.mdx,
# docs/concepts/features/interfaces/http.mdx). Building that page walks the whole database catalog:
# a database that cannot hold replicated tables is skipped without its tables being resolved, a
# replica that is read-only is reported as read-only instead of being asked for a delay it cannot
# compute, and a query string the form parser refuses is answered by the handler's catch-all rather
# than left without a response.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

STATUS_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/replicas_status"

# A full-definition ATTACH warns on stderr, which says nothing about the page under test.
CLIENT="${CLICKHOUSE_CLIENT} --server_logs_file=/dev/null"

# A `Filesystem` database is external: it holds no ClickHouse tables at all. A relative path
# resolves inside user_files, which the server has already created.
${CLIENT} -q "CREATE DATABASE ${CLICKHOUSE_DATABASE}_fs ENGINE = Filesystem('.')"

# A replica that works, and a replica attached over an empty Keeper path, which stays read-only.
${CLIENT} -q "CREATE TABLE t_live (a UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/live', 'r1') ORDER BY a"
TABLE_UUID=$(${CLIENT} -q "SELECT generateUUIDv4()")
${CLIENT} -q "ATTACH TABLE t_ro UUID '${TABLE_UUID}' (a UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/absent', 'r1') ORDER BY a" 2>/dev/null

# In range: the two tables really are in the two states the page has to tell apart.
${CLIENT} -q "SELECT table, is_readonly FROM system.replicas WHERE database = currentDatabase() ORDER BY table"

body=$(${CLICKHOUSE_CURL} -sS "${STATUS_URL}?verbose=1")

# Each count is scoped to this test's own databases, so no other test's replicated table can
# answer for them and this test disturbs none. The page's status is server-wide and is not
# asserted: a table of another test lagging would flip it.
echo "live replica lines: $(echo "$body" | grep -c "^${CLICKHOUSE_DATABASE}\.t_live:")"
echo "read-only replica lines: $(echo "$body" | grep -c "^${CLICKHOUSE_DATABASE}\.t_ro:.*is readonly")"
echo "external database lines: $(echo "$body" | grep -c "^${CLICKHOUSE_DATABASE}_fs\.")"

# More form fields than `http_max_fields` allows: the query string is refused before the page is
# built, and the catch-all answers with the server-error status instead of no answer at all.
too_many_fields=$(printf 'a=1&%.0s' {1..1100})
echo "refused request status: $(${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{http_code}' "${STATUS_URL}?${too_many_fields}")"

${CLIENT} -q "DROP TABLE t_ro SYNC"
${CLIENT} -q "DROP TABLE t_live SYNC"
${CLIENT} -q "DROP DATABASE ${CLICKHOUSE_DATABASE}_fs"
