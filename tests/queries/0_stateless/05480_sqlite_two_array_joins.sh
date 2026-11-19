#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

# A query with more than one ARRAY JOIN over an external database table must work, and its LIMIT must not be
# pushed to the external database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_two_array_joins.db"
trap 'rm -f "$DB_PATH"' EXIT
rm -f "$DB_PATH"

# The first two rows give no output through `ARRAY JOIN range(id)`.
sqlite3 "$DB_PATH" 'CREATE TABLE t (id INTEGER); INSERT INTO t VALUES (0), (0), (3);'

# The LIMIT push-down is what inspects the ARRAY JOINs.
LOCAL="${CLICKHOUSE_LOCAL} --external_storage_push_down_limit=1"
T="sqlite('${DB_PATH}', 't')"

echo 'two ARRAY JOINs'
${LOCAL} --query "SELECT id, a, b FROM $T ARRAY JOIN range(id) AS a ARRAY JOIN [1, 2] AS b ORDER BY ALL"
echo 'LEFT ARRAY JOIN and ARRAY JOIN'
${LOCAL} --query "SELECT id, a, b FROM $T LEFT ARRAY JOIN range(id) AS a ARRAY JOIN [1] AS b ORDER BY ALL"
echo 'one ARRAY JOIN with LIMIT'
${LOCAL} --query "SELECT count() FROM (SELECT id, a FROM $T ARRAY JOIN range(id) AS a LIMIT 2)"
echo 'two ARRAY JOINs with LIMIT'
${LOCAL} --query "SELECT count() FROM (SELECT id, a, b FROM $T ARRAY JOIN range(id) AS a ARRAY JOIN [1, 2] AS b LIMIT 2)"
echo 'query sent to SQLite'
# shellcheck disable=SC2016
${LOCAL} --send_logs_level=trace --query "SELECT id, a, b FROM $T ARRAY JOIN range(id) AS a ARRAY JOIN [1, 2] AS b LIMIT 2 FORMAT Null" 2>&1 \
    | grep -oE 'Query: SELECT .* FROM `t`.*$'
