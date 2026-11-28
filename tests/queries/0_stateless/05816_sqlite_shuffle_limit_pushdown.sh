#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `SHUFFLE LIMIT` samples from all rows of a `sqlite` table, so its `LIMIT` is not sent to SQLite, while a plain
# `LIMIT` still is (with an `OFFSET m`, as `LIMIT n + m`).

DB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_shuffle_limit.db"
trap 'rm -f "$DB_PATH"' EXIT
rm -f "$DB_PATH"

sqlite3 "$DB_PATH" "
CREATE TABLE t (id INTEGER PRIMARY KEY);
WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 1000) INSERT INTO t SELECT x FROM c;
"

LOCAL="${CLICKHOUSE_LOCAL} --allow_experimental_shuffle_query=1"
T="sqlite('${DB_PATH}', 't')"

# SQLite returns the rows in `id` order, so a sample of its first 10 rows would have `max(id) = 10`.
${LOCAL} --query "SELECT count(), max(id) > 10 FROM (SELECT id FROM $T SHUFFLE LIMIT 10)"
${LOCAL} --query "SELECT count() FROM (SELECT id FROM $T SHUFFLE LIMIT 5 OFFSET 3)"

${LOCAL} --send_logs_level=trace --query "
SELECT id FROM $T LIMIT 5 FORMAT Null;
SELECT id FROM $T LIMIT 5 OFFSET 3 FORMAT Null;
SELECT id FROM $T SHUFFLE LIMIT 5 FORMAT Null;
SELECT id FROM $T SHUFFLE LIMIT 5 OFFSET 3 FORMAT Null;
" 2>&1 | grep -oE 'Query: SELECT `id` FROM `t`( LIMIT [0-9]+)?$'
