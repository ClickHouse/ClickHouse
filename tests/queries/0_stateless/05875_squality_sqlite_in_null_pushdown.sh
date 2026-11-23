#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_squality_in_null_pushdown.db"
trap 'rm -f "$DB_PATH"' EXIT
rm -f "$DB_PATH"

sqlite3 "$DB_PATH" "
    CREATE TABLE t (x INTEGER NOT NULL) STRICT;
    INSERT INTO t VALUES (1), (2);
"
${CLICKHOUSE_LOCAL} --query="
    SET in_null_as_unknown = 1;
    SELECT x FROM sqlite('$DB_PATH', 't') WHERE isNull(x IN (1, NULL)) ORDER BY x;
    SELECT x FROM VALUES('x Int64', (1), (2)) WHERE isNull(x IN (1, NULL)) ORDER BY x;
"
