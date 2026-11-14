#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: SQLite format is not available in the fasttest environment

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# One async insert batch reuses one SQLite input format for all its entries; each entry must be read from its own database.

url="${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=0&async_insert_use_adaptive_busy_timeout=0&async_insert_busy_timeout_min_ms=100000&async_insert_busy_timeout_max_ms=100000"

DIR=$(mktemp -d "$CLICKHOUSE_TMP/sqlite_async_insert_XXXXXX")
trap 'rm -rf "$DIR"' EXIT

python3 -c "
import sqlite3, sys
def db(name, table, column, *values):
    conn = sqlite3.connect(sys.argv[1] + '/' + name)
    conn.execute(f'CREATE TABLE {table} ({column})')
    conn.executemany(f'INSERT INTO {table} VALUES (?)', [(v,) for v in values])
    conn.commit()
    conn.close()
db('v1', 't', 'x INTEGER', 1, 2)
db('v2', 't', 'x INTEGER', 3, 4)
db('v3', 'other', 'x INTEGER', 5)
db('broken', 't', 'x TEXT', '1', 'abc', '7')
db('typeless', 't', 'x', 2.5)
db('nan', 't', 'x REAL', 'nan')
" "$DIR"

function insert()
{
    ${CLICKHOUSE_CURL} -sS "${url}&query=INSERT+INTO+$1+FORMAT+SQLite" --data-binary @"$DIR/$2"
}

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_valid (x Int64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE t_after_error (x Int64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE t_column_types (x Float64) ENGINE = MergeTree ORDER BY x;
"

# Three databases, the last one with a differently named table.
insert t_valid v1
insert t_valid v2
insert t_valid v3
# The read of the first database fails at its 2nd row, which is not an Int64.
insert t_after_error broken
insert t_after_error v2
# A column without a declared type only accepts numeric storage classes; a REAL column also accepts the TEXT 'nan'.
insert t_column_types typeless
insert t_column_types nan

for table in t_valid t_after_error t_column_types; do
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE ${table}"
    ${CLICKHOUSE_CLIENT} -q "SELECT groupArray(x) FROM (SELECT x FROM ${table} ORDER BY x)"
done

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_valid; DROP TABLE t_after_error; DROP TABLE t_column_types"
