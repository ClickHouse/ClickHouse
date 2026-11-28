#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A program of the `q` dialect is compiled before the query is interpreted, and the compilation resolves the names of
# the tables. So the database of the request (the setting `database`) and its external tables must be set up before it:
# a name is looked up in the database of the setting, and an external table comes before a table of the database.

other="${CLICKHOUSE_DATABASE}_other"

$CLICKHOUSE_CLIENT -q "
    DROP DATABASE IF EXISTS ${other};
    CREATE DATABASE ${other};
    CREATE TABLE t (x String) ENGINE = Memory;
    INSERT INTO t VALUES ('the database of the connection');
    CREATE TABLE ${other}.t (x String) ENGINE = Memory;
    INSERT INTO ${other}.t VALUES ('the database of the setting');
    CREATE TABLE ext (x String) ENGINE = Memory;
    INSERT INTO ext VALUES ('the table of the database');
"

echo "--- without the setting"
$CLICKHOUSE_CLIENT --enable_q_dialect 1 --dialect q -q "select from t"

echo "--- with the setting 'database'"
$CLICKHOUSE_CLIENT --enable_q_dialect 1 --dialect q -q "SET database = '${other}';
select from t"

echo "--- an external table"
echo "the external table" | $CLICKHOUSE_CLIENT --enable_q_dialect 1 --dialect q -q "select from ext" \
    --external --file=- --name=ext --structure='x String'

$CLICKHOUSE_CLIENT -q "DROP DATABASE ${other}"
