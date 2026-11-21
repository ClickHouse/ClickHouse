#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# A table created `AS file('stdin')` without a structure argument, whose columns are not among the inferred ones, reads its own
# columns from the data that the table function has already read from standard input to infer the columns.

echo '--- CSV'
printf '1,hello\n2,world\n' | $CLICKHOUSE_LOCAL --query "
    CREATE TABLE t (id UInt32, s String) AS file('stdin', 'CSV');
    SELECT * FROM t ORDER BY id;"

echo '--- more data than schema inference reads'
seq 1 100000 | sed 's/$/,x/' | $CLICKHOUSE_LOCAL --query "
    CREATE TABLE t (id UInt32, s String) AS file('stdin', 'CSV');
    SELECT count(), sum(id), min(id), max(id) FROM t;"

echo '--- format detected from the data'
printf '{"a":42,"b":"hello"}\n' | $CLICKHOUSE_LOCAL --query "
    CREATE TABLE t (renamed String, other UInt32 DEFAULT 7) AS file('stdin');
    SELECT renamed = '', other FROM t;"
