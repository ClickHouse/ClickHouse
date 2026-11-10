#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: depends on Parquet

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `COLUMNS` matchers and lambda bodies in the SELECT of INSERT SELECT refer to columns of the table function,
# so the structure of the insertion table must not be used as a partial structure hint for it.
# https://github.com/ClickHouse/ClickHouse/issues/70795
# https://github.com/ClickHouse/ClickHouse/issues/75145

CSV_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.csv"
PARQUET_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
STRING_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}_string.csv"

$CLICKHOUSE_CLIENT -q "
INSERT INTO FUNCTION file('$CSV_FILE', CSVWithNames, 'x Int16, y Int16') SELECT 1, 2 SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file('$PARQUET_FILE', Parquet, '\`0\` Int64, \`1\` Int64, other String') SELECT 1, 2, 'a' SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file('$STRING_FILE', LineAsString) SETTINGS engine_file_truncate_on_insert = 1 VALUES ('s,n'), ('0123,1');
"

$CLICKHOUSE_CLIENT -q "
SET use_structure_from_insertion_table_in_table_functions = 2;

DROP TABLE IF EXISTS t_05511;
DROP TABLE IF EXISTS t_05511_parquet;
DROP TABLE IF EXISTS t_05511_string;
CREATE TABLE t_05511 (x Int16, y Int16) ENGINE = Memory;
CREATE TABLE t_05511_parquet (other String, arr Array(Int64)) ENGINE = Memory;
CREATE TABLE t_05511_string (s String, arr Array(UInt8)) ENGINE = Memory;

SELECT 'COLUMNS regexp inside a function';
INSERT INTO t_05511 SELECT x, coalesce(COLUMNS('y'), 0) FROM file('$CSV_FILE', CSVWithNames);
SELECT * FROM t_05511;
TRUNCATE TABLE t_05511;

SELECT 'COLUMNS list inside a function';
INSERT INTO t_05511 SELECT x, coalesce(COLUMNS(y), 0) FROM file('$CSV_FILE', CSVWithNames);
SELECT * FROM t_05511;
TRUNCATE TABLE t_05511;

SELECT 'COLUMNS regexp';
INSERT INTO t_05511 SELECT x, COLUMNS('y') FROM file('$CSV_FILE', CSVWithNames);
SELECT * FROM t_05511;
TRUNCATE TABLE t_05511;

SELECT 'lambda';
INSERT INTO t_05511 SELECT x, arrayMap(a -> a + y, [0])[1] FROM file('$CSV_FILE', CSVWithNames);
SELECT * FROM t_05511;
TRUNCATE TABLE t_05511;

SELECT 'lambda using only its own argument keeps the structure of the insertion table';
INSERT INTO t_05511_string SELECT s, arrayMap(a -> a * 2, [1]) FROM file('$STRING_FILE', CSVWithNames);
SELECT * FROM t_05511_string;

SELECT 'identifier inside a function';
INSERT INTO t_05511 SELECT x, coalesce(y, 0) FROM file('$CSV_FILE', CSVWithNames);
SELECT * FROM t_05511;
TRUNCATE TABLE t_05511;

SELECT 'count(*)';
INSERT INTO t_05511 SELECT x, toInt16(count(*)) FROM file('$CSV_FILE', CSVWithNames) GROUP BY x;
SELECT * FROM t_05511;
TRUNCATE TABLE t_05511;

SELECT 'asterisk';
INSERT INTO t_05511 SELECT * FROM file('$CSV_FILE', CSVWithNames);
SELECT * FROM t_05511;
TRUNCATE TABLE t_05511;

SELECT 'asterisk with EXCEPT';
INSERT INTO t_05511 (x) SELECT * EXCEPT y FROM file('$CSV_FILE', CSVWithNames);
SELECT * FROM t_05511;
TRUNCATE TABLE t_05511;

SELECT 'Parquet';
INSERT INTO t_05511_parquet SELECT other, array(COLUMNS('^[0-9]{1,3}\$')) FROM file('$PARQUET_FILE');
SELECT * FROM t_05511_parquet;

DROP TABLE t_05511;
DROP TABLE t_05511_parquet;
DROP TABLE t_05511_string;
"

rm -f "${USER_FILES_PATH:?}/$CSV_FILE" "${USER_FILES_PATH:?}/$PARQUET_FILE" "${USER_FILES_PATH:?}/$STRING_FILE"
