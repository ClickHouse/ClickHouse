#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-replicated-database: a refreshable materialized view in a `Replicated` database rejects a non-replicated target.

# A row policy or `PREWHERE` on a `Merge` table or a materialized view over a data lake table must be checked
# against the snapshot the read uses: after another writer widens `k` to `Int64` and inserts a row, the read
# must see that row, and the filter must not be pushed into it as built for `Int32`. Each case has its own
# table, so its read is the first after its own widening. `t_writer*` is a second table object over the same
# Iceberg table, standing in for an external writer.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_LAZY="${CLICKHOUSE_DATABASE}_lazy"

drop_policies()
{
    $CLICKHOUSE_CLIENT -m -q "
    DROP ROW POLICY IF EXISTS p1 ON ${CLICKHOUSE_DATABASE}.t_merge1;
    DROP ROW POLICY IF EXISTS p3 ON ${CLICKHOUSE_DATABASE}.mv3;
    DROP ROW POLICY IF EXISTS p4 ON ${CLICKHOUSE_DATABASE}.rmv4;
    DROP ROW POLICY IF EXISTS p5 ON ${CLICKHOUSE_DATABASE}.t_tf5;
    DROP ROW POLICY IF EXISTS p6 ON ${DB_LAZY}.t_merge;
    "
}
trap 'drop_policies; $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${DB_LAZY}"; rm -rf "${DIR}"_*' EXIT

CLIENT="$CLICKHOUSE_CLIENT --allow_insert_into_iceberg 1 --allow_experimental_iceberg_compaction 0 --iceberg_delete_data_on_drop 0"

for i in 1 2 3 4 5; do
    $CLIENT -m -q "
    CREATE TABLE t_lake$i (k Int32) ENGINE = IcebergLocal('${DIR}_$i/', 'Parquet');
    INSERT INTO t_lake$i SELECT number FROM numbers(100);
    CREATE TABLE t_writer$i ENGINE = IcebergLocal('${DIR}_$i/', 'Parquet');
    "
done

$CLIENT -m -q "
CREATE TABLE t_merge1 ENGINE = Merge(currentDatabase(), '^t_lake1\$');
CREATE ROW POLICY p1 ON t_merge1 USING -k > -10 TO ALL;
SELECT count() FROM t_lake1 FORMAT Null;
ALTER TABLE t_writer1 MODIFY COLUMN k Int64;
INSERT INTO t_writer1 VALUES (5);
SELECT 'Merge, row policy', count(), sum(k) FROM t_merge1;

CREATE TABLE t_merge2 ENGINE = Merge(currentDatabase(), '^t_lake2\$');
SELECT count() FROM t_lake2 FORMAT Null;
ALTER TABLE t_writer2 MODIFY COLUMN k Int64;
SELECT 'Merge, PREWHERE', count(), sum(k) FROM t_merge2 PREWHERE -k > -10; -- { serverError ILLEGAL_PREWHERE }

CREATE TABLE src (k Int32) ENGINE = Null;
CREATE MATERIALIZED VIEW mv3 TO t_lake3 (k Int32) AS SELECT k FROM src;
CREATE ROW POLICY p3 ON mv3 USING -k > -10 TO ALL;
SELECT count() FROM t_lake3 FORMAT Null;
ALTER TABLE t_writer3 MODIFY COLUMN k Int64;
INSERT INTO t_writer3 VALUES (5);
SELECT 'materialized view, row policy', count(), sum(k) FROM mv3;

CREATE MATERIALIZED VIEW rmv4 REFRESH EVERY 1 YEAR APPEND TO t_lake4 (k Int32) EMPTY AS SELECT k FROM src;
CREATE ROW POLICY p4 ON rmv4 USING -k > -10 TO ALL;
SELECT count() FROM t_lake4 FORMAT Null;
ALTER TABLE t_writer4 MODIFY COLUMN k Int64;
INSERT INTO t_writer4 VALUES (5);
SELECT 'refreshable materialized view, row policy', count(), sum(k) FROM rmv4;

CREATE TABLE t_tf5 AS merge(currentDatabase(), '^t_lake5\$');
CREATE ROW POLICY p5 ON t_tf5 USING -k > -10 TO ALL;
SELECT count() FROM t_lake5 FORMAT Null;
SELECT count() FROM t_tf5 FORMAT Null;
ALTER TABLE t_writer5 MODIFY COLUMN k Int64;
INSERT INTO t_writer5 VALUES (5);
SELECT 'table AS merge(), row policy', count(), sum(k) FROM t_tf5;
"

# In a database with `lazy_load_tables`, `DETACH` / `ATTACH DATABASE` installs a stand-in that reports
# the columns of its `CREATE` query until it is loaded.
$CLIENT -m -q "
DROP DATABASE IF EXISTS ${DB_LAZY};
CREATE DATABASE ${DB_LAZY} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE ${DB_LAZY}.t_lake (k Int32) ENGINE = IcebergLocal('${DIR}_6/', 'Parquet');
INSERT INTO ${DB_LAZY}.t_lake SELECT number FROM numbers(100);
CREATE TABLE ${DB_LAZY}.t_writer ENGINE = IcebergLocal('${DIR}_6/', 'Parquet');
CREATE TABLE ${DB_LAZY}.t_merge ENGINE = Merge('${DB_LAZY}', '^t_lake\$');
CREATE ROW POLICY p6 ON ${DB_LAZY}.t_merge USING -k > -10 TO ALL;
ALTER TABLE ${DB_LAZY}.t_writer MODIFY COLUMN k Int64;
INSERT INTO ${DB_LAZY}.t_writer VALUES (5);
DETACH DATABASE ${DB_LAZY};
ATTACH DATABASE ${DB_LAZY};
SELECT 'lazy Merge, row policy', count(), sum(k) FROM ${DB_LAZY}.t_merge;
"

# A child that cannot be refreshed (here: its newest metadata file is corrupt) fails only a query that reads it,
# and is not read from its old snapshot.
$CLIENT -m -q "
CREATE TABLE t_lake7 (k Int32) ENGINE = IcebergLocal('${DIR}_7/', 'Parquet');
INSERT INTO t_lake7 VALUES (1);
CREATE TABLE t_ok7 (k Int32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_ok7 VALUES (2);
CREATE TABLE t_merge7 (k Int32) ENGINE = Merge(currentDatabase(), '^t_(lake|ok)7\$');
CREATE MATERIALIZED VIEW mv7 TO t_lake7 (k Int32) AS SELECT k FROM src;
CREATE MATERIALIZED VIEW mv7_definer TO t_merge7 (k Int32) DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT k FROM src;
"
echo '{' > "${DIR}_7/metadata/v99.metadata.json"
$CLIENT -m -q "
SELECT 'broken child skipped by _table', count(), sum(k) FROM t_merge7 WHERE _table = 't_ok7';
DESCRIBE TABLE t_merge7;
DESCRIBE TABLE mv7;
SELECT count(), sum(k) FROM t_merge7; -- { serverError INCORRECT_DATA }
SELECT count(), sum(k) FROM mv7; -- { serverError INCORRECT_DATA }
SELECT count(), sum(k) FROM mv7_definer; -- { serverError INCORRECT_DATA }
"

# A read of `Merge` tables that read each other fails with `TOO_DEEP_RECURSION`, as any cycle does, without walking
# every path through them first, and their `DESCRIBE` still works.
$CLIENT -m -q "
CREATE TABLE cyc8 (k Int32) ENGINE = Memory;
CREATE TABLE cyc8_m1 (k Int32) ENGINE = Merge(currentDatabase(), '^cyc8');
CREATE TABLE cyc8_m2 (k Int32) ENGINE = Merge(currentDatabase(), '^cyc8');
CREATE TABLE cyc8_m3 (k Int32) ENGINE = Merge(currentDatabase(), '^cyc8');
DESCRIBE TABLE cyc8_m1;
SELECT count() FROM cyc8_m1; -- { serverError TOO_DEEP_RECURSION }
DROP TABLE cyc8_m1;
DROP TABLE cyc8_m2;
DROP TABLE cyc8_m3;
"
