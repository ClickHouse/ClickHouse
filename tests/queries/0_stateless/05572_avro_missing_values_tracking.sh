#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Avro format is not available in the fast test build.

# The rows of a column that the file does not have a value for (a column absent from the file, the Avro null of a union
# read as the default of a non-`Nullable` type) take the `DEFAULT` expression of the column on INSERT. The Rust decoder
# tracks these rows per column, and tracks nothing for a column that the file always fills, so the rows
# that it reports have to be exactly these, in every batch of every block: a value that is present must never be
# replaced by a default, even when it equals the default of the type.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"
trap 'rm -rf "$WORK_DIR"' EXIT

ROWS=20000

# Several blocks (the sync interval is large enough that the 20000 rows fall in a few blocks), and each of them is decoded in more than one batch.
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${WORK_DIR}/f.avro', Avro, 'p0 Int64, p1 Int64, u Nullable(Int64), s String, w Nullable(String)')
    SELECT number, -number, if(number % 3 = 0, NULL, number * 10), toString(number), if(number % 5 = 0, NULL, toString(number))
    FROM numbers(${ROWS})
    SETTINGS output_format_avro_sync_interval = 1000000, engine_file_truncate_on_insert = 1"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_present"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_missing"

# Every column is in the file and every row has a value: nothing is replaced, the value 0 and the empty string included.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_present (p0 Int64 DEFAULT 123456789012, p1 Int64 DEFAULT 123456789012, s String DEFAULT 'default') ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_present (p0, p1, s) FORMAT Avro" < "${WORK_DIR}/f.avro"

echo "present"
${CLICKHOUSE_CLIENT} --query "
    WITH
        (SELECT (count(), sum(p0), sum(p1), sum(toInt64(s)), countIf(p0 = 123456789012), countIf(s = 'default')) FROM t_present) AS actual,
        (SELECT (count(), sum(number), -sum(number), sum(number), 0, 0) FROM numbers(${ROWS})) AS expected
    SELECT actual = expected"

# Columns that the file has not (x, e), the null of a union as a non-Nullable value (u, w), and columns that it always has (p0, s).
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_missing (p0 Int64 DEFAULT 123456789012, u Int64 DEFAULT 7, x Int64 DEFAULT 42, e String DEFAULT 'e', w String DEFAULT 'none', s String DEFAULT 'default')
    ENGINE = Memory"
${CLICKHOUSE_CLIENT} --input_format_avro_allow_missing_fields=1 --query "INSERT INTO t_missing (p0, u, x, e, w, s) FORMAT Avro" < "${WORK_DIR}/f.avro"

# The rows that were replaced are the rows that held the null, and the others keep their values.
echo "missing"
${CLICKHOUSE_CLIENT} --query "
    WITH
        (
            SELECT (
                count(), sum(p0), sum(x), countIf(e = 'e'), sum(toInt64(s)),
                countIf(u = 7), sumIf(p0, u = 7), sumIf(u, u != 7),
                countIf(w = 'none'), sumIf(p0, w = 'none'), sumIf(toInt64OrZero(w), w != 'none'))
            FROM t_missing
        ) AS actual,
        (
            SELECT (
                count(), sum(number), 42 * count(), count(), sum(number),
                countIf(number % 3 = 0), sumIf(number, number % 3 = 0), sumIf(number * 10, number % 3 != 0),
                countIf(number % 5 = 0), sumIf(number, number % 5 = 0), sumIf(number, number % 5 != 0))
            FROM numbers(${ROWS})
        ) AS expected
    SELECT actual = expected"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_present"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_missing"
