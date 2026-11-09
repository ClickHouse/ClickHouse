#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# The Iceberg spec stores `decimal(P, S)` in Parquet data files as INT32 for P <= 9, INT64 for P <= 18, and otherwise as
# FIXED_LEN_BYTE_ARRAY of the minimum number of bytes for P. Iceberg Java (Spark) hashes bloom filter lookups of
# FIXED_LEN_BYTE_ARRAY decimals in that width, so a wider array made it skip the row groups of matching rows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LAKE="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${LAKE}"
mkdir -p "${LAKE}"

CH="${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0"
IS_CLOUD=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")

# Physical type, logical type and FIXED_LEN_BYTE_ARRAY width (in bytes, from the statistics) of every leaf of the
# Parquet files matching the glob `$1`.
layout()
{
    ${CH} --query "
        SELECT path, groupUniqArray(physical_type), groupUniqArray(logical_type), groupUniqArray(width) FROM (
            SELECT c.path AS path, c.physical_type AS physical_type, c.logical_type AS logical_type,
                if(c.physical_type = 'FIXED_LEN_BYTE_ARRAY', length(splitByWhitespace(assumeNotNull(rc.statistics.min))), 0) AS width
            FROM file('$1', ParquetMetadata)
            ARRAY JOIN columns AS c, row_groups[1].columns AS rc)
        GROUP BY path ORDER BY path"
}

live_files()
{
    ${CH} --query "SELECT arraySort(groupUniqArray(_file)) FROM t"
}

# The same, for the data files of the current snapshot of table `t`.
live_layout()
{
    local files
    files=$(${CH} --query "SELECT arrayStringConcat(arraySort(groupUniqArray(_file)), ',') FROM t")
    layout "${LAKE}/t/data/{${files}}"
}

echo '--- physical types'
${CH} --query "
    CREATE TABLE t (id Int32, d5 Decimal(5, 2), d9 Decimal(9, 2), d10 Decimal(10, 2), d20 Decimal(20, 2), d38 Decimal(38, 2),
        n6 Nullable(Decimal(6, 2)), s Tuple(x Decimal(5, 2), y Int32))
    ENGINE = IcebergLocal('${LAKE}/t', 'Parquet');
    INSERT INTO t VALUES
        (1, 1.23, 1.23, 1.23, 1.23, 1.23, 1.23, (1.23, 1)),
        (2, -4.56, -4.56, -4.56, -4.56, -4.56, NULL, (-4.56, 2)),
        (3, 999.99, 9999999.99, 99999999.99, 999999999999999999.99, 999999999999999999999999999999999999.99, 9999.99, (999.99, 3)),
        (4, -999.99, -9999999.99, -99999999.99, -999999999999999999.99, -999999999999999999999999999999999999.99, -9999.99, (-999.99, 4)),
        (5, 0, 0, 0, 0, 0, 0, (0, 5));"
live_layout

echo '--- statistics'
${CH} --query "
    SELECT rc.path, trimRight(rc.statistics.min), trimRight(rc.statistics.max)
    FROM file('${LAKE}/t/data/*.parquet', ParquetMetadata) ARRAY JOIN row_groups[1].columns AS rc
    ORDER BY rc.path"

echo '--- round trip'
${CH} --query "SELECT * FROM t ORDER BY id"

echo '--- filtered reads'
for predicate in \
    'd5 = 1.23' \
    'd9 = -9999999.99' \
    "d10 IN (toDecimal64('1.23', 2), toDecimal64('-4.56', 2))" \
    "d20 = toDecimal128('-4.56', 2)" \
    "d38 = toDecimal128('-999999999999999999999999999999999999.99', 2)" \
    'd20 < 0' \
    'n6 = -9999.99' \
    'n6 IS NULL' \
    's.x = 1.23'; do
    ${CH} --input_format_parquet_bloom_filter_push_down=1 --input_format_parquet_filter_push_down=1 --query "
        SELECT \$\$${predicate}\$\$, groupArray(id) FROM (SELECT id FROM t WHERE ${predicate} ORDER BY id)"
done

echo '--- DELETE and OPTIMIZE'
${CH} --query "ALTER TABLE t DELETE WHERE id = 5"
files_before=$(live_files)
# The cloud build runs `OPTIMIZE` as a background compaction.
if [[ "${IS_CLOUD}" = "1" ]]; then
    echo '0'
else
    ${CH} --allow_experimental_iceberg_compaction=1 --query "OPTIMIZE TABLE t"
    ${CH} --query "SELECT hasAny($(live_files), ${files_before})"
fi
live_layout
${CH} --query "SELECT * FROM t ORDER BY id"

echo '--- UPDATE'
files_before=$(live_files)
${CH} --query "ALTER TABLE t UPDATE d5 = -1.11, d10 = -1.11, d20 = -1.11, s = (-1.11, 1) WHERE id = 1"
${CH} --query "SELECT length(arrayFilter(f -> NOT has(${files_before}, f), $(live_files)))"
live_layout
${CH} --query "SELECT id, d5, d10, d20, s FROM t ORDER BY id"

echo '--- a value out of the range of its precision'
${CH} --query "
    CREATE TABLE t_overflow (id Int32, d20 Decimal(20, 2)) ENGINE = IcebergLocal('${LAKE}/t_overflow', 'Parquet');
    INSERT INTO t_overflow VALUES (1, -999999999999999999.99);"
${CH} --query "
    INSERT INTO t_overflow SELECT 2, toDecimal128('99999999999999999999.99', 2) * 1000000 SETTINGS decimal_check_overflow = 0" 2>&1 \
    | grep -o -m1 'DECIMAL_OVERFLOW'
${CH} --query "SELECT * FROM t_overflow ORDER BY id"

echo '--- plain Parquet output is unchanged'
${CH} --query "
    INSERT INTO FUNCTION file('${LAKE}/plain.parquet', Parquet)
    SELECT CAST(1.23 AS Decimal(5, 2)) AS d5, CAST(1.23 AS Decimal(20, 2)) AS d20"
layout "${LAKE}/plain.parquet"

${CH} --query "DROP TABLE t; DROP TABLE t_overflow"
rm -rf "${LAKE}"
