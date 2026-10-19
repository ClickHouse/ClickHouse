#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on Parquet

# The data file of a data part in Parquet format can be read by an external application.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_parquet_external;
    CREATE TABLE t_parquet_external
    (
        key UInt64,
        s String CODEC(ZSTD(5)),
        x Int64 CODEC(Delta, LZ4),
        f Float64 CODEC(Gorilla),
        v Variant(UInt64, String),
        n Nested(a UInt32, b String)
    )
    ENGINE = MergeTree ORDER BY key
    SETTINGS min_rows_for_parquet_part = 1, parquet_row_group_granularity_rows = 300, storage_policy = 'default', min_bytes_for_full_part_storage = 0, default_compression_codec = 'LZ4';
    INSERT INTO t_parquet_external SELECT number, toString(number), number * 3, number / 7, if(number % 2, number::Variant(UInt64, String), toString(number)::Variant(UInt64, String)), [number, number + 1], ['a', 'b'] FROM numbers(1000);
"

part_path=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_external' AND active")
data_file="${part_path}/data.parquet"

echo "data in the file"
$CLICKHOUSE_LOCAL -q "SELECT count(), sum(key), sum(length(s)), sum(x), round(sum(f), 3), sum(length(n.a)) FROM file('$data_file', Parquet)"
$CLICKHOUSE_LOCAL -q "SELECT key, s, x, \`n.a\`, \`n.b\` FROM file('$data_file', Parquet) ORDER BY key LIMIT 3"

echo "row groups"
$CLICKHOUSE_LOCAL -q "SELECT num_row_groups, num_rows FROM file('$data_file', ParquetMetadata)"
$CLICKHOUSE_LOCAL -q "SELECT arrayJoin(row_groups).num_rows FROM file('$data_file', ParquetMetadata)"

echo "compression and encodings"
$CLICKHOUSE_LOCAL -q "
    SELECT c.path, c.physical_type, c.compression, has(c.encodings, 'DELTA_BINARY_PACKED'), has(c.encodings, 'BYTE_STREAM_SPLIT')
    FROM file('$data_file', ParquetMetadata) ARRAY JOIN columns AS c"

echo "columns metadata"
grep -a -o '"encoding":"[a-z]*"' "$data_file"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_parquet_external"
