#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on Parquet

# The columns of `String` of a data part in Parquet format have the `STRING` logical type, which requires UTF-8,
# only if all their values are valid UTF-8. Otherwise, external applications read them as binary.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_parquet_non_utf8;
    CREATE TABLE t_parquet_non_utf8 (k UInt64, valid String, invalid String, split String, nested Array(String), lc LowCardinality(String))
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_rows_for_parquet_part = 1, storage_policy = 'default', min_bytes_for_full_part_storage = 0;
    -- In the column 'split' the concatenation of the values is valid UTF-8, but the values are not.
    INSERT INTO t_parquet_non_utf8 VALUES (1, 'привет', unhex('FFFE8000'), unhex('D0'), ['a', unhex('FF')], unhex('FF')), (2, 'мир', 'b', unhex('BF'), ['b'], 'c');
"

$CLICKHOUSE_CLIENT -q "SELECT k, valid, hex(invalid), hex(split), arrayMap(x -> hex(x), nested), hex(lc) FROM t_parquet_non_utf8 ORDER BY k"

part_path=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_non_utf8' AND active")
data_file="${part_path}/data.parquet"

$CLICKHOUSE_LOCAL -q "SELECT c.path, c.logical_type FROM file('$data_file', ParquetMetadata) ARRAY JOIN columns AS c ORDER BY c.path"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_parquet_non_utf8"
