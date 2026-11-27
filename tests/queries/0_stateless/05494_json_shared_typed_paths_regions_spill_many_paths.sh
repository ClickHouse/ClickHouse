#!/usr/bin/env bash
# Tags: no-distributed-cache
# no-distributed-cache: the temporary storage is the distributed cache, which the spiller does not use

# A merge of a JSON column with 1000 SHARED paths over a small memory budget spills the largest substreams of many
# paths, compared with a Memory table: the data, the subcolumns, and the spill itself.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

shared_paths=$(seq 0 999 | awk '{printf "%sSHARED p%d UInt64", (NR > 1 ? ", " : ""), $1}')
typed_paths=$(seq 0 999 | awk '{printf "%sp%d UInt64", (NR > 1 ? ", " : ""), $1}')

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_many_mem;
    DROP TABLE IF EXISTS t_many;
    CREATE TABLE t_many_mem (x UInt64, json JSON($typed_paths)) ENGINE = Memory;
    CREATE TABLE t_many (x UInt64, json JSON($shared_paths))
    ENGINE = MergeTree ORDER BY x
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 16, index_granularity_bytes = '10Mi',
             enable_vertical_merge_algorithm = 0, merge_max_block_size = 7, object_typed_shared_data_serialization_version = 'v2',
             serialization_spill_max_bytes_to_buffer = 1048576;
"

# Each row holds every path, so a granule of every path grows every buffer, about 2.4 MB of them in all.
src="SELECT number AS x, toJSONString(mapFromArrays(arrayMap(i -> concat('p', toString(i)), range(1000)), arrayMap(i -> number * 1000 + i, range(1000)))) AS json FROM numbers(300)"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO t_many_mem SELECT x, json FROM ($src);
    INSERT INTO t_many SELECT x, json FROM ($src) WHERE x % 4 = 0;
    INSERT INTO t_many SELECT x, json FROM ($src) WHERE x % 4 = 1;
    INSERT INTO t_many SELECT x, json FROM ($src) WHERE x % 4 = 2;
    INSERT INTO t_many SELECT x, json FROM ($src) WHERE x % 4 = 3;
    OPTIMIZE TABLE t_many FINAL;
"

$CLICKHOUSE_CLIENT -q "
    SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_many' AND active;
    SELECT 'json', (SELECT sum(cityHash64(x, toString(json))) FROM t_many) = (SELECT sum(cityHash64(x, toString(json))) FROM t_many_mem);
    SELECT 'subcolumns', (SELECT sum(cityHash64(x, json.p0, json.p500, json.p999)) FROM t_many) = (SELECT sum(cityHash64(x, json.p0, json.p500, json.p999)) FROM t_many_mem);
    SYSTEM FLUSH LOGS part_log;
    SELECT 'spilled', ProfileEvents['SerializationSpilledSegments'] > 0
    FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_many' AND event_type = 'MergeParts'
        AND part_name IN (SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_many' AND active);
    CHECK TABLE t_many SETTINGS check_query_single_value_result = 1;
    DROP TABLE t_many_mem;
    DROP TABLE t_many;
"
