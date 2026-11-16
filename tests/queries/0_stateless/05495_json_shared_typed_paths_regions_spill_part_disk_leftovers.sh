#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-distributed-cache
# no-parallel: the failpoint is global and would break the spills of the other tests
# no-fasttest: needs S3
# no-distributed-cache: the temporary storage is the distributed cache, which the spiller does not use

# Spills of a part write to the part's disk go to a temporary directory of the table, which is removed when the write
# ends: after a merge and a mutation that commit their parts, after a merge that fails in the query, and after a
# mutation that fails in the background and is killed. Checked for a horizontal merge on a local disk and a vertical
# merge on an S3 disk: no temporary directory is left in the table's directory, and the data stays intact.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_mem;
    CREATE TABLE t_mem (x UInt64, json JSON(a UInt64, b String, c Array(UInt8))) ENGINE = Memory;
    INSERT INTO t_mem SELECT number, concat('{\"a\":', toString(number), ',\"b\":\"', repeat('b', number % 11), '\",\"c\":', toString(range(number % 5)), '}') FROM numbers(100);
"

function wait_for()
{
    local query=$1
    for _ in {1..600}; do
        [ "$($CLICKHOUSE_CLIENT -q "$query")" = 1 ] && return
        sleep 0.1
    done
    echo "Timed out waiting for: $query"
}

function run()
{
    local table=$1
    local settings=$2

    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS $table;
        CREATE TABLE $table (x UInt64, json JSON(SHARED a UInt64, SHARED b String, SHARED c Array(UInt8)))
        ENGINE = MergeTree ORDER BY x
        SETTINGS $settings, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 3,
                 index_granularity_bytes = 10485760, merge_max_block_size = 5, object_typed_shared_data_serialization_version = 'v2',
                 serialization_spill_max_bytes_to_buffer = 1, serialization_spill_to_part_disk_only = 1;
        INSERT INTO $table SELECT * FROM t_mem WHERE x < 50;
        INSERT INTO $table SELECT * FROM t_mem WHERE x >= 50;
        OPTIMIZE TABLE $table FINAL;
        ALTER TABLE $table UPDATE json = json WHERE 1 SETTINGS mutations_sync = 2;

        SYSTEM ENABLE FAILPOINT serialization_spill_fail_part_disk;
        OPTIMIZE TABLE $table FINAL; -- { serverError FAULT_INJECTED }
        ALTER TABLE $table UPDATE json = json WHERE 1;
    "

    wait_for "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = '$table' AND NOT is_done AND latest_fail_reason LIKE '%FAULT_INJECTED%'"
    $CLICKHOUSE_CLIENT -q "KILL MUTATION WHERE database = currentDatabase() AND table = '$table' SYNC FORMAT Null"
    wait_for "SELECT count() = 0 FROM system.merges WHERE database = currentDatabase() AND table = '$table'"

    $CLICKHOUSE_CLIENT -q "
        SYSTEM DISABLE FAILPOINT serialization_spill_fail_part_disk;

        SYSTEM FLUSH LOGS part_log;
        SELECT '$table', 'merge spilled', max(ProfileEvents['SerializationSpilledToPartDisk'] > 0), any(merge_algorithm)
        FROM system.part_log WHERE database = currentDatabase() AND table = '$table' AND event_type = 'MergeParts' AND error = 0;
        SELECT '$table', 'mutation spilled', max(ProfileEvents['SerializationSpilledToPartDisk'] > 0)
        FROM system.part_log WHERE database = currentDatabase() AND table = '$table' AND event_type = 'MutatePart' AND error = 0;
        SELECT '$table', 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = '$table' AND active;
        CHECK TABLE $table SETTINGS check_query_single_value_result = 1;
        SELECT '$table', 'data', (SELECT arraySort(groupArray((x, toString(json)))) FROM $table) = (SELECT arraySort(groupArray((x, toString(json)))) FROM t_mem);
    "

    local data_path
    data_path=$($CLICKHOUSE_CLIENT -q "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = '$table'")
    echo "$table temporary directories left: $(find "$data_path" -mindepth 1 -maxdepth 1 \( -name 'tmp*' -o -name 'delete_tmp*' \) | wc -l)"

    $CLICKHOUSE_CLIENT -q "DROP TABLE $table"
}

run t_local_horizontal "enable_vertical_merge_algorithm = 0"
run t_s3_vertical "disk = 's3_disk', enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_mem"
