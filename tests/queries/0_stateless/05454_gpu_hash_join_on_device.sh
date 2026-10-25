#!/usr/bin/env bash

# Checks `gpu_hash_join_on_device`: a `gpu_hash` join whose columns of both sides, all of a fixed width, are on the device,
# and whose result is gathered there, returns what the CPU returns - by itself, in a chain of joins that hand their
# rows over on the device, under an aggregation, with an empty right table, with keys that match many rows, and in
# blocks smaller than a probe's result, with the tables read onto the device and the aggregations on it. A join with a
# `String` column is joined with only its keys on the device.
#
# Where the build has no GPU support, or the machine no usable device, the settings are not applied and both sides of
# every comparison run on the CPU - the checks hold trivially.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The device takes a join on the estimates the statistics of the tables give, made as the rows are inserted.
$CLICKHOUSE_CLIENT --materialize_statistics_on_insert 1 --query "
    CREATE TABLE gpu_dj_fact (k UInt32, d UInt32, v Float64, q Int32, day Date, code FixedString(4), s String)
    ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'basic, uniq_v2';
    CREATE TABLE gpu_dj_dim (k UInt32, w Int64, day2 Date, f Float32)
    ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'basic, uniq_v2';
    CREATE TABLE gpu_dj_dim2 (d UInt32, n UInt16)
    ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'basic, uniq_v2';
    CREATE TABLE gpu_dj_empty (k UInt32, w Int64)
    ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'basic, uniq_v2';

    INSERT INTO gpu_dj_fact
    SELECT number % 20000, number % 300, number / 8, -number, toDate('2020-01-01') + number % 1000,
           toFixedString(toString(number % 9999), 4), toString(number % 17)
    FROM numbers(200000);
    -- Each key of the dimension once, and every eighth one three times, so that some rows match several.
    INSERT INTO gpu_dj_dim SELECT number * 8, number * 3, toDate('2021-01-01') + number % 100, number / 4 FROM numbers(2500);
    INSERT INTO gpu_dj_dim SELECT number * 64, number, toDate('2022-01-01'), 0.5 FROM numbers(312);
    INSERT INTO gpu_dj_dim SELECT number * 64, -number, toDate('2023-01-01'), 1.5 FROM numbers(312);
    INSERT INTO gpu_dj_dim2 SELECT number * 4, number % 7 FROM numbers(75);
"

GPU_SETTINGS=()
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name = 'USE_GPU'")" == "1" ]; then
    # A device that fails for any other reason than being absent is a bug, and is reported as one rather than being
    # compared with itself on the CPU.
    PROBE_ERROR=$($CLICKHOUSE_CLIENT --join_algorithm gpu_hash \
        --query "SELECT count() FROM gpu_dj_fact AS l INNER JOIN gpu_dj_dim AS r ON l.k = r.k" 2>&1 > /dev/null)
    if [ -z "$PROBE_ERROR" ]; then
        # The joins are on the GPU whatever their estimated sizes, as `gpu_hash` listed alone asks, and without the
        # runtime filters the test harness may turn on.
        # The reads go onto the device and the aggregations run there too, so that the rows of a query stay on the device
        # from its reads to its aggregation.
        GPU_SETTINGS=(--join_algorithm gpu_hash --gpu_hash_join_on_device 1 --enable_join_runtime_filters 0
                      --gpu_read_to_device 1 --allow_experimental_gpu_aggregation 1)
    elif [[ "$PROBE_ERROR" != *"There is no GPU"* ]]; then
        echo "$PROBE_ERROR"
    fi
fi

function compare_with_cpu()
{
    local query="$1"
    local on_cpu
    local on_gpu

    on_cpu=$($CLICKHOUSE_CLIENT --query "$query")
    on_gpu=$($CLICKHOUSE_CLIENT "${GPU_SETTINGS[@]}" --query "$query")

    if [ "$on_cpu" == "$on_gpu" ]; then
        echo "ok"
    else
        echo "MISMATCH for '$query': $on_cpu on the CPU, $on_gpu on the GPU"
    fi
}

FIXED="l.k, l.d, l.v, l.q, l.day, l.code"

# Every column of either side, and keys that match several rows of the right table.
compare_with_cpu "SELECT sum(cityHash64($FIXED, r.k, r.w, r.day2, r.f)), count() FROM gpu_dj_fact AS l INNER JOIN gpu_dj_dim AS r ON l.k = r.k"
compare_with_cpu "SELECT $FIXED, r.w, r.day2, r.f FROM gpu_dj_fact AS l INNER JOIN gpu_dj_dim AS r ON l.k = r.k ORDER BY l.q LIMIT 5"
# A chain of joins, each taking the one below's rows on the device, under an aggregation.
compare_with_cpu "SELECT r2.n, count(), sum(l.v * r.w) FROM gpu_dj_fact AS l INNER JOIN gpu_dj_dim AS r ON l.k = r.k INNER JOIN gpu_dj_dim2 AS r2 ON l.d = r2.d GROUP BY r2.n ORDER BY r2.n"
# A `GROUP BY` of a date and an integer over the joined rows on the device.
compare_with_cpu "SELECT l.day, r.w % 5 AS m, count(), sum(l.v), max(r.f) FROM gpu_dj_fact AS l INNER JOIN gpu_dj_dim AS r ON l.k = r.k GROUP BY l.day, m ORDER BY l.day, m LIMIT 7"
# With runtime filters: the device filters the rows of the left table by the right table's keys as it reads them, and
# waits for the right table to be read first.
compare_with_cpu "SELECT count(), sum(l.v), sum(r.w) FROM gpu_dj_fact AS l INNER JOIN gpu_dj_dim AS r ON l.k = r.k WHERE r.w % 3 = 0 SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0"
# Results larger than the blocks the join hands out.
compare_with_cpu "SELECT sum(cityHash64($FIXED, r.w)), count() FROM gpu_dj_fact AS l INNER JOIN gpu_dj_dim AS r ON l.k = r.k SETTINGS max_joined_block_size_rows = 777, max_block_size = 5000"
# An empty right table.
compare_with_cpu "SELECT count(), sum(r.w) FROM gpu_dj_fact AS l INNER JOIN gpu_dj_empty AS r ON l.k = r.k"
# A `String` column: only the keys are on the device.
compare_with_cpu "SELECT l.s, count(), sum(r.w) FROM gpu_dj_fact AS l INNER JOIN gpu_dj_dim AS r ON l.k = r.k GROUP BY l.s ORDER BY l.s"

$CLICKHOUSE_CLIENT --query "DROP TABLE gpu_dj_fact; DROP TABLE gpu_dj_dim; DROP TABLE gpu_dj_dim2; DROP TABLE gpu_dj_empty"
