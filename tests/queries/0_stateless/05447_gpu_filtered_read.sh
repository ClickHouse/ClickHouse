#!/usr/bin/env bash

# Checks `allow_experimental_gpu_filtered_read`: a read whose `PREWHERE` the device evaluates, as far as it can, and
# sends back only the rows it keeps. The conjuncts the device leaves - over strings - are evaluated on the CPU over the
# rows sent back, and a conjunct the device evaluates that a filter above takes as a column is true for every one of
# them. The results are those of the CPU, and the plans say where the read is filtered.
#
# Where the build has no GPU support, or the machine no usable device, the setting is not applied and both sides of
# every comparison run on the CPU - the checks hold trivially.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE gpu_filtered_read (k UInt32, d Date, c Date, r Date, m String, p Float64, q Int64, t DateTime('UTC'))
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1,
        serialization_info_version = 'with_types', string_serialization_version = 'with_size_stream',
        auto_statistics_types = 'basic, uniq_v2';

    INSERT INTO gpu_filtered_read
    SELECT number, toDate('1992-01-01') + number % 2500, toDate('1992-01-01') + (number * 7) % 2500,
           toDate('1992-01-01') + (number * 13) % 2500, ['MAIL', 'SHIP', 'AIR', 'RAIL', 'TRUCK', 'FOB', 'REG AIR'][number % 7 + 1],
           number / 3, -number, toDateTime('1994-01-01 00:00:00', 'UTC') + number * 37
    FROM numbers(300007);
"

GPU_SETTINGS=()
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name = 'USE_GPU'")" == "1" ]; then
    # A device that fails for any other reason than being absent is a bug, and is reported as one rather than being
    # compared with itself on the CPU.
    PROBE_ERROR=$($CLICKHOUSE_CLIENT --allow_experimental_gpu_filtered_read 1 \
        --query "SELECT count() FROM gpu_filtered_read WHERE d < '1993-01-01' AND q < 0" 2>&1 > /dev/null)
    if [ -z "$PROBE_ERROR" ]; then
        # The plans checked below take the WHERE moved to PREWHERE, its selectivity estimated by the statistics, a read
        # in the order of the sorting key as the planner makes it by default, and a sort with a LIMIT as a sort, not a
        # top-k, whatever the test harness may set.
        GPU_SETTINGS=(--allow_experimental_gpu_filtered_read 1 --optimize_move_to_prewhere 1 --use_statistics 1 --optimize_read_in_order 1 --query_plan_max_limit_for_top_k_optimization 0)
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

# Whether the read is filtered on the device, and whether a part of its `PREWHERE` is left to the CPU.
function plan_of()
{
    local query="$1"

    if [ ${#GPU_SETTINGS[@]} -eq 0 ]; then
        echo "$2"
        return
    fi

    local plan
    plan=$($CLICKHOUSE_CLIENT "${GPU_SETTINGS[@]}" --query "EXPLAIN $query")
    if [[ "$plan" == *"The rest of the PREWHERE, on the CPU"* ]]; then
        echo "device and cpu"
    elif [[ "$plan" == *"filtered on the device"* ]]; then
        echo "device"
    else
        echo "cpu"
    fi
}

# Dates against dates written as strings, as TPC-H writes them, and every column sent back.
QUERY="SELECT count(), sum(k), round(sum(p), 3), sum(q), sum(cityHash64(m)), max(t) FROM gpu_filtered_read WHERE d >= date '1994-01-01' AND d < date '1994-01-01' + INTERVAL 1 YEAR"
plan_of "$QUERY" device
compare_with_cpu "$QUERY"

# A string conjunct is left to the CPU; the others, which a filter above takes as columns, are true for the rows sent back.
QUERY="SELECT m, count(), round(sum(p), 3) FROM gpu_filtered_read WHERE m IN ('MAIL', 'SHIP') AND c < r AND d < c AND r >= date '1994-01-01' AND r < date '1994-01-01' + INTERVAL 1 YEAR GROUP BY m ORDER BY m"
plan_of "$QUERY" "device and cpu"
compare_with_cpu "$QUERY"

# The string conjunct is over a column the read does not output otherwise.
QUERY="SELECT round(sum(p), 3), count() FROM gpu_filtered_read WHERE d < '1993-01-01' AND m = 'AIR'"
plan_of "$QUERY" "device and cpu"
compare_with_cpu "$QUERY"

compare_with_cpu "SELECT count(), sum(cityHash64(m)) FROM gpu_filtered_read WHERE c >= '1995-01-01' OR r < '1993-01-01'"
compare_with_cpu "SELECT count(), sum(k) FROM gpu_filtered_read WHERE t >= '1994-01-02 00:00:00' AND t < '1994-01-03 00:00:00' AND q > -200000"
compare_with_cpu "SELECT count() FROM gpu_filtered_read WHERE d < '1992-01-01' AND q < 0"
compare_with_cpu "SELECT k, m, p FROM gpu_filtered_read WHERE d = '1996-03-04' AND q < 0 ORDER BY k LIMIT 5"

# A read in the order of the sorting key stays on the CPU, as the steps above take its rows as sorted.
QUERY="SELECT k, m, p FROM gpu_filtered_read WHERE d = '1996-03-04' ORDER BY k LIMIT 5"
plan_of "$QUERY" cpu
compare_with_cpu "$QUERY"

$CLICKHOUSE_CLIENT --query "DROP TABLE gpu_filtered_read"
