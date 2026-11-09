#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `adaptive_aggregator_combine_arg_min_max` across the key kinds and record formats of the adaptive
# aggregator, a block whose keys are all one constant, an ordering column in `Sparse` serialization, and
# staged records written to disk. Each shape runs with the setting off and then on, and prints whether the
# results match and whether the setting staged fewer records.
#
# Everything runs in one clickhouse-local process, so the staged-record counter in `system.events` belongs to
# these queries, and each query's share is the difference of the counter around it. Four UNION ALL lanes feed
# the four producers, one each. A key holds 4 consecutive rows of a lane, ordered without ties by `o`, so the
# results are exact.

ROWS="SELECT number AS x, intDiv(number, 4) AS k, (number * 3 + intDiv(number, 4)) % 4 AS o FROM (
    SELECT number FROM numbers(0, 20000) UNION ALL SELECT number FROM numbers(20000, 20000)
    UNION ALL SELECT number FROM numbers(40000, 20000) UNION ALL SELECT number FROM numbers(60000, 20000))"

queries=""
counter="SELECT sum(value) FROM system.events WHERE event = 'AdaptiveAggregationStagedRecords'"

# A shape whose query takes `__VALUE__` for the setting.
function add_shape()
{
    local label=$1 query=$2
    for value in 0 1; do
        queries+="
        SELECT 'result', '${label}', count(), sum(cityHash64(*)) FROM (${query//__VALUE__/$value});
        SELECT 'staged', '${label}', (${counter});"
    done
}

# A shape whose staged records go to disk: it also prints whether the run with the setting on wrote records.
function add_spilled_shape()
{
    local label=$1 query=$2
    for value in 0 1; do
        queries+="
        SELECT 'result', '${label}', count(), sum(cityHash64(*)) FROM (${query//__VALUE__/$value});
        SELECT 'spilled', '${label}', (SELECT sum(value) FROM system.events WHERE event = 'AdaptiveAggregationSpilledRecords');
        SELECT 'staged', '${label}', (${counter});"
    done
}

# A shape whose staged records depend on the platform, so only its results are compared.
function add_results_only()
{
    local label=$1 query=$2
    queries+="
    SELECT 'check', '${label}: results match',
        (SELECT count(), sum(cityHash64(*)) FROM (${query//__VALUE__/0})) = (SELECT count(), sum(cityHash64(*)) FROM (${query//__VALUE__/1}));
    SELECT 'sync', '', (${counter});"
}

function grouped()
{
    local keys=$1 aggregates=$2 extra=$3
    echo "SELECT ${keys}, ${aggregates} FROM (${ROWS}) GROUP BY ${keys}
        SETTINGS adaptive_aggregator_combine_arg_min_max = __VALUE__ ${extra}"
}

# A fixed-width key with only fixed-size arguments has fixed-stride records; with `String` arguments its
# records have no header; any other variable-size argument, or a key staged as bytes, takes the general
# format.
add_shape "fixed key, String argument" "$(grouped k "argMax(toString(x), o), max(o), count()")"
add_shape "fixed key, Array argument" "$(grouped k "argMax([x, x + 1], o), max(o), count()")"
add_shape "String key" "$(grouped "toString(k) AS s" "argMax(x, o), max(o), count()" ", enable_packed_string_keys_in_aggregation = 0")"
add_shape "packed String key" "$(grouped "toString(k) AS s" "argMin(x, o), min(o), count()" ", enable_packed_string_keys_in_aggregation = 1")"
add_shape "two fixed-width keys" "$(grouped "k, k % 7 AS r" "argMax(x, o), max(o), count()")"
add_shape "Nullable fixed-width keys" "$(grouped "toNullable(k) AS n, toNullable(k % 7) AS r" "argMax(x, o), max(o), count()")"
add_shape "serialized keys" "$(grouped "toString(k) AS s, k" "argMax(x, o), max(o), count()")"
# Serialized keys longer than 128 bytes on average are serialized row by row on x86, whose key views do not
# outlive their row, so their misses combine only into records of earlier batches there. ARM serializes them
# in batches and combines them within a batch as well, so the staged records depend on the platform.
add_results_only "long serialized keys" "$(grouped "repeat(toString(k), 30) AS s, k" "argMax(x, o), max(o), count()")"
# A limit of external aggregation of one byte writes the staged records to disk, with their multiplicities.
add_spilled_shape "staged records on disk" "$(grouped k "argMax(x, o), max(o), count()" ", max_bytes_before_external_group_by = 1")"

# The tables are small, so the two MergeTree shapes ask for concurrent reads, which give every producer a
# stream.
CONCURRENT_READS="merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 1"

# The keys of a block from one part are the constant `_part_index`. With more parts than producers, some
# producer reads blocks of a second part after its table froze on the first, so its constant key misses and
# the block is one run.
add_shape "constant-key blocks" "SELECT _part_index AS p, argMax(n, o), max(o), count() FROM parts GROUP BY p
    SETTINGS adaptive_aggregator_combine_arg_min_max = __VALUE__, adaptive_aggregator_freeze_threshold = 1, max_block_size = 1024,
    ${CONCURRENT_READS}"
add_shape "constant-key blocks without count" "SELECT _part_index AS p, argMin(n, o), min(o) FROM parts GROUP BY p
    SETTINGS adaptive_aggregator_combine_arg_min_max = __VALUE__, adaptive_aggregator_freeze_threshold = 1, max_block_size = 1024,
    ${CONCURRENT_READS}"

# `max(ts)` comes first, so the ordering column is its single argument, which keeps the `Sparse` column of the
# part. `x` depends on `ts` only, so the rows that tie on `ts` give the same results.
queries+="
SELECT 'check', 'serialization of ts', serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 'sparse' AND column = 'ts' AND active;"
add_shape "Sparse ordering column" "SELECT k, max(ts), argMax(x, ts), count() FROM sparse GROUP BY k
    SETTINGS adaptive_aggregator_combine_arg_min_max = __VALUE__, ${CONCURRENT_READS}"

$CLICKHOUSE_LOCAL --query "
SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 8192;
SET enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 1024, adaptive_aggregator_freeze_threshold_bytes = 0;
SET adaptive_aggregator_disable_thaw = 1, collect_hash_table_stats_during_aggregation = 0;
SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 1000000000;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SET optimize_aggregation_in_order = 0, optimize_group_by_constant_keys = 1;

CREATE TABLE parts (n UInt64, o UInt64) ENGINE = MergeTree ORDER BY n SETTINGS index_granularity = 1024;
SYSTEM STOP MERGES parts;
INSERT INTO parts SELECT number, (number * 7919) % 1000003 FROM numbers(0, 4096);
INSERT INTO parts SELECT number, (number * 7919) % 1000003 FROM numbers(4096, 4096);
INSERT INTO parts SELECT number, (number * 7919) % 1000003 FROM numbers(8192, 4096);
INSERT INTO parts SELECT number, (number * 7919) % 1000003 FROM numbers(12288, 4096);
INSERT INTO parts SELECT number, (number * 7919) % 1000003 FROM numbers(16384, 4096);
INSERT INTO parts SELECT number, (number * 7919) % 1000003 FROM numbers(20480, 4096);
INSERT INTO parts SELECT number, (number * 7919) % 1000003 FROM numbers(24576, 4096);
INSERT INTO parts SELECT number, (number * 7919) % 1000003 FROM numbers(28672, 4096);

CREATE TABLE sparse (k UInt64, ts UInt64, x UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5, index_granularity = 8192;
INSERT INTO sparse SELECT intDiv(number, 5), if(number % 20 = 3, number, 0) AS ts, ts * 2 + 1 FROM numbers(200000);
OPTIMIZE TABLE sparse FINAL;
${queries}
" | awk -F'\t' '
    # A shape prints its result and the cumulative staged-record counter with the setting off, then on.
    $1 == "result" { result[$2, ++results[$2]] = $3 "\t" $4; next }
    $1 == "spilled" { spilled[$2, ++spills[$2]] = $3 - previous_spilled; previous_spilled = $3; next }
    $1 == "staged" {
        staged[$2, ++counters[$2]] = $3 - previous
        previous = $3
        if (counters[$2] < 2)
            next
        line = $2 "\tresults match: " (result[$2, 1] == result[$2, 2]) "\tfewer records: " (staged[$2, 2] < staged[$2, 1])
        if (($2, 2) in spilled)
            line = line "\twritten to disk: " (spilled[$2, 2] > 0)
        print line
        next
    }
    $1 == "sync" { previous = $3; next }
    $1 == "check" { print $2 "\t" $3 }'
