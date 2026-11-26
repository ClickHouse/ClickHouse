#!/usr/bin/env bash
# Tags: no-parallel-replicas
# no-parallel-replicas: the order is read from the planning log of the initiator

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# PREWHERE conditions are ordered by the column sizes of the whole table, so the cheap selective `a1 < 4294967`
# runs before `editDistance` even though `s1` compresses much better in the selected partition (#131714).
# The order of the PREWHERE steps is only visible in the test-level log of the optimizer.

${CLICKHOUSE_CLIENT} --query "
    SET materialize_statistics_on_insert = 1, max_insert_threads = 1;
    CREATE TABLE t_stats (p1 UInt8, a1 UInt32 CODEC(LZ4), s1 String CODEC(LZ4))
    ENGINE = MergeTree PARTITION BY p1 ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, auto_statistics_types = 'basic', index_granularity = 128;
    CREATE TABLE t_nostats (p1 UInt8, a1 UInt32 CODEC(LZ4), s1 String CODEC(LZ4))
    ENGINE = MergeTree PARTITION BY p1 ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, auto_statistics_types = '', index_granularity = 128;
    INSERT INTO t_stats SELECT number % 4, 0, randomPrintableASCII(64) FROM numbers(40000);
    INSERT INTO t_stats SELECT 9, toUInt32(cityHash64(number)), concat('k0-', repeat('ab-cd-', 5)) FROM numbers(100000);
    INSERT INTO t_nostats SELECT * FROM t_stats;
"

function moved()
{
    ${CLICKHOUSE_CLIENT} --send_logs_level=test --query "
        EXPLAIN SELECT count() FROM $1 WHERE $2
        SETTINGS optimize_move_to_prewhere = 1, allow_reorder_prewhere_conditions = 1, move_all_conditions_to_prewhere = 1,
            use_statistics = 1, automatic_parallel_replicas_mode = 0$3" 2>&1 >/dev/null \
        | grep -F 'moved to PREWHERE' \
        | sed -E 's/.*Condition\(exp:(.*) viable: .*(estimated_row_count: [0-9]+).*/\1 \2/'
}

CLASSIFY='{ c = "" } /editDistance/ { c = "editDistance" } c == "" && /(^|[^[:alnum:]_])a1([^[:alnum:]_]|$)/ { c = "a1" }'

# Prints the conditions on `a1` and `editDistance` in the order they are moved to PREWHERE.
function order()
{
    moved "$@" | awk "$CLASSIFY"' c != "" && !seen[c]++ { printf "%s%s", (n++ ? " " : ""), c } END { print "" }'
}

# The statistics of t_stats must be loaded, or its lines repeat t_nostats.
function estimates()
{
    moved "$1" "$A1 AND p1 = 9 AND $ED" | awk "$CLASSIFY"' c == "a1" && $NF > 0 { u = 1 } END { print u + 0 }'
}

ED="editDistance(s1, repeat('ab-dc-', 50)) > 3"
A1="a1 < 4294967"

for t in t_stats t_nostats; do
    echo "$t, editDistance written first: $(order $t "$ED AND p1 = 9 AND $A1")"
    echo "$t, a1 written first, no short-circuit: $(order $t "p1 = 9 AND $A1 AND $ED" ", short_circuit_function_evaluation = 'disable'")"
    echo "$t, a1 written first: $(order $t "p1 = 9 AND $A1 AND $ED")"
    echo "$t, statistics used: $(estimates $t)"
done

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_stats; DROP TABLE t_nostats"
