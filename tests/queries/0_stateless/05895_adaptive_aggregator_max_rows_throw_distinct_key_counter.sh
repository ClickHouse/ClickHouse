#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Under a throw-mode `max_rows_to_group_by`, each thread of the adaptive aggregator counts its distinct
# keys approximately: the keys that its tables insert and the keys that it stages. A frozen table stages
# the keys that it misses, so its size stops counting them. The distinct key counter makes the thread
# throw while it reads instead of at the merge. A throw by the counter reports "has about N rows", one by
# an exact count "has N rows". The cells read one stream, so each has one producer, whose table freezes
# after its first slice of a block, at 4096 keys. Every cell runs in a clickhouse-local process of its
# own, so the events in `system.events` belong to it alone.
SETTINGS="
SET max_threads = 1, max_block_size = 8192;
SET adaptive_aggregator_freeze_threshold = 128, adaptive_aggregator_freeze_threshold_bytes = 0;
SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 1000000000;
SET collect_hash_table_stats_during_aggregation = 0;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SET enable_adaptive_aggregator = 1, max_rows_to_group_by = 10000, group_by_overflow_mode = 'throw';
"

# Prints the label, the check that threw (`estimated`, `exact` or `none`), the result if there is one,
# and the thaws.
function check()
{
    local label=$1 query=$2 errors="$CLICKHOUSE_TMP/05895_$3.err"
    local output
    output=$($CLICKHOUSE_LOCAL --ignore-error --send_logs_level=trace --query "
$SETTINGS
$query;
SELECT 'thaws', coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationThaws';
" 2>"$errors")
    local thrown
    thrown=$(grep -oE "has (about )?[0-9]+ rows, maximum" "$errors" | head -1 | sed -E 's/^has about.*/estimated/; s/^has [0-9].*/exact/')
    echo "$label: ${thrown:-none}"
    echo "$output"
}

# Every key after the freeze is staged, and the counter counts it: the producer throws after two blocks,
# where an exact count waits for the merge.
check "Staged keys" "SELECT count() FROM (SELECT number AS k FROM numbers(100000) GROUP BY k)" staged

# The stream thaws its producer and then switches to new keys, which the thawed table inserts. It starts
# with 6000 keys, which the table holds or stages, then repeats 10 keys until the thaw, then brings 5000
# new keys. The thawed table holds at most 9106 keys, under the limit, while the producer saw 11010. So
# only the counter, which takes the keys that a table inserts, throws before the merge. The cells cover a
# table without aggregates, with a lone `count` and with another aggregate, whose inserts take separate
# paths.
keys="multiIf(number < 6000, 1000000 + number, number < 106000, 2000000 + number % 10, 3000000 + number % 5000)"
check "Keys of a thawed table, no aggregates" "SELECT count() FROM (SELECT $keys AS k FROM numbers(126000) GROUP BY k)" keys
check "Keys of a thawed table, count" "SELECT count() FROM (SELECT $keys AS k, count() FROM numbers(126000) GROUP BY k)" count
check "Keys of a thawed table, sum" "SELECT count() FROM (SELECT $keys AS k, sum(number) FROM numbers(126000) GROUP BY k)" sum

# A thawed table that keeps getting the keys of the stream takes back the keys that its producer staged.
# Once it holds all but a sixteenth of the limit of the counted keys, at about 98000 of the 100000 keys,
# the producer drops the counter, and the exact count of the table takes over. The two `String` keys
# take a method that precomputes the hashes of its rows once the table is past the prefetch threshold, the
# size of the L2 cache, as the thawed table is well before the drop. The producer drops the counter only if
# the hash of a key that the table takes back, rehashed or precomputed, equals the routing hash of the key's
# staged records: otherwise the key counts twice. The injective rewrite is off, because it would group by
# the numbers instead of the strings.
check "A thawed table that takes its staged keys back" \
    "SELECT count() FROM (SELECT toString(number % 100000) AS a, toString(number % 100000 + 1) AS b FROM numbers(500000) GROUP BY a, b)
    SETTINGS max_rows_to_group_by = 110000, enable_software_prefetch_in_aggregation = 1, optimize_injective_functions_in_group_by = 0" drop
echo "dropped counters: $(grep -c 'dropped the distinct key counter' "$CLICKHOUSE_TMP/05895_drop.err")"

# At a limit of 150000 the counter samples one key in two. It samples by a mixed hash, because the table hash
# `HashCRC32` of an integer is linear: the 100000 numbers below 200000 that pass this filter on the parity of
# some of their bits share the low bit of their table hashes. A sample by that bit would hold all of them and
# double the count, so the producer would throw under the limit.
check "Keys whose table hashes share their low bit" \
    "SELECT count() FROM (SELECT number AS k FROM numbers(200000) WHERE bitCount(bitAnd(number, 0x32c25)) % 2 = 1 GROUP BY k)
    SETTINGS max_rows_to_group_by = 150000" parity

# A producer over the limit by less than a sixteenth does not throw, because the error of the estimate
# could explain the excess. The exact count of the merge throws. The thaw is off, so the frozen table never
# takes the keys itself.
check "Over the limit by less than the margin" \
    "SELECT count() FROM (SELECT toUInt64(number % 10050) AS k FROM numbers(200000) GROUP BY k) SETTINGS adaptive_aggregator_disable_thaw = 1" margin
