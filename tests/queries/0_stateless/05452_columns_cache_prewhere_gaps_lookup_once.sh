#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# After every gap that `PREWHERE` leaves, the reader re-enters its mark range. It must keep the
# columns cache entries it looked up when the range began, not look up the rest of the range again
# per gap. Every third granule passes here: a lookup per gap would be about 330 of them, against a
# few with one lookup per range. The rows served after a re-entry must be right.

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_cc_gaps;
    CREATE TABLE t_cc_gaps (k UInt64, m UInt8, v UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 16, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
    INSERT INTO t_cc_gaps SELECT number, intDiv(number, 16) % 3, number FROM numbers(16000);
    SELECT sum(v) FROM t_cc_gaps SETTINGS use_columns_cache = 1, enable_writes_to_columns_cache = 1,
        columns_cache_max_bytes_to_write_to_cache = 0, columns_cache_max_estimated_bytes_to_write_to_cache = 0 FORMAT Null;
"

SETTINGS="use_columns_cache = 1, enable_reads_from_columns_cache = 1, max_threads = 1, use_query_condition_cache = 0"

# Gaps of whole granules, and gaps inside granules.
for condition in "m = 0" "m = 0 AND k % 16 < 8"
do
    query="SELECT sum(v), count() FROM t_cc_gaps PREWHERE $condition"
    $CLICKHOUSE_CLIENT -q "$query SETTINGS $SETTINGS"
    lookups=$($CLICKHOUSE_CLIENT --allow_repeated_settings --send_logs_level=test -q "$query SETTINGS $SETTINGS FORMAT Null" 2>&1 \
        | grep -c "granules are in the columns cache")
    echo "lookups: $(( lookups > 0 && lookups < 10 ))"
done

$CLICKHOUSE_CLIENT -q "DROP TABLE t_cc_gaps"
