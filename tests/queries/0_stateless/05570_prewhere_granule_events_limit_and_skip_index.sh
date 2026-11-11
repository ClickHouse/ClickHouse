#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
# no-parallel-replicas: the test asserts exact per-reader PREWHERE granule counts for a single-server
# read plan; parallel replicas distribute marks across replicas and make the LIMIT early-termination
# case non-deterministic.
# The PREWHERE granule ProfileEvents must count only the granules PREWHERE actually evaluated:
#   - a query that stops early (LIMIT) still counts the granules it read, and never declares the
#     unread remainder of the granule it stopped in as dropped;
#   - marks pruned by a skip index before PREWHERE reads them are not attributed to PREWHERE.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

dump_events() {
  local query_id=$1
  ${CLICKHOUSE_CLIENT} -q "
    SELECT
      sum(ProfileEvents['GranulesReadByPrewhereReaders']),
      sum(ProfileEvents['GranulesSkippedByPrewhereReaders'])
    FROM system.query_log
   WHERE event_date >= yesterday() AND event_time >= now() - 600
     AND has(databases, currentDatabase()) AND initial_query_id = '$query_id' AND type = 'QueryFinish'
  "
}

# -------- Early termination (LIMIT) --------
# max_threads=1 and max_block_size=8192 make each block exactly one granule, so the read is deterministic.
# PREWHERE b >= 8192 drops granule 0 (rows 0..8191, all < 8192) and keeps granule 1; LIMIT 1 is satisfied
# by the first row of granule 1, so PREWHERE read 2 granules and dropped 1. The granule it stopped in
# (granule 1) is counted as read but not dropped.
${CLICKHOUSE_CLIENT} -q "
  DROP TABLE IF EXISTS t_limit;
  CREATE TABLE t_limit(a UInt32, b UInt32) ENGINE=MergeTree ORDER BY a
    SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0, index_granularity=8192, add_minmax_index_for_numeric_columns=0;
  INSERT INTO t_limit SELECT number, number FROM numbers(1000000)
    SETTINGS min_insert_block_size_rows = 1000001, min_insert_block_size_bytes = 0;
"
qid_limit="${CLICKHOUSE_DATABASE}_limit_$RANDOM$RANDOM"
${CLICKHOUSE_CLIENT} --max_threads 1 --max_block_size 8192 --use_query_condition_cache 0 --optimize_read_in_order 0 \
  --query_id "$qid_limit" -q "SELECT a FROM t_limit PREWHERE b >= 8192 LIMIT 1 FORMAT Null"

# -------- Skip index pruning before PREWHERE --------
# A minmax index on `v` prunes the marks whose rows all have v >= 500 before PREWHERE reads them. With 64-row
# granules and v = number, only marks 0..7 (rows 0..511) can hold a row with v < 500, so 8 marks reach
# PREWHERE and the other ~1555 are pruned by the index. optimize_move_to_prewhere=0 keeps `v < 500` in WHERE
# (evaluated by the index), use_skip_indexes_on_data_read=1 prunes during the data read. PREWHERE b % 2 = 0
# keeps half of every evaluated granule, so PREWHERE read 8 granules and dropped none -- the index-pruned
# marks must not be counted as read or dropped by PREWHERE.
${CLICKHOUSE_CLIENT} -q "
  DROP TABLE IF EXISTS t_skip;
  CREATE TABLE t_skip(a UInt32, b UInt32, v UInt32, INDEX idx_v v TYPE minmax GRANULARITY 1) ENGINE=MergeTree ORDER BY a
    SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0, index_granularity=64, add_minmax_index_for_numeric_columns=0;
  INSERT INTO t_skip SELECT number, number, number FROM numbers(100000)
    SETTINGS min_insert_block_size_rows = 100001, min_insert_block_size_bytes = 0;
"
qid_skip="${CLICKHOUSE_DATABASE}_skip_$RANDOM$RANDOM"
${CLICKHOUSE_CLIENT} --max_threads 1 --use_query_condition_cache 0 --optimize_move_to_prewhere 0 --use_skip_indexes_on_data_read 1 \
  --query_id "$qid_skip" -q "SELECT sum(a) FROM t_skip PREWHERE b % 2 = 0 WHERE v < 500 FORMAT Null"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"

# granules read / granules dropped by PREWHERE.
#   limit: 2, 1
#   skip:  8, 0
dump_events "$qid_limit"
dump_events "$qid_skip"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_limit; DROP TABLE t_skip"
