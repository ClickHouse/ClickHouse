#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings
# The PREWHERE effect ProfileEvents must count only the query's PREWHERE predicate. A read chain also applies
# other filters that are not PREWHERE -- on-fly mutations (UPDATE/DELETE), lightweight-delete masks, row-level
# policies -- as the same kind of filter step. Those must not be reported as PREWHERE reading or dropping
# granules/rows: a query without PREWHERE must report nothing, and a PREWHERE that keeps every surviving row
# must report no drops even when an earlier on-fly DELETE emptied some granules.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

dump_events() {
  local query_id=$1
  ${CLICKHOUSE_CLIENT} -q "
    SELECT
      sum(ProfileEvents['GranulesReadByPrewhereReaders']),
      sum(ProfileEvents['GranulesSkippedByPrewhereReaders']),
      sum(ProfileEvents['RowsSkippedByPrewhereReaders'])
    FROM system.query_log
   WHERE event_date >= yesterday() AND event_time >= now() - 600
     AND has(databases, currentDatabase()) AND initial_query_id = '$query_id' AND type = 'QueryFinish'
  "
}

# 16 granules of 8192 rows. An unmaterialized on-fly DELETE removes granule 0 (rows 0..8191, a < 8192) at read
# time; SYSTEM STOP MERGES and mutations_sync=0 keep it unmaterialized so apply_mutations_on_fly applies it.
${CLICKHOUSE_CLIENT} -q "
  DROP TABLE IF EXISTS t_mut;
  CREATE TABLE t_mut(a UInt32, b UInt32) ENGINE=MergeTree ORDER BY a
    SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0, index_granularity=8192, add_minmax_index_for_numeric_columns=0;
  INSERT INTO t_mut SELECT number, number FROM numbers(131072)
    SETTINGS min_insert_block_size_rows = 131073, min_insert_block_size_bytes = 0;
  SYSTEM STOP MERGES t_mut;
  ALTER TABLE t_mut DELETE WHERE a < 8192 SETTINGS mutations_sync = 0;
"

# -------- No PREWHERE, only an on-fly DELETE --------
# The query has no PREWHERE, so the DELETE filter must not be counted: 0 granules read, 0 dropped, 0 rows.
qid_no_prewhere="${CLICKHOUSE_DATABASE}_no_prewhere_$RANDOM$RANDOM"
${CLICKHOUSE_CLIENT} --apply_mutations_on_fly 1 --optimize_move_to_prewhere 0 --max_threads 1 --use_query_condition_cache 0 \
  --query_id "$qid_no_prewhere" -q "SELECT sum(a) FROM t_mut FORMAT Null"

# -------- A PREWHERE that keeps every surviving row, plus the on-fly DELETE --------
# PREWHERE b != 4294967295 retains all rows the DELETE leaves. The DELETE emptied granule 0 before PREWHERE,
# so PREWHERE read the 15 surviving granules and dropped none; the DELETE's rows are not counted as PREWHERE
# drops.
qid_keep_all="${CLICKHOUSE_DATABASE}_keep_all_$RANDOM$RANDOM"
${CLICKHOUSE_CLIENT} --apply_mutations_on_fly 1 --optimize_move_to_prewhere 0 --max_threads 1 --use_query_condition_cache 0 \
  --query_id "$qid_keep_all" -q "SELECT sum(a) FROM t_mut PREWHERE b != 4294967295 FORMAT Null"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"

# granules read / granules dropped / rows dropped by PREWHERE.
#   no_prewhere: 0, 0, 0
#   keep_all:    15, 0, 0
dump_events "$qid_no_prewhere"
dump_events "$qid_keep_all"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_mut"
