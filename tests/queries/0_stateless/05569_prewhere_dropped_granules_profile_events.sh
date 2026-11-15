#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings
# add_minmax_index_for_numeric_columns=0: keep `b`/`c` unindexed so that PREWHERE, not index pruning, drops the granules.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
  DROP TABLE IF EXISTS t;

  CREATE TABLE t(a UInt32, b UInt32, c UInt32) ENGINE=MergeTree ORDER BY a
    SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0, index_granularity=8192, add_minmax_index_for_numeric_columns=0;

  -- One part with 8192-aligned granules: 1e7 rows == 1221 granules. Squash the whole input into a single
  -- block so the part layout (and therefore the granule counters) is deterministic.
  INSERT INTO t SELECT number, number, number FROM numbers(1e7)
    SETTINGS min_insert_block_size_rows = 10000001, min_insert_block_size_bytes = 0;

  SELECT throwIf(count() != 1, 'the insert must produce exactly one part') FROM system.parts
   WHERE active AND database = currentDatabase() AND table = 't' FORMAT Null;
"

# The granule counters must be exact regardless of how the read is split into blocks or across threads.
# max_block_size 65409 (the default) is not a multiple of 8192, so a granule is split across read blocks;
# 8192 keeps every block granule-aligned. Both must report the same, granule-accurate numbers: a granule
# split across blocks whose rows are all filtered has to be counted as dropped exactly once.
#
# max_threads 16 divides the part into several reading ranges so that a fully dropped granule falls on a
# range/block boundary. That is the layout that used to miscount it (the split granule's unfiltered
# leading piece was mistaken for a survivor); pinning max_threads also makes the result independent of
# the test machine's core count.
#
# use_query_condition_cache=0: a warm query condition cache skips fully-filtered granules *before* PREWHERE
# reads them, so PREWHERE would then legitimately read and drop fewer granules. Disable it so the counters
# reflect pure PREWHERE and do not depend on whether the query ran before.
run_query() {
  local query_id=$1
  local block_size=$2
  local predicate=$3
  ${CLICKHOUSE_CLIENT} --use_query_condition_cache 0 --max_threads 16 --max_block_size "$block_size" --query_id "$query_id" -q "
    SELECT count() FROM t PREWHERE $predicate FORMAT Null
  "
}

# b/c are not the primary key and have no minmax index, so these granules are dropped by PREWHERE itself.

# b < 5000000 drops every granule whose rows are all >= 5000000: granules 611..1220 (610 of 1221), 5000000 rows.
qid_drop_frag="${CLICKHOUSE_DATABASE}_drop_frag_$RANDOM$RANDOM"
qid_drop_align="${CLICKHOUSE_DATABASE}_drop_align_$RANDOM$RANDOM"
run_query "$qid_drop_frag" 65409 "b < 5000000"
run_query "$qid_drop_align" 8192 "b < 5000000"

# b % 8192 = 42 keeps exactly one row in every granule, so PREWHERE drops no granule entirely (9998779 rows dropped).
qid_keep_frag="${CLICKHOUSE_DATABASE}_keep_frag_$RANDOM$RANDOM"
qid_keep_align="${CLICKHOUSE_DATABASE}_keep_align_$RANDOM$RANDOM"
run_query "$qid_keep_frag" 65409 "(b % 8192) = 42"
run_query "$qid_keep_align" 8192 "(b % 8192) = 42"

# A multi-condition PREWHERE over two columns is evaluated in several read steps; the granule counters are
# taken from the survivor set after all steps, so they must still be exact. b < 5000000 AND c even drops the
# same 610 granules (no row of granules 611..1220 has b < 5000000) and keeps 2500000 rows, so 7500000 rows
# are dropped.
qid_multi_frag="${CLICKHOUSE_DATABASE}_multi_frag_$RANDOM$RANDOM"
qid_multi_align="${CLICKHOUSE_DATABASE}_multi_align_$RANDOM$RANDOM"
run_query "$qid_multi_frag" 65409 "b < 5000000 AND (c % 2) = 0"
run_query "$qid_multi_align" 8192 "b < 5000000 AND (c % 2) = 0"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"

# granules read / granules dropped / rows dropped by PREWHERE, summed over all reading threads of the query.
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

# Expected (identical for both block sizes):
#   drop:  1221, 610, 5000000
#   keep:  1221,   0, 9998779
#   multi: 1221, 610, 7500000
dump_events "$qid_drop_frag"
dump_events "$qid_drop_align"
dump_events "$qid_keep_frag"
dump_events "$qid_keep_align"
dump_events "$qid_multi_frag"
dump_events "$qid_multi_align"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t"
