#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
# no-parallel-replicas: the PREWHERE case asserts exact per-reader granule counts for a single-server read plan.
#
# The PREWHERE-effectiveness ProfileEvents must count only the user's PREWHERE/WHERE predicate, not the
# dynamic filters an optimizer injects into the read:
#   - the `__topKFilter` of `ORDER BY <non-key> LIMIT n` (use_top_k_dynamic_filtering), and
#   - a join runtime filter pushed into the probe-side scan.
# Both are synthesized conjuncts of `prewhere_info`, so a query without a user predicate must report zero;
# a real PREWHERE is still counted.

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

${CLICKHOUSE_CLIENT} -q "
  DROP TABLE IF EXISTS t_facts;
  DROP TABLE IF EXISTS t_dim;
  CREATE TABLE t_facts(a UInt32, b UInt32, c UInt32) ENGINE=MergeTree ORDER BY a
    SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0, index_granularity=8192, add_minmax_index_for_numeric_columns=0;
  INSERT INTO t_facts SELECT number, number, number FROM numbers(200000)
    SETTINGS min_insert_block_size_rows = 200001, min_insert_block_size_bytes = 0;
  CREATE TABLE t_dim(x UInt32) ENGINE=MergeTree ORDER BY x;
  INSERT INTO t_dim VALUES (42)(100)(7);
"

# -------- ORDER BY <non-key> LIMIT: the top-K dynamic threshold filter is not PREWHERE --------
# `__topKFilter` is injected as the leading PREWHERE conjunct to drop rows that cannot enter the top n, but
# the query has no user PREWHERE/WHERE, so nothing must be attributed to PREWHERE.
qid_topk="${CLICKHOUSE_DATABASE}_topk_$RANDOM$RANDOM"
${CLICKHOUSE_CLIENT} --use_top_k_dynamic_filtering 1 --max_threads 1 --use_query_condition_cache 0 \
  --query_id "$qid_topk" -q "SELECT a FROM t_facts ORDER BY b LIMIT 10 FORMAT Null"

# -------- Join runtime filter: the probe-side dynamic filter is not PREWHERE --------
# The join builds a runtime filter on the small side and pushes it into the scan of t_facts (`__applyFilter`).
# With no user predicate it must not be counted, even though it rejects almost every row.
qid_rf="${CLICKHOUSE_DATABASE}_rf_$RANDOM$RANDOM"
${CLICKHOUSE_CLIENT} --enable_join_runtime_filters 1 --optimize_move_to_prewhere 1 --max_threads 1 --use_query_condition_cache 0 \
  --query_id "$qid_rf" -q "SELECT t.a FROM t_facts t JOIN t_dim d ON t.b = d.x FORMAT Null"

# -------- A real PREWHERE is still counted --------
# PREWHERE b < 8192 keeps granule 0 (rows 0..8191) and rejects every row of granules 1..24: 25 read, 24 dropped.
qid_pw="${CLICKHOUSE_DATABASE}_pw_$RANDOM$RANDOM"
${CLICKHOUSE_CLIENT} --max_threads 1 --use_query_condition_cache 0 \
  --query_id "$qid_pw" -q "SELECT a FROM t_facts PREWHERE b < 8192 FORMAT Null"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"

# granules read / granules dropped / rows dropped by PREWHERE.
#   top-K:     0, 0, 0        (dynamic threshold filter, no user predicate)
#   join RF:   0, 0, 0        (dynamic join runtime filter, no user predicate)
#   prewhere: 25, 24, 191808
dump_events "$qid_topk"
dump_events "$qid_rf"
dump_events "$qid_pw"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_facts; DROP TABLE t_dim"
