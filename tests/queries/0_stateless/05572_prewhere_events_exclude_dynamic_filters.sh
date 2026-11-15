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
  DROP TABLE IF EXISTS t_prefix;
  CREATE TABLE t_facts(a UInt32, b UInt32, c UInt32) ENGINE=MergeTree ORDER BY a
    SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0, index_granularity=8192, add_minmax_index_for_numeric_columns=0;
  INSERT INTO t_facts SELECT number, number, number FROM numbers(200000)
    SETTINGS min_insert_block_size_rows = 200001, min_insert_block_size_bytes = 0;
  CREATE TABLE t_dim(x UInt32) ENGINE=MergeTree ORDER BY x;
  INSERT INTO t_dim VALUES (42)(100)(7);
  -- For number >= 65536, b = number % 8192, so ORDER BY b LIMIT keeps the ten smallest-b rows of each of those
  -- granules; those ten rows are exactly the ones with c = 0, so the user predicate c = 1 rejects the whole
  -- top-K remnant while thousands of rows per granule still satisfy c = 1.
  CREATE TABLE t_prefix(a UInt32, b UInt32, c UInt8) ENGINE=MergeTree ORDER BY a
    SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0, index_granularity=8192, add_minmax_index_for_numeric_columns=0;
  INSERT INTO t_prefix SELECT number, if(number < 65536, number, number % 8192), number < 65536 OR number % 8192 >= 10 FROM numbers(200000)
    SETTINGS min_insert_block_size_rows = 200001, min_insert_block_size_bytes = 0;
  -- Within each granule (except granule 0) the first half has b = 0, c = 0 and the second half has b = 200,
  -- c = 1. At index_granularity 8192 with max_block_size 4096 the two halves fall in separate read blocks, so
  -- the leading top-K (smallest b) keeps the first half and empties the second-half block before the user step.
  DROP TABLE IF EXISTS t_split_fragment;
  CREATE TABLE t_split_fragment(a UInt32, b UInt32, c UInt8) ENGINE=MergeTree ORDER BY a
    SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0, index_granularity=8192, add_minmax_index_for_numeric_columns=0;
  INSERT INTO t_split_fragment SELECT number, if(number < 8192, 100, if(number % 8192 < 4096, 0, 200)), number < 8192 OR number % 8192 >= 4096 FROM numbers(81920)
    SETTINGS min_insert_block_size_rows = 81921, min_insert_block_size_bytes = 0;
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

# -------- A join runtime filter after a user predicate is not counted, but the user predicate still is --------
# With the user condition in WHERE, `optimizePrewhere` moves both it and `__applyFilter` into PREWHERE as two
# steps: the user `c % 2 = 0` first, the runtime filter second. The runtime filter empties almost every granule,
# but the granules it drops must not be attributed to PREWHERE. `c % 2 = 0` keeps half of every granule, so the
# user predicate drops no granule and half the rows: 25 read, 0 dropped, 100000 rows dropped.
qid_rf_mixed="${CLICKHOUSE_DATABASE}_rf_mixed_$RANDOM$RANDOM"
${CLICKHOUSE_CLIENT} --enable_join_runtime_filters 1 --enable_join_runtime_filters_index_analysis 0 \
  --optimize_move_to_prewhere 1 --query_plan_join_swap_table 0 --enable_multiple_prewhere_read_steps 1 \
  --max_threads 1 --use_query_condition_cache 0 \
  --query_id "$qid_rf_mixed" -q "SELECT t.a FROM t_facts t JOIN t_dim d ON t.b = d.x WHERE t.c % 2 = 0 FORMAT Null"

# -------- A join runtime filter interleaved between two user steps is not counted --------
# With `optimize_prewhere_after_pushdown`, `PREWHERE c % 2 = 0` and the pushed-down `WHERE a < 199999` become
# two user steps with the runtime filter `__applyFilter` between them: `c % 2 = 0` -> `__applyFilter` -> `a < 199999`.
# The runtime filter empties almost every granule, but a granule it empties must not be attributed to PREWHERE,
# even though a later user step reads the already-emptied granule. Neither user predicate empties a granule
# (`c % 2 = 0` keeps half of each, `a < 199999` drops only the single last row), so: 25 read, 0 dropped, 100000
# rows dropped. A boundary snapshot at the last user step would still see the runtime filter's drops; per-step
# attribution does not.
qid_rf_interleaved="${CLICKHOUSE_DATABASE}_rf_interleaved_$RANDOM$RANDOM"
${CLICKHOUSE_CLIENT} --optimize_prewhere_after_pushdown 1 --enable_join_runtime_filters 1 \
  --enable_join_runtime_filters_index_analysis 0 --query_plan_join_swap_table 0 --enable_multiple_prewhere_read_steps 1 \
  --max_threads 1 --use_query_condition_cache 0 \
  --query_id "$qid_rf_interleaved" -q "SELECT t.a FROM t_facts t JOIN t_dim d ON t.b = d.x PREWHERE t.c % 2 = 0 WHERE t.a < 199999 FORMAT Null"

# -------- A runtime filter that only partially empties a granule, then a user step empties the remnant --------
# `c % 2 = 0` -> `__applyFilter(b)` -> `a > 100`. The runtime filter keeps only b in {42,100} of granule 0, then
# `a > 100` rejects both, emptying the granule. But granule 0 has many rows satisfying both user predicates
# (even a > 100), so the user predicate alone does NOT empty it -- the runtime filter did. The granule must not
# be attributed to PREWHERE. Counting the last-emptying user step would wrongly credit it. Run at two block sizes
# so a granule split into fragments is covered: the verdict must be the same, 25 read, 0 dropped.
qid_rf_partial_big="${CLICKHOUSE_DATABASE}_rf_partial_big_$RANDOM$RANDOM"
${CLICKHOUSE_CLIENT} --optimize_prewhere_after_pushdown 1 --enable_join_runtime_filters 1 \
  --enable_join_runtime_filters_index_analysis 0 --query_plan_join_swap_table 0 --enable_multiple_prewhere_read_steps 1 \
  --max_threads 1 --max_block_size 65409 --use_query_condition_cache 0 \
  --query_id "$qid_rf_partial_big" -q "SELECT t.a FROM t_facts t JOIN t_dim d ON t.b = d.x PREWHERE t.c % 2 = 0 WHERE t.a > 100 FORMAT Null"

qid_rf_partial_small="${CLICKHOUSE_DATABASE}_rf_partial_small_$RANDOM$RANDOM"
${CLICKHOUSE_CLIENT} --optimize_prewhere_after_pushdown 1 --enable_join_runtime_filters 1 \
  --enable_join_runtime_filters_index_analysis 0 --query_plan_join_swap_table 0 --enable_multiple_prewhere_read_steps 1 \
  --max_threads 1 --max_block_size 4096 --use_query_condition_cache 0 \
  --query_id "$qid_rf_partial_small" -q "SELECT t.a FROM t_facts t JOIN t_dim d ON t.b = d.x PREWHERE t.c % 2 = 0 WHERE t.a > 100 FORMAT Null"

# -------- A fused step mixing a user predicate with top-K is not counted --------
# With step splitting disabled the user `c % 2 = 0` and the top-K `__topKFilter` share one fused step whose
# rejections cannot be separated, so nothing is attributed to PREWHERE rather than counting top-K's rejections.
qid_topk_fused="${CLICKHOUSE_DATABASE}_topk_fused_$RANDOM$RANDOM"
${CLICKHOUSE_CLIENT} --use_top_k_dynamic_filtering 1 --enable_multiple_prewhere_read_steps 0 \
  --max_threads 1 --use_query_condition_cache 0 \
  --query_id "$qid_topk_fused" -q "SELECT a FROM t_facts PREWHERE c % 2 = 0 ORDER BY b LIMIT 10 FORMAT Null"

# -------- A leading top-K filter (before the user step) that empties the granule's remnant is not counted ------
# In a split plan the top-K `__topKFilter(b)` runs as its own step BEFORE the user `c = 1` step. It keeps only ten
# rows per affected granule, and those ten are exactly the c = 0 rows, so `c = 1` rejects the whole remnant and
# empties the granule. But every granule has thousands of rows satisfying `c = 1`, so the user predicate alone
# does not empty it -- the top-K did. The granule must not be attributed to PREWHERE, which requires tracking a
# dynamic filter that precedes the first user step. 25 read, 0 dropped, 170 rows dropped (the c = 0 rows).
qid_topk_lead="${CLICKHOUSE_DATABASE}_topk_lead_$RANDOM$RANDOM"
${CLICKHOUSE_CLIENT} --use_top_k_dynamic_filtering 1 --enable_multiple_prewhere_read_steps 1 \
  --max_threads 1 --max_block_size 65536 --preferred_block_size_bytes 0 --use_query_condition_cache 0 \
  --query_id "$qid_topk_lead" -q "SELECT a FROM t_prefix PREWHERE c = 1 ORDER BY b LIMIT 10 FORMAT Null"

# -------- A leading top-K that empties one whole block-fragment of a granule is not counted ------------------
# The granule spans two read blocks. Top-K keeps the first-half fragment (b = 0) and empties the second-half
# fragment (b = 200) before the user step, so the fragment carrying the user-surviving rows (c = 1) has zero rows
# entering `c = 1`. Its contamination must still reach the granule's mark; otherwise the first fragment, which
# `c = 1` cleanly empties, would make the granule count as dropped. Every granule keeps 4096 rows with c = 1, so
# the user predicate empties none: 10 read, 0 dropped, 36864 rows dropped.
qid_topk_frag="${CLICKHOUSE_DATABASE}_topk_frag_$RANDOM$RANDOM"
${CLICKHOUSE_CLIENT} --use_top_k_dynamic_filtering 1 --enable_multiple_prewhere_read_steps 1 \
  --max_threads 1 --max_block_size 4096 --preferred_block_size_bytes 0 --use_query_condition_cache 0 \
  --query_id "$qid_topk_frag" -q "SELECT a FROM t_split_fragment PREWHERE c = 1 ORDER BY b LIMIT 10 FORMAT Null"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"

# granules read / granules dropped / rows dropped by PREWHERE.
#   top-K:          0, 0, 0          (dynamic threshold filter, no user predicate)
#   join RF:        0, 0, 0          (dynamic join runtime filter, no user predicate)
#   prewhere:      25, 24, 191808
#   RF after user: 25, 0, 100000     (runtime filter's granule drops excluded; user predicate counted)
#   RF interleaved:25, 0, 100000     (runtime filter between two user steps excluded; user predicates counted)
#   RF partial big: 25, 0, 100002    (runtime filter thinned the granule; user step emptied only the remnant)
#   RF partial small:25, 0, 100002   (same, with the granule split into block fragments)
#   topK fused:     0, 0, 0          (mixed fused step not separable, not attributed)
#   topK leading:  25, 0, 170        (top-K before the user step thinned the granule; user step emptied remnant)
#   topK fragment: 10, 0, 36864      (top-K emptied one block-fragment of the granule; contamination kept per mark)
dump_events "$qid_topk"
dump_events "$qid_rf"
dump_events "$qid_pw"
dump_events "$qid_rf_mixed"
dump_events "$qid_rf_interleaved"
dump_events "$qid_rf_partial_big"
dump_events "$qid_rf_partial_small"
dump_events "$qid_topk_fused"
dump_events "$qid_topk_lead"
dump_events "$qid_topk_frag"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_facts; DROP TABLE t_dim; DROP TABLE t_prefix; DROP TABLE t_split_fragment"
