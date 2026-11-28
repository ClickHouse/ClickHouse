#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
# no-parallel-replicas: the test asserts exact per-reader granule counts for a single-server read plan.
#
# A granule is not dropped by PREWHERE when PREWHERE rejects its first block fragment and the top-K threshold
# skips the rest of it unread (`use_skip_indexes_on_data_read`). Rows 12288..16383 of granule 1 satisfy `c = 1`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
  DROP TABLE IF EXISTS t_topk_remainder;
  CREATE TABLE t_topk_remainder(k UInt8, b UInt32, c UInt8) ENGINE=MergeTree ORDER BY (k, b)
    SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0, index_granularity=8192, add_minmax_index_for_numeric_columns=0;
  INSERT INTO t_topk_remainder SELECT 0, number, number < 8192 OR number >= 12288 FROM numbers(24576)
    SETTINGS min_insert_block_size_rows = 24577, min_insert_block_size_bytes = 0;
"

for skip_on_read in 0 1; do
  query_id="${CLICKHOUSE_DATABASE}_topk_remainder_${skip_on_read}_$RANDOM$RANDOM"
  ${CLICKHOUSE_CLIENT} --max_threads 1 --max_block_size 12288 --preferred_block_size_bytes 0 \
    --use_query_condition_cache 0 --use_top_k_dynamic_filtering 1 --use_skip_indexes_on_data_read $skip_on_read \
    --query_id "$query_id" -q "SELECT b FROM t_topk_remainder PREWHERE c = 1 ORDER BY b LIMIT 10 FORMAT Null"

  ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
  ${CLICKHOUSE_CLIENT} -q "
    SELECT
      ProfileEvents['GranulesReadByPrewhereReaders'],
      ProfileEvents['GranulesSkippedByPrewhereReaders'],
      ProfileEvents['RowsSkippedByPrewhereReaders'],
      ProfileEvents['TopKGranulesSkippedByPrimaryKey'] > 0
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND current_database = currentDatabase() AND query_id = '$query_id' AND type = 'QueryFinish'
  "
done

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_topk_remainder"
