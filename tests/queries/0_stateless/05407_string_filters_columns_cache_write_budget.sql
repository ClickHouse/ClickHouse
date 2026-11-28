-- The readers do not apply string value filters during the scan when they may write the columns to the
-- columns cache. When the estimate of the query is over `columns_cache_max_estimated_bytes_to_write_to_cache`,
-- the query writes nothing to the cache, so the readers keep the filters.

SET apply_string_filters_during_scan = 1;
SET use_columns_cache = 1, enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 1;
-- A read ranges refiner makes the pool skip the estimate gate.
SET use_indexes_refiner_in_read_pools = 0;
-- The profile events of the reads are checked in the log entry of the query, not of the queries to the replicas.
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_string_filter_write_budget;

CREATE TABLE t_string_filter_write_budget (id UInt32, s String) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1.0;

INSERT INTO t_string_filter_write_budget SELECT number, if(number % 100 = 0, 'needle ', 'haystack ') || toString(number) FROM numbers(10000);

SELECT count(), sum(length(s)) FROM t_string_filter_write_budget PREWHERE s LIKE '%needle%'
SETTINGS columns_cache_max_estimated_bytes_to_write_to_cache = 1, log_comment = '05407_over_budget';

SELECT count() FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_string_filter_write_budget';

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['StringValueFilterValuesChecked'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05407_over_budget';

DROP TABLE t_string_filter_write_budget;
