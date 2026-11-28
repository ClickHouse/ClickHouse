-- A read that needs no column of the table reads the column that is the smallest on disk,
-- with `query_plan_remove_unused_columns = 1` as with 0.

DROP TABLE IF EXISTS t_no_column_read;

CREATE TABLE t_no_column_read (k UInt64, cheap UInt64 CODEC(LZ4), narrow UInt8 CODEC(LZ4))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = '10Mi';

-- `narrow` has the smallest type but random values, `cheap` is the smallest on disk.
INSERT INTO t_no_column_read SELECT number, 0, rand() FROM numbers(1000000);

-- After a lightweight delete `count()` has to scan the table.
DELETE FROM t_no_column_read WHERE k = 7 SETTINGS lightweight_deletes_sync = 2;

SELECT count() FROM t_no_column_read
SETTINGS query_plan_remove_unused_columns = 1, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0,
    use_uncompressed_cache = 0, use_columns_cache = 0, log_comment = '05629-count';

SELECT isNullable(narrow) FROM t_no_column_read FORMAT Null
SETTINGS query_plan_remove_unused_columns = 1, use_uncompressed_cache = 0, use_columns_cache = 0, log_comment = '05629-isnullable';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, max(ProfileEvents['ReadCompressedBytes']) < 200000
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05629-%'
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE t_no_column_read;
