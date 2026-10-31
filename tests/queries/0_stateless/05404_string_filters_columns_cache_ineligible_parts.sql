-- The readers do not apply string value filters during the scan to the parts whose columns they may write
-- to the columns cache. The optimizer still moves the conditions to PREWHERE: the decision does not depend
-- on the columns cache. Only a part of a table with a UUID can be written to the cache: a table in an
-- `Ordinary` database keeps the filters with the columns cache enabled. Both compact and wide parts are
-- written to the cache.

SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, optimize_prewhere_after_pushdown = 1;
SET apply_string_filters_during_scan = 1;
SET use_columns_cache = 1, enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 1;
SET allow_deprecated_database_ordinary = 1;
-- The profile events of the reads are checked in the log entry of the query, not of the queries to the replicas.
SET enable_parallel_replicas = 0;
-- The warning about the deprecated `Ordinary` engine.
SET send_logs_level = 'error';

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Ordinary;

CREATE TABLE t_compact (id UInt32, s String) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000, ratio_of_defaults_for_sparse_serialization = 1.0;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_ordinary (id UInt32, s String) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1.0;

CREATE TABLE t_wide (id UInt32, s String) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1.0;

INSERT INTO t_compact SELECT number, if(number % 100 = 0, 'needle ', 'haystack ') || toString(number) FROM numbers(10000);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_ordinary SELECT * FROM t_compact;
INSERT INTO t_wide SELECT * FROM t_compact;

SELECT 'compact part';
SELECT count() FROM t_compact PREWHERE s LIKE '%needle%' SETTINGS log_comment = '05404_compact';
SELECT countIf(explain LIKE '%Prewhere filter column%LIKE%') > 0 FROM (
    EXPLAIN actions = 1 SELECT s FROM t_compact WHERE s LIKE '%needle%');

SELECT 'Ordinary database';
SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_ordinary PREWHERE s LIKE '%needle%' SETTINGS log_comment = '05404_ordinary';
SELECT countIf(explain LIKE '%Prewhere filter column%LIKE%') > 0 FROM (
    EXPLAIN actions = 1 SELECT s FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_ordinary WHERE s LIKE '%needle%');

SELECT 'wide part';
SELECT count() FROM t_wide PREWHERE s LIKE '%needle%' SETTINGS log_comment = '05404_wide';
SELECT countIf(explain LIKE '%Prewhere filter column%LIKE%') > 0 FROM (
    EXPLAIN actions = 1 SELECT s FROM t_wide WHERE s LIKE '%needle%');

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['StringValueFilterValuesChecked'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05404_compact', '05404_ordinary', '05404_wide')
ORDER BY log_comment;

DROP TABLE t_compact;
DROP TABLE t_wide;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
