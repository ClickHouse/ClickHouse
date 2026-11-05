-- A cached plan must be revalidated against everything it was built from:
-- 1. the definitions of `ALIAS` columns that were expanded only to compute another `ALIAS` column;
-- 2. the absence of a table column that would shadow an identifier (a new column `b` takes precedence
--    over `WITH 100 AS b` under `prefer_column_name_to_alias`);
-- 3. the query's limits on temporary data, which a cached aggregation must keep enforcing when it spills.

SET enable_query_plan_cache = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS qpc_05508_alias;
DROP TABLE IF EXISTS qpc_05508_shadow;
DROP TABLE IF EXISTS qpc_05508_spill;

CREATE TABLE qpc_05508_alias (a UInt64, y UInt64 ALIAS a + 1, z UInt64 ALIAS y + 1) ENGINE = MergeTree ORDER BY a;
INSERT INTO qpc_05508_alias VALUES (1);

SELECT z FROM qpc_05508_alias SETTINGS log_comment = 'qpc_05508_alias_seed';
SELECT z FROM qpc_05508_alias SETTINGS log_comment = 'qpc_05508_alias_hit';
ALTER TABLE qpc_05508_alias MODIFY COLUMN y UInt64 ALIAS a + 10;
SELECT z FROM qpc_05508_alias SETTINGS log_comment = 'qpc_05508_alias_after_modify';

CREATE TABLE qpc_05508_shadow (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO qpc_05508_shadow VALUES (1);

WITH 100 AS b SELECT b FROM qpc_05508_shadow SETTINGS prefer_column_name_to_alias = 1, log_comment = 'qpc_05508_shadow_seed';
-- A column the query does not mention leaves the entry valid.
ALTER TABLE qpc_05508_shadow ADD COLUMN c UInt64 DEFAULT 300;
WITH 100 AS b SELECT b FROM qpc_05508_shadow SETTINGS prefer_column_name_to_alias = 1, log_comment = 'qpc_05508_shadow_after_unrelated_add';
-- A column named like the alias changes the result.
ALTER TABLE qpc_05508_shadow ADD COLUMN b UInt64 DEFAULT 200;
WITH 100 AS b SELECT b FROM qpc_05508_shadow SETTINGS prefer_column_name_to_alias = 1, log_comment = 'qpc_05508_shadow_after_add';

CREATE TABLE qpc_05508_spill (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO qpc_05508_spill SELECT number FROM numbers(200000);

-- Both the miss and the hit spill the aggregation state and must hit the temporary data limit.
SELECT k, count() FROM qpc_05508_spill GROUP BY k FORMAT Null
SETTINGS max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0, max_temporary_data_on_disk_size_for_query = 1, log_comment = 'qpc_05508_spill_seed'; -- { serverError TOO_MANY_ROWS_OR_BYTES }
SELECT k, count() FROM qpc_05508_spill GROUP BY k FORMAT Null
SETTINGS max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0, max_temporary_data_on_disk_size_for_query = 1, log_comment = 'qpc_05508_spill_hit'; -- { serverError TOO_MANY_ROWS_OR_BYTES }

SYSTEM FLUSH LOGS query_log;
SELECT
    log_comment,
    ProfileEvents['QueryPlanCacheHits'],
    ProfileEvents['QueryPlanCacheValidationMisses']
FROM system.query_log
WHERE type != 'QueryStart'
  AND current_database = currentDatabase()
  AND event_date >= yesterday()
  AND startsWith(log_comment, 'qpc_05508_')
  AND NOT endsWith(log_comment, '_seed')
ORDER BY log_comment;

DROP TABLE qpc_05508_alias;
DROP TABLE qpc_05508_shadow;
DROP TABLE qpc_05508_spill;
