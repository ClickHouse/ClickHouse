-- An identifier in `parallel_replicas_custom_key` that is qualified like an existing table (`system.one`)
-- still refers to an `ALIAS` column of the source table when the source is aliased as `system`, so the
-- alias expression must be validated. The catalog table is considered only on the right-hand side of `IN`.

DROP TABLE IF EXISTS custom_key_alias_catalog_collision;
DROP TABLE IF EXISTS custom_key_alias_catalog_collision_set;

CREATE TABLE custom_key_alias_catalog_collision_set (id UInt64) ENGINE = Set;
INSERT INTO custom_key_alias_catalog_collision_set VALUES (1), (2);

CREATE TABLE custom_key_alias_catalog_collision
(
    x UInt64,
    one UInt8 ALIAS x IN custom_key_alias_catalog_collision_set,
    harmless UInt64 ALIAS x * 3
)
ENGINE = MergeTree ORDER BY x;
INSERT INTO custom_key_alias_catalog_collision SELECT number FROM numbers(4);

-- The custom key filter is applied locally through `parallel_replicas_count` and `parallel_replica_offset`,
-- so the randomized reading from parallel replicas of the test runner must not interfere.
SET automatic_parallel_replicas_mode = 0;
SET cluster_for_parallel_replicas = '';
SET parallel_replicas_for_non_replicated_merge_tree = 0;
SET allow_experimental_parallel_reading_from_replicas = 1;
SET max_parallel_replicas = 2;
SET parallel_replicas_count = 2;
SET parallel_replica_offset = 0;

SELECT count() FROM custom_key_alias_catalog_collision AS system
SETTINGS parallel_replicas_custom_key = 'system.one', parallel_replicas_mode = 'custom_key_sampling'; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM custom_key_alias_catalog_collision AS system
SETTINGS parallel_replicas_custom_key = 'toUInt64(system.one)', parallel_replicas_mode = 'custom_key_range'; -- { serverError BAD_ARGUMENTS }

-- A table on the right-hand side of `IN` is still rejected.
SELECT count() FROM custom_key_alias_catalog_collision AS system
SETTINGS parallel_replicas_custom_key = 'x IN system.one', parallel_replicas_mode = 'custom_key_sampling'; -- { serverError BAD_ARGUMENTS }

-- A harmless alias keeps working.
SELECT arraySort(groupArray(x)) FROM (SELECT x FROM custom_key_alias_catalog_collision AS system
SETTINGS parallel_replicas_custom_key = 'harmless', parallel_replicas_mode = 'custom_key_sampling');

DROP TABLE custom_key_alias_catalog_collision;
DROP TABLE custom_key_alias_catalog_collision_set;
