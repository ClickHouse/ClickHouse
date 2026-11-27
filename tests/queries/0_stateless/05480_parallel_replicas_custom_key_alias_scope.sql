-- The expression of an `ALIAS` column referenced by `parallel_replicas_custom_key` is resolved at table scope,
-- even inside a lambda whose argument shadows a column it uses. A table hidden there must be rejected.
-- A qualified table on the right-hand side of `IN` must be rejected even when an `ALIAS` column of the
-- source table is named like the table (`one` and `system.one`).

DROP TABLE IF EXISTS custom_key_alias_scope;
DROP TABLE IF EXISTS custom_key_alias_scope_set;

CREATE TABLE custom_key_alias_scope_set (id UInt64) ENGINE = Set;
INSERT INTO custom_key_alias_scope_set VALUES (0), (2);

CREATE TABLE custom_key_alias_scope
(
    x UInt64,
    a Tuple(v UInt8) ALIAS tuple(x IN custom_key_alias_scope_set),
    b UInt8 ALIAS a.v,
    one Array(UInt64) ALIAS [toUInt64(1)],
    d Tuple(w UInt64) ALIAS tuple(x * 7),
    e UInt64 ALIAS d.w
)
ENGINE = MergeTree ORDER BY x;
INSERT INTO custom_key_alias_scope SELECT number FROM numbers(4);

-- The custom key filter is applied locally through `parallel_replicas_count` and `parallel_replica_offset`,
-- so the randomized reading from parallel replicas of the test runner must not interfere.
SET automatic_parallel_replicas_mode = 0;
SET cluster_for_parallel_replicas = '';
SET parallel_replicas_for_non_replicated_merge_tree = 0;
SET allow_experimental_parallel_reading_from_replicas = 1;
SET max_parallel_replicas = 2;
SET parallel_replicas_count = 2;
SET parallel_replica_offset = 0;

SELECT count() FROM custom_key_alias_scope
SETTINGS parallel_replicas_custom_key = 'arraySum(arrayMap(a -> b, [0]))', parallel_replicas_mode = 'custom_key_sampling'; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM custom_key_alias_scope
SETTINGS parallel_replicas_custom_key = 'arraySum(arrayMap(x -> b, [0]))', parallel_replicas_mode = 'custom_key_sampling'; -- { serverError BAD_ARGUMENTS }

SELECT groupArray(x) FROM custom_key_alias_scope
SETTINGS parallel_replicas_custom_key = 'x IN system.one', parallel_replicas_mode = 'custom_key_sampling'; -- { serverError BAD_ARGUMENTS }

-- The alias column itself, unqualified, is an array.
SELECT arraySort(groupArray(x)) FROM (SELECT x FROM custom_key_alias_scope
SETTINGS parallel_replicas_custom_key = 'x IN one', parallel_replicas_mode = 'custom_key_sampling');

-- A harmless chain inside a lambda that shadows its root keeps working.
SELECT count() FROM custom_key_alias_scope
SETTINGS parallel_replicas_custom_key = 'arraySum(arrayMap(d -> e, [0]))', parallel_replicas_mode = 'custom_key_sampling';

DROP TABLE custom_key_alias_scope;
DROP TABLE custom_key_alias_scope_set;
