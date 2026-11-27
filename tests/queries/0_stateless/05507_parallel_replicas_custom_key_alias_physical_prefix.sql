-- A qualified identifier in `parallel_replicas_custom_key` whose qualifier is also the name of a physical
-- column (`q.a` with a column `q UInt64`) does not refer to that column: the analyzer resolves `q` as the
-- table qualifier and `a` as the `ALIAS` column, so the alias expression must be validated.

DROP TABLE IF EXISTS q;
DROP TABLE IF EXISTS custom_key_alias_physical_prefix_set;

CREATE TABLE custom_key_alias_physical_prefix_set (id UInt64) ENGINE = Set;
INSERT INTO custom_key_alias_physical_prefix_set VALUES (1), (2);

CREATE TABLE q
(
    x UInt64,
    q UInt64,
    t Tuple(a UInt64),
    a UInt8 ALIAS x IN custom_key_alias_physical_prefix_set,
    harmless UInt64 ALIAS x * 3
)
ENGINE = MergeTree ORDER BY x;
INSERT INTO q SELECT number, 0, tuple(number) FROM numbers(4);

-- The custom key filter is applied locally through `parallel_replicas_count` and `parallel_replica_offset`,
-- so the randomized reading from parallel replicas of the test runner must not interfere.
SET automatic_parallel_replicas_mode = 0;
SET cluster_for_parallel_replicas = '';
SET parallel_replicas_for_non_replicated_merge_tree = 0;
SET allow_experimental_parallel_reading_from_replicas = 1;
SET max_parallel_replicas = 2;
SET parallel_replicas_count = 2;
SET parallel_replica_offset = 0;

SELECT count() FROM q AS q
SETTINGS parallel_replicas_custom_key = 'q.a', parallel_replicas_mode = 'custom_key_sampling'; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM q AS q
SETTINGS parallel_replicas_custom_key = 'toUInt64(q.a)', parallel_replicas_mode = 'custom_key_range'; -- { serverError BAD_ARGUMENTS }

-- A subcolumn of a physical column that is named like the `ALIAS` column still resolves to the subcolumn.
SELECT arraySort(groupArray(x)) FROM (SELECT x FROM q AS q
SETTINGS parallel_replicas_custom_key = 't.a', parallel_replicas_mode = 'custom_key_sampling');

-- A harmless alias behind the same qualifier keeps working.
SELECT arraySort(groupArray(x)) FROM (SELECT x FROM q AS q
SETTINGS parallel_replicas_custom_key = 'q.harmless', parallel_replicas_mode = 'custom_key_sampling');

DROP TABLE q;
DROP TABLE custom_key_alias_physical_prefix_set;
