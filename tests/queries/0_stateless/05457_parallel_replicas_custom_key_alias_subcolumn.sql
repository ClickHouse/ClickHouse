-- `parallel_replicas_custom_key` may reference a subcolumn of an `ALIAS` column (`a.v`), possibly qualified with
-- the table name (`t.a.v`). A subquery hidden in such a column must be rejected the same way as when the whole
-- column is referenced.

DROP TABLE IF EXISTS custom_key_alias_subcolumn;
DROP TABLE IF EXISTS custom_key_alias_subcolumn_set;

CREATE TABLE custom_key_alias_subcolumn_set (id UInt64) ENGINE = Set;
INSERT INTO custom_key_alias_subcolumn_set VALUES (1), (2);

CREATE TABLE custom_key_alias_subcolumn
(
    x UInt64,
    a Tuple(v UInt8) ALIAS tuple(x IN custom_key_alias_subcolumn_set),
    b Tuple(v UInt64) ALIAS tuple(x * 7)
)
ENGINE = MergeTree ORDER BY x;
INSERT INTO custom_key_alias_subcolumn SELECT number FROM numbers(10);

-- The custom key filter is applied locally through `parallel_replicas_count` and `parallel_replica_offset`,
-- so the randomized reading from parallel replicas of the test runner must not interfere.
SET automatic_parallel_replicas_mode = 0;
SET cluster_for_parallel_replicas = '';
SET parallel_replicas_for_non_replicated_merge_tree = 0;
SET allow_experimental_parallel_reading_from_replicas = 1;
SET max_parallel_replicas = 2;
SET parallel_replicas_count = 2;
SET parallel_replica_offset = 0;

SELECT count() FROM custom_key_alias_subcolumn
SETTINGS parallel_replicas_custom_key = 'tupleElement(a, 1)', parallel_replicas_mode = 'custom_key_sampling'; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM custom_key_alias_subcolumn
SETTINGS parallel_replicas_custom_key = 'a.v', parallel_replicas_mode = 'custom_key_sampling'; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM custom_key_alias_subcolumn
SETTINGS parallel_replicas_custom_key = 'toUInt64(a.v)', parallel_replicas_mode = 'custom_key_range'; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM custom_key_alias_subcolumn AS t
SETTINGS parallel_replicas_custom_key = 't.a.v', parallel_replicas_mode = 'custom_key_sampling'; -- { serverError BAD_ARGUMENTS }

-- A subcolumn of an `ALIAS` column without forbidden constructs keeps working.
SELECT count() FROM custom_key_alias_subcolumn
SETTINGS parallel_replicas_custom_key = 'b.v', parallel_replicas_mode = 'custom_key_sampling';

DROP TABLE custom_key_alias_subcolumn;
DROP TABLE custom_key_alias_subcolumn_set;
