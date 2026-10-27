-- `parallel_replicas_custom_key` may reference an `ALIAS` column qualified with the table name (`t.a`) or with
-- the database and table names. A subquery hidden in such a column must be rejected the same way as when the
-- column is referenced unqualified.

DROP TABLE IF EXISTS custom_key_qualified_alias;
DROP TABLE IF EXISTS custom_key_qualified_alias_set;

CREATE TABLE custom_key_qualified_alias_set (id UInt64) ENGINE = Set;
INSERT INTO custom_key_qualified_alias_set VALUES (1), (2);

CREATE TABLE custom_key_qualified_alias
(
    x UInt64,
    a ALIAS x IN custom_key_qualified_alias_set,
    b ALIAS x * 7
)
ENGINE = MergeTree ORDER BY x;
INSERT INTO custom_key_qualified_alias SELECT number FROM numbers(10);

SET allow_experimental_parallel_reading_from_replicas = 1;
SET max_parallel_replicas = 2;
SET parallel_replicas_count = 2;
SET parallel_replica_offset = 0;

SELECT count() FROM custom_key_qualified_alias
SETTINGS parallel_replicas_custom_key = 'a', parallel_replicas_mode = 'custom_key_sampling'; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM custom_key_qualified_alias
SETTINGS parallel_replicas_custom_key = 'custom_key_qualified_alias.a', parallel_replicas_mode = 'custom_key_sampling'; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM custom_key_qualified_alias
SETTINGS parallel_replicas_custom_key = 'toUInt64(custom_key_qualified_alias.a)', parallel_replicas_mode = 'custom_key_range'; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM custom_key_qualified_alias AS t
SETTINGS parallel_replicas_custom_key = 't.a', parallel_replicas_mode = 'custom_key_sampling'; -- { serverError BAD_ARGUMENTS }

-- A qualified `ALIAS` column without forbidden constructs keeps working.
SELECT count() FROM custom_key_qualified_alias
SETTINGS parallel_replicas_custom_key = 'custom_key_qualified_alias.b', parallel_replicas_mode = 'custom_key_sampling';

DROP TABLE custom_key_qualified_alias;
DROP TABLE custom_key_qualified_alias_set;
