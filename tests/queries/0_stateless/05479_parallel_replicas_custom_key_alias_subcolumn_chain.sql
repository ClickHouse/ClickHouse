-- `parallel_replicas_custom_key` may reference an `ALIAS` column whose expression is a subcolumn of another
-- `ALIAS` column (`b ALIAS a.v`). A subquery hidden at the end of such a chain must be rejected too.

DROP TABLE IF EXISTS custom_key_alias_chain;
DROP TABLE IF EXISTS custom_key_alias_chain_set;

CREATE TABLE custom_key_alias_chain_set (id UInt64) ENGINE = Set;
INSERT INTO custom_key_alias_chain_set VALUES (1), (2);

CREATE TABLE custom_key_alias_chain
(
    x UInt64,
    a Tuple(v UInt8) ALIAS tuple(x IN custom_key_alias_chain_set),
    b UInt8 ALIAS a.v,
    c UInt8 ALIAS toUInt8(b + 1),
    d Tuple(w UInt64) ALIAS tuple(x * 7),
    e UInt64 ALIAS d.w
)
ENGINE = MergeTree ORDER BY x;
INSERT INTO custom_key_alias_chain SELECT number FROM numbers(10);

-- The custom key filter is applied locally through `parallel_replicas_count` and `parallel_replica_offset`,
-- so the randomized reading from parallel replicas of the test runner must not interfere.
SET automatic_parallel_replicas_mode = 0;
SET cluster_for_parallel_replicas = '';
SET parallel_replicas_for_non_replicated_merge_tree = 0;
SET allow_experimental_parallel_reading_from_replicas = 1;
SET max_parallel_replicas = 2;
SET parallel_replicas_count = 2;
SET parallel_replica_offset = 0;

SELECT count() FROM custom_key_alias_chain
SETTINGS parallel_replicas_custom_key = 'b', parallel_replicas_mode = 'custom_key_sampling'; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM custom_key_alias_chain
SETTINGS parallel_replicas_custom_key = 'toUInt64(b)', parallel_replicas_mode = 'custom_key_range'; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM custom_key_alias_chain AS t
SETTINGS parallel_replicas_custom_key = 't.c', parallel_replicas_mode = 'custom_key_sampling'; -- { serverError BAD_ARGUMENTS }

-- A chain without forbidden constructs keeps working.
SELECT count() FROM custom_key_alias_chain
SETTINGS parallel_replicas_custom_key = 'e', parallel_replicas_mode = 'custom_key_sampling';

DROP TABLE custom_key_alias_chain;
DROP TABLE custom_key_alias_chain_set;
