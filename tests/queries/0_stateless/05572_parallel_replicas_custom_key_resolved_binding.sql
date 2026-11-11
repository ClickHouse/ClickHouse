-- The custom key for parallel replicas must depend only on the row. Besides the checks of the key as
-- written, the binding the analyzer makes is checked: a table on the right-hand side of `IN` reached
-- through an `ALIAS` column is rejected however the column is spelled.

DROP TABLE IF EXISTS q;
DROP TABLE IF EXISTS ext_set_05572;

CREATE TABLE ext_set_05572 (id UInt64) ENGINE = Set;
INSERT INTO ext_set_05572 VALUES (1), (2);

-- `q.a` over `FROM q AS q`: `q` is a harmless `ALIAS` column of a scalar type, so `q.a` is not its
-- subcolumn, and the analyzer resolves `a` with the table qualifier stripped.
CREATE TABLE q (x UInt64, q UInt64 ALIAS x * 3, a UInt8 ALIAS x IN ext_set_05572) ENGINE = MergeTree ORDER BY x;
INSERT INTO q SELECT number FROM numbers(4);

-- Only the custom key filter on this server acting as a replica, whatever settings are randomized.
SET automatic_parallel_replicas_mode = 0, cluster_for_parallel_replicas = '', parallel_replicas_for_non_replicated_merge_tree = 0,
    serialize_query_plan = 0;
SET parallel_replicas_mode = 'custom_key_sampling', max_parallel_replicas = 2, allow_experimental_parallel_reading_from_replicas = 1,
    parallel_replicas_count = 2, parallel_replica_offset = 0;

SELECT count() FROM q AS q SETTINGS parallel_replicas_custom_key = 'q.a'; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM q AS q SETTINGS parallel_replicas_custom_key = 'a'; -- { serverError BAD_ARGUMENTS }

-- The harmless `ALIAS` column still works.
SELECT count() FROM q AS q SETTINGS parallel_replicas_custom_key = 'q';

DROP TABLE q;
DROP TABLE ext_set_05572;
