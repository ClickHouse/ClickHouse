-- The custom key for parallel replicas is checked on the binding the analyzer makes also when the
-- initiator routes the read to the replicas of a cluster (`parallel_replicas_count = 0`), where the
-- replicas receive a filter generated from the key as written.

DROP TABLE IF EXISTS q;
DROP TABLE IF EXISTS ext_set_05620;

CREATE TABLE ext_set_05620 (id UInt64) ENGINE = Set;
INSERT INTO ext_set_05620 VALUES (1), (2);

CREATE TABLE q (x UInt64, q UInt64 ALIAS x * 3, a UInt8 ALIAS x IN ext_set_05620) ENGINE = MergeTree ORDER BY x;
INSERT INTO q SELECT number FROM numbers(4);

SET automatic_parallel_replicas_mode = 0, allow_experimental_parallel_reading_from_replicas = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas',
    max_parallel_replicas = 2, parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_count = 0,
    serialize_query_plan = 0;

SELECT count() FROM q AS q SETTINGS parallel_replicas_custom_key = 'q.a'; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM q AS q SETTINGS parallel_replicas_custom_key = 'a'; -- { serverError BAD_ARGUMENTS }

-- The harmless `ALIAS` column still works.
SELECT count() FROM q AS q SETTINGS parallel_replicas_custom_key = 'q';

DROP TABLE q;
DROP TABLE ext_set_05620;
