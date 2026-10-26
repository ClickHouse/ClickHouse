-- A replica that reads a `SQL SECURITY DEFINER` / `NONE` view for `sampling_key` parallel replicas
-- must read only its slice of the view, not the whole of it.

DROP TABLE IF EXISTS t_sampling;
DROP TABLE IF EXISTS t_plain;
CREATE TABLE t_sampling (id UInt64) ENGINE = MergeTree ORDER BY intHash32(id) SAMPLE BY intHash32(id);
CREATE TABLE t_plain (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_sampling SELECT number FROM numbers(100);
INSERT INTO t_plain SELECT number FROM numbers(100);

CREATE VIEW v_none_sampling SQL SECURITY NONE AS SELECT id FROM t_sampling;
CREATE VIEW v_definer_sampling DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT id FROM t_sampling;
CREATE VIEW v_none_plain SQL SECURITY NONE AS SELECT id FROM t_plain;

CREATE TABLE d_none_sampling AS v_none_sampling ENGINE = Distributed(test_cluster_one_shard_two_replicas, currentDatabase(), v_none_sampling);
CREATE TABLE d_definer_sampling AS v_definer_sampling ENGINE = Distributed(test_cluster_one_shard_two_replicas, currentDatabase(), v_definer_sampling);
CREATE TABLE d_none_plain AS v_none_plain ENGINE = Distributed(test_cluster_one_shard_two_replicas, currentDatabase(), v_none_plain);

SET enable_parallel_replicas = 1, max_parallel_replicas = 2, parallel_replicas_mode = 'sampling_key',
    prefer_localhost_replica = 0, serialize_query_plan = 0, optimize_trivial_count_query = 0;

SELECT count(), sum(id) FROM d_none_sampling;
SELECT count(), sum(id) FROM d_definer_sampling;
SELECT count(), sum(id) FROM d_none_plain;

DROP TABLE d_none_sampling;
DROP TABLE d_definer_sampling;
DROP TABLE d_none_plain;
DROP VIEW v_none_sampling;
DROP VIEW v_definer_sampling;
DROP VIEW v_none_plain;
DROP TABLE t_sampling;
DROP TABLE t_plain;
