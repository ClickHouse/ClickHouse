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

-- The view reads a `Distributed` table: the slice must survive the next cluster hop.
CREATE TABLE inner_d_sampling AS t_sampling ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_sampling);
CREATE TABLE inner_d_plain AS t_plain ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_plain);
CREATE VIEW v_none_nested SQL SECURITY NONE AS SELECT id FROM inner_d_plain;
CREATE VIEW v_definer_nested DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT id FROM inner_d_sampling;
CREATE TABLE d_none_nested AS v_none_nested ENGINE = Distributed(test_cluster_one_shard_two_replicas, currentDatabase(), v_none_nested);
CREATE TABLE d_definer_nested AS v_definer_nested ENGINE = Distributed(test_cluster_one_shard_two_replicas, currentDatabase(), v_definer_nested);

SET enable_parallel_replicas = 1, max_parallel_replicas = 2, parallel_replicas_mode = 'sampling_key',
    prefer_localhost_replica = 0, serialize_query_plan = 0, optimize_trivial_count_query = 0;

SELECT count(), sum(id) FROM d_none_sampling;
SELECT count(), sum(id) FROM d_definer_sampling;
SELECT count(), sum(id) FROM d_none_plain;
SELECT count(), sum(id) FROM d_none_nested;
SELECT count(), sum(id) FROM d_definer_nested;
SELECT count(), sum(id) FROM d_none_nested SETTINGS analyzer_inline_views = 1;

DROP TABLE d_none_sampling;
DROP TABLE d_definer_sampling;
DROP TABLE d_none_plain;
DROP TABLE d_none_nested;
DROP TABLE d_definer_nested;
DROP VIEW v_none_nested;
DROP VIEW v_definer_nested;
DROP TABLE inner_d_sampling;
DROP TABLE inner_d_plain;
DROP VIEW v_none_sampling;
DROP VIEW v_definer_sampling;
DROP VIEW v_none_plain;
DROP TABLE t_sampling;
DROP TABLE t_plain;
