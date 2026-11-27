SET allow_experimental_shuffle_query = 1;
SET max_threads = 4, max_block_size = 1000;
SET query_plan_enable_optimizations = 1, query_plan_push_down_limit = 1;

-- An outer `LIMIT` must sample the whole population, not only its prefix.
SELECT count() = 1000, min(number) < 10000, max(number) > 90000
FROM (SELECT number FROM (SELECT number FROM numbers(100000) SHUFFLE) LIMIT 1000)
SETTINGS max_threads = 1;

-- A shuffled view over `Distributed` is not a trivial view eligible for shard pushdown.
SET analyzer_inline_views = 0, optimize_trivial_view_pushdown_to_distributed = 1;
DROP VIEW IF EXISTS shuffle_boundary_view;
DROP TABLE IF EXISTS shuffle_boundary_dist;
DROP TABLE IF EXISTS shuffle_boundary_local;
CREATE TABLE shuffle_boundary_local (number UInt64) ENGINE = Memory;
INSERT INTO shuffle_boundary_local SELECT number FROM numbers(100000);
CREATE TABLE shuffle_boundary_dist AS shuffle_boundary_local
ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), shuffle_boundary_local);
CREATE VIEW shuffle_boundary_view SQL SECURITY INVOKER AS
SELECT number, _shard_num AS s FROM shuffle_boundary_dist SHUFFLE;

SELECT count() = 10000, avg(s = 1) BETWEEN 0.45 AND 0.55
FROM (SELECT * FROM shuffle_boundary_view LIMIT 10000);

DROP VIEW shuffle_boundary_view;
DROP TABLE shuffle_boundary_dist;
DROP TABLE shuffle_boundary_local;
