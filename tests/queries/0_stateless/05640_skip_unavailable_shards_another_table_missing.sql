-- Tags: shard

-- `skip_unavailable_shards` skips a shard without the table the query is distributed over,
-- but not a shard where the query fails because some other table is missing.

SET skip_unavailable_shards = 1, serialize_query_plan = 0, enable_parallel_replicas = 0;
SET parallel_distributed_insert_select = 2, distributed_foreground_insert = 1;

CREATE TABLE t (x UInt8) ENGINE = MergeTree ORDER BY x AS SELECT number FROM numbers(3);
CREATE TABLE dist AS t ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t);
CREATE TABLE dst AS t;
CREATE TABLE dist_dst AS t ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), dst, x);

-- Where this name is sent without the database, the first shard (local) has the table, and the second one (remote) does not.
CREATE TABLE ref_05640 (x UInt8) ENGINE = Memory AS SELECT 1;
SET prefer_localhost_replica = 1;

SELECT count() FROM dist WHERE x NOT IN (SELECT x FROM view(SELECT x FROM ref_05640)); -- { serverError UNKNOWN_TABLE }
SELECT count() FROM dist WHERE x NOT IN (SELECT x FROM view(SELECT x FROM ref_05640)) SETTINGS skip_unavailable_shards_mode = 'unavailable_or_exception_before_processing';
SELECT count() FROM dist WHERE x GLOBAL NOT IN (SELECT x FROM view(SELECT x FROM ref_05640));
SELECT count() FROM cluster(test_cluster_two_shards, numbers(3)) WHERE number NOT IN (SELECT x FROM view(SELECT x FROM ref_05640)); -- { serverError UNKNOWN_TABLE }
SELECT count() FROM cluster(test_cluster_two_shards, view(SELECT x FROM ref_05640)); -- { serverError UNKNOWN_TABLE }
INSERT INTO dist_dst SELECT x FROM dist WHERE x NOT IN (SELECT x FROM ref_05640); -- { serverError UNKNOWN_TABLE }

SET prefer_localhost_replica = 0;

-- Cluster table functions enable `skip_unavailable_shards` on their own.
INSERT INTO FUNCTION file(currentDatabase() || '_05640.tsv', TSV, 'x UInt8') SELECT x FROM t SETTINGS engine_file_truncate_on_insert = 1;
SELECT count() FROM fileCluster(test_cluster_two_shards, currentDatabase() || '_05640.tsv', TSV, 'x UInt8')
WHERE x NOT IN (SELECT x FROM view(SELECT x FROM ref_05640)) SETTINGS skip_unavailable_shards = 0; -- { serverError UNKNOWN_TABLE }

-- A view without its table.
CREATE TABLE src AS t;
CREATE VIEW v AS SELECT x FROM src;
CREATE TABLE dist_v AS t ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), v);
DROP TABLE src;
SELECT count() FROM dist_v; -- { serverError UNKNOWN_TABLE }
INSERT INTO dist_dst SELECT x FROM dist_v; -- { serverError UNKNOWN_TABLE }

-- A materialized view without its target.
CREATE TABLE mv_dst AS t;
CREATE MATERIALIZED VIEW mv TO mv_dst AS SELECT x FROM dst;
DROP TABLE mv_dst;
INSERT INTO dist_dst VALUES (1), (2); -- { serverError UNKNOWN_TABLE }
INSERT INTO dist_dst SELECT x FROM dist; -- { serverError UNKNOWN_TABLE }
