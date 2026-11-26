-- Tags: no-random-settings
-- With the plan-level distributed read (`serialize_query_plan = 1` and
-- `query_plan_optimize_distributed_lazy_materialization = 1`) but without the coordinated Top-K
-- (here because `query_plan_optimize_lazy_materialization = 0`), `ORDER BY ... LIMIT` over a
-- `Distributed` table must still sort and limit on each shard, so that a shard sends at most
-- `limit + offset` rows instead of all of them.
-- DISTINCT and LIMIT BY keep the classic path, which applies them on the shards as well.

SET serialize_query_plan = 1;
SET query_plan_optimize_distributed_lazy_materialization = 1;
SET query_plan_optimize_lazy_materialization = 0;
SET distributed_push_down_limit = 1;
SET enable_parallel_replicas = 0;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS sort_limit_local;
DROP TABLE IF EXISTS sort_limit_dist;

CREATE TABLE sort_limit_local (key UInt64, payload String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE sort_limit_dist AS sort_limit_local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), sort_limit_local);

INSERT INTO sort_limit_local SELECT number, toString(number) FROM numbers(1000);

SELECT '-- Each shard sorts and keeps limit + offset rows';
EXPLAIN distributed = 1 SELECT payload FROM sort_limit_dist ORDER BY key DESC LIMIT 3 OFFSET 2;

SELECT '-- Shard limits are limit + offset, sorted on each shard with that limit';
SELECT count() FROM (EXPLAIN actions = 1, distributed = 1 SELECT payload FROM sort_limit_dist ORDER BY key DESC LIMIT 3 OFFSET 2) WHERE trim(explain) = 'Limit 5';

SELECT '-- Same result as the classic path';
SELECT payload FROM sort_limit_dist ORDER BY key DESC LIMIT 3 OFFSET 2;
SELECT payload FROM sort_limit_dist ORDER BY key DESC LIMIT 3 OFFSET 2 SETTINGS serialize_query_plan = 0;

SELECT '-- Without distributed_push_down_limit the classic path is used: shards sort, the initiator merges';
EXPLAIN distributed = 1 SELECT payload FROM sort_limit_dist ORDER BY key DESC LIMIT 3 SETTINGS distributed_push_down_limit = 0;

SELECT '-- DISTINCT keeps the classic path';
SELECT explain LIKE '%Preliminary DISTINCT%' AS has_preliminary_distinct FROM (EXPLAIN distributed = 1 SELECT DISTINCT payload FROM sort_limit_dist ORDER BY payload LIMIT 3) WHERE has_preliminary_distinct;
SELECT DISTINCT key % 7 AS k FROM sort_limit_dist ORDER BY k LIMIT 3;

SELECT '-- LIMIT BY keeps the classic path';
SELECT count() FROM (EXPLAIN distributed = 1 SELECT payload FROM sort_limit_dist ORDER BY key LIMIT 1 BY payload LIMIT 3) WHERE explain LIKE '%LimitBy%';
SELECT key FROM sort_limit_dist ORDER BY key LIMIT 1 BY key LIMIT 3;

DROP TABLE sort_limit_dist;
DROP TABLE sort_limit_local;
