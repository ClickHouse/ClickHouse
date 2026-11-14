-- Tags: no-parallel
-- no-parallel: creates and drops a global SQL-managed cluster

-- SQL cluster names are stored as literals. `Clusters::getCluster` expands macros in the
-- lookup argument, so `CREATE CLUSTER \`c_{replica}\`` would not be found as `c_r1`.
-- The tests/config macros define `replica = r1`.

DROP CLUSTER IF EXISTS c_r1;

CREATE CLUSTER `c_{replica}` (SHARD (host = '127.0.0.1', port = 9000)); -- { serverError BAD_ARGUMENTS }

CREATE CLUSTER `{test}` (SHARD (host = '127.0.0.1', port = 9000)); -- { serverError BAD_ARGUMENTS }

CREATE CLUSTER `c_{no_such_macro}` (SHARD (host = '127.0.0.1', port = 9000)); -- { serverError BAD_ARGUMENTS }

SELECT count() FROM system.clusters WHERE cluster IN ('c_{replica}', 'c_r1', '{test}');

CREATE CLUSTER c_r1 (SHARD (host = '127.0.0.1', port = 9000));

SELECT cluster, source
FROM system.clusters
WHERE cluster = 'c_r1'
ORDER BY replica_num
LIMIT 1
FORMAT TSV;

SELECT count() FROM cluster('c_{replica}', system.one);

ALTER CLUSTER `c_{replica}` (SHARD (host = '127.0.0.1', port = 9000)); -- { serverError BAD_ARGUMENTS }

DROP CLUSTER `c_{replica}`; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM system.clusters WHERE cluster = 'c_r1';

DROP CLUSTER c_r1;
