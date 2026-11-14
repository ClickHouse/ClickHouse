-- The subquery of a `CHECK` constraint reading a `Distributed` table must get its filters collected,
-- so that `optimize_skip_unused_shards` can prune the shards and `force_optimize_skip_unused_shards` does not reject the insert.

DROP TABLE IF EXISTS t_constraint_check;
DROP TABLE IF EXISTS t_constraint_dist;
DROP TABLE IF EXISTS t_constraint_local;

CREATE TABLE t_constraint_local (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_constraint_local VALUES (1), (2);
CREATE TABLE t_constraint_dist (x UInt64) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t_constraint_local, x);

SET optimize_skip_unused_shards = 1;
SET force_optimize_skip_unused_shards = 1;

CREATE TABLE t_constraint_check (x UInt64, CONSTRAINT c CHECK x IN (SELECT x FROM t_constraint_dist WHERE x = 1)) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_constraint_check VALUES (1);
INSERT INTO t_constraint_check VALUES (2); -- { serverError VIOLATED_CONSTRAINT }
SELECT * FROM t_constraint_check;

DROP TABLE t_constraint_check;
DROP TABLE t_constraint_dist;
DROP TABLE t_constraint_local;
