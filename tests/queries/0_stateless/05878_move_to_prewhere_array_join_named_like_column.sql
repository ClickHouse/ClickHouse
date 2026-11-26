-- a condition on arrayJoin stays out of PREWHERE when a column has the name of that expression

DROP TABLE IF EXISTS t_probe;
DROP TABLE IF EXISTS t_build;

CREATE TABLE t_probe (ids Array(String), `arrayJoin(ids)` String, k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_build (v String, k UInt64) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_probe VALUES (['hit', 'x'], 'miss', 1), (['y'], 'hit', 2);
INSERT INTO t_build VALUES ('hit', 1);

SET enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, optimize_move_to_prewhere = 1;

SELECT count() FROM t_probe AS p INNER JOIN t_build AS b ON arrayJoin(p.ids) = b.v AND p.k = b.k;
SELECT count(), any(p.k) FROM t_probe AS p INNER JOIN t_build AS b ON arrayJoin(p.ids) = b.v;
SELECT count() FROM t_probe AS p LEFT SEMI JOIN t_build AS b ON arrayJoin(p.ids) = b.v AND p.k = b.k;

DROP TABLE t_probe;
DROP TABLE t_build;
