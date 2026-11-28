-- a key that GROUP BY repeats is repeated in the sorting key of the projection, as the existing parts have it

DROP TABLE IF EXISTS t_projection_repeated_key;

CREATE TABLE t_projection_repeated_key (a Int32, PROJECTION p (SELECT a, toString(a), count() GROUP BY a, toString(a), a))
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_projection_repeated_key (a Int32, PROJECTION p (SELECT a, toString(a), count() GROUP BY a, toString(a), a))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS allow_suspicious_indices = 1;

SELECT sorting_key FROM system.projections WHERE database = currentDatabase() AND table = 't_projection_repeated_key';

INSERT INTO t_projection_repeated_key SELECT number % 5 FROM numbers(100);

SELECT a, toString(a) AS s, count() FROM t_projection_repeated_key WHERE a = 3 GROUP BY a, s
SETTINGS force_optimize_projection = 1;

DROP TABLE t_projection_repeated_key;
