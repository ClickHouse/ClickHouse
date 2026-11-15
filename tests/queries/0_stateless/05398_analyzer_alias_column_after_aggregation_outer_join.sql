-- An outer JOIN fills the columns of a table in the rows without a match with default values, including ALIAS columns,
-- so such an ALIAS column is not a function of the (default) GROUP BY keys and cannot be computed after aggregation.
-- https://github.com/ClickHouse/ClickHouse/issues/121400

SET enable_analyzer = 1;
SET join_use_nulls = 0;

DROP TABLE IF EXISTS t_alias_join_l;
DROP TABLE IF EXISTS t_alias_join_r;

CREATE TABLE t_alias_join_l (k UInt64, b UInt64 ALIAS k + 10) ENGINE = Memory;
CREATE TABLE t_alias_join_r (k UInt64, a UInt64 ALIAS k + 1) ENGINE = Memory;

INSERT INTO t_alias_join_l VALUES (0), (1);
INSERT INTO t_alias_join_r VALUES (0), (2);

SELECT '-- the value of the ALIAS column in the row without a match';
SELECT l.k, r.k, r.a FROM t_alias_join_l AS l LEFT JOIN t_alias_join_r AS r ON l.k = r.k ORDER BY l.k;

SELECT '-- the side that an outer JOIN fills with default values';
SELECT l.k, r.k, r.a FROM t_alias_join_l AS l LEFT JOIN t_alias_join_r AS r ON l.k = r.k GROUP BY l.k, r.k; -- { serverError NOT_AN_AGGREGATE }
SELECT l.k FROM t_alias_join_l AS l LEFT JOIN t_alias_join_r AS r ON l.k = r.k GROUP BY l.k, r.k HAVING r.a = 1; -- { serverError NOT_AN_AGGREGATE }
SELECT r.k, l.b FROM t_alias_join_l AS l RIGHT JOIN t_alias_join_r AS r ON l.k = r.k GROUP BY r.k, l.k; -- { serverError NOT_AN_AGGREGATE }
SELECT l.b FROM t_alias_join_l AS l FULL JOIN t_alias_join_r AS r ON l.k = r.k GROUP BY l.k; -- { serverError NOT_AN_AGGREGATE }
SELECT r.a FROM t_alias_join_l AS l FULL JOIN t_alias_join_r AS r ON l.k = r.k GROUP BY r.k; -- { serverError NOT_AN_AGGREGATE }
SELECT l.k FROM t_alias_join_l AS l LEFT JOIN t_alias_join_r AS r ON l.k = r.k GROUP BY l.k, r.k HAVING EXISTS (SELECT 1 WHERE r.a = 1); -- { serverError NOT_AN_AGGREGATE }
SELECT l.k, r.a FROM t_alias_join_l AS l LEFT JOIN (SELECT 1) AS s ON 1 LEFT JOIN t_alias_join_r AS r ON l.k = r.k GROUP BY l.k, r.k; -- { serverError NOT_AN_AGGREGATE }

SELECT '-- the preserved side of an outer JOIN and INNER JOIN';
SELECT l.k, l.b, count() FROM t_alias_join_l AS l LEFT JOIN t_alias_join_r AS r ON l.k = r.k GROUP BY l.k ORDER BY l.k;
SELECT r.k, r.a, count() FROM t_alias_join_l AS l RIGHT JOIN t_alias_join_r AS r ON l.k = r.k GROUP BY r.k ORDER BY r.k;
SELECT l.k, r.a, l.b FROM t_alias_join_l AS l INNER JOIN t_alias_join_r AS r ON l.k = r.k GROUP BY l.k, r.k ORDER BY l.k;
SELECT l.k, l.b, r.a FROM t_alias_join_l AS l CROSS JOIN t_alias_join_r AS r GROUP BY l.k, r.k ORDER BY l.k, r.a;

SELECT '-- an ALIAS column that is a GROUP BY key itself';
SELECT l.k, r.a FROM t_alias_join_l AS l LEFT JOIN t_alias_join_r AS r ON l.k = r.k GROUP BY l.k, r.a ORDER BY l.k;

DROP TABLE t_alias_join_l;
DROP TABLE t_alias_join_r;
