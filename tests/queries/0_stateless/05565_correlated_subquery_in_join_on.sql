-- Correlated subqueries in `JOIN ON` that reference the columns of only one side of the JOIN.
-- https://github.com/ClickHouse/ClickHouse/issues/129644

SET join_use_nulls = 0;

DROP TABLE IF EXISTS t_l;
DROP TABLE IF EXISTS t_r;
DROP TABLE IF EXISTS t_p;
DROP TABLE IF EXISTS t_rj;

CREATE TABLE t_l (id Int32, owner Int32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_r (id Int32, name String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_p (id Int32, owner Int32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_l VALUES (1, 10), (2, 20), (3, 30), (4, 50);
INSERT INTO t_r VALUES (10, 'a'), (20, 'b'), (50, 'e'), (60, 'f');
INSERT INTO t_p VALUES (1, 10), (2, 20), (3, 50), (10, 1), (20, 2);

-- The query from the issue.
SELECT r.id, u.name
FROM values('id Int32, owner Int32', (1, 10), (2, 20)) r
JOIN values('id Int32, name String', (10, 'a'), (20, 'b')) u
    ON u.id = (SELECT p.owner FROM values('id Int32, owner Int32', (1, 10), (2, 20)) p WHERE p.id = r.id)
ORDER BY 1;

SELECT 'subquery over the left table';
SELECT 'inner';
SELECT l.id, r.name FROM t_l l JOIN t_r r ON r.id = (SELECT p.owner FROM t_p p WHERE p.id = l.id) ORDER BY ALL;
SELECT 'left';
SELECT l.id, r.name FROM t_l l LEFT JOIN t_r r ON r.id = (SELECT p.owner FROM t_p p WHERE p.id = l.id) ORDER BY ALL;
SELECT 'right';
SELECT l.id, r.name FROM t_l l RIGHT JOIN t_r r ON r.id = (SELECT p.owner FROM t_p p WHERE p.id = l.id) ORDER BY ALL;
SELECT 'full';
SELECT l.id, r.name FROM t_l l FULL JOIN t_r r ON r.id = (SELECT p.owner FROM t_p p WHERE p.id = l.id) ORDER BY ALL;
SELECT 'any left';
SELECT l.id, r.name FROM t_l l ANY LEFT JOIN t_r r ON r.id = (SELECT p.owner FROM t_p p WHERE p.id = l.id) ORDER BY ALL;
SELECT 'left semi';
SELECT l.id FROM t_l l LEFT SEMI JOIN t_r r ON r.id = (SELECT p.owner FROM t_p p WHERE p.id = l.id) ORDER BY ALL;
SELECT 'left anti';
SELECT l.id FROM t_l l LEFT ANTI JOIN t_r r ON r.id = (SELECT p.owner FROM t_p p WHERE p.id = l.id) ORDER BY ALL;

SELECT 'subquery over the right table';
SELECT 'inner';
SELECT l.id, r.name FROM t_l l JOIN t_r r ON l.id = (SELECT p.owner FROM t_p p WHERE p.id = r.id) ORDER BY ALL;
SELECT 'left';
SELECT l.id, r.name FROM t_l l LEFT JOIN t_r r ON l.id = (SELECT p.owner FROM t_p p WHERE p.id = r.id) ORDER BY ALL;

SELECT 'join_use_nulls = 1';
SELECT l.id, r.name FROM t_l l LEFT JOIN t_r r ON r.id = (SELECT p.owner FROM t_p p WHERE p.id = l.id) ORDER BY ALL SETTINGS join_use_nulls = 1;
SELECT '-';
SELECT l.id, r.name FROM t_l l LEFT JOIN t_r r ON l.id = (SELECT p.owner FROM t_p p WHERE p.id = r.id) ORDER BY ALL SETTINGS join_use_nulls = 1;
SELECT '-';
SELECT l.id, r.name FROM t_l l FULL JOIN t_r r ON r.id = (SELECT p.owner FROM t_p p WHERE p.id = l.id) ORDER BY ALL SETTINGS join_use_nulls = 1;

SELECT 'exists';
SELECT l.id, r.name FROM t_l l JOIN t_r r ON r.id = l.owner AND EXISTS (SELECT 1 FROM t_p p WHERE p.id = l.id) ORDER BY ALL;
SELECT '-';
SELECT l.id, r.name FROM t_l l JOIN t_r r ON r.id = l.owner AND NOT EXISTS (SELECT 1 FROM t_p p WHERE p.id = l.id) ORDER BY ALL;
SELECT '-';
SELECT l.id, r.name FROM t_l l JOIN t_r r ON r.id = l.owner AND EXISTS (SELECT 1 FROM t_p p WHERE p.id = r.id) ORDER BY ALL;
SELECT '-';
SELECT l.id, r.name FROM t_l l JOIN t_r r ON r.id = l.owner AND NOT EXISTS (SELECT 1 FROM t_p p WHERE p.id = r.id) ORDER BY ALL;

SELECT 'one subquery per side';
SELECT l.id, r.name FROM t_l l JOIN t_r r
    ON r.id = (SELECT p.owner FROM t_p p WHERE p.id = l.id) AND l.id = (SELECT p.owner FROM t_p p WHERE p.id = r.id)
ORDER BY ALL;

SELECT 'left side is a join';
SELECT l.id, r.name, p2.owner FROM t_l l JOIN t_r r ON r.id = l.owner
    JOIN t_p p2 ON p2.id = (SELECT max(p.id) FROM t_p p WHERE p.owner = l.owner)
ORDER BY ALL;

SELECT 'or';
SELECT l.id, r.name FROM t_l l JOIN t_r r ON r.id = l.owner OR r.id = (SELECT p.owner FROM t_p p WHERE p.id = l.id) ORDER BY ALL;

SELECT 'alias shared with SELECT';
SELECT l.id, s FROM t_l l JOIN t_r r ON r.id = ((SELECT p.owner FROM t_p p WHERE p.id = l.id) AS s) ORDER BY l.id;

SELECT 'Join engine on the right';
CREATE TABLE t_rj (id Int32, name String) ENGINE = Join(ALL, LEFT, id);
INSERT INTO t_rj SELECT * FROM t_r;
SELECT l.id, r.name FROM t_l l LEFT JOIN t_rj r ON r.id = (SELECT p.owner FROM t_p p WHERE p.id = l.id) ORDER BY ALL;
SELECT '-';
SELECT l.id, r.name FROM t_l l LEFT JOIN t_rj r ON l.id = (SELECT p.owner FROM t_p p WHERE p.id = r.id) ORDER BY ALL;

SELECT 'join algorithms';
SELECT l.id, r.name FROM t_l l JOIN t_r r ON r.id = (SELECT p.owner FROM t_p p WHERE p.id = l.id) ORDER BY ALL SETTINGS join_algorithm = 'hash';
SELECT l.id, r.name FROM t_l l JOIN t_r r ON r.id = (SELECT p.owner FROM t_p p WHERE p.id = l.id) ORDER BY ALL SETTINGS join_algorithm = 'parallel_hash';
SELECT l.id, r.name FROM t_l l JOIN t_r r ON r.id = (SELECT p.owner FROM t_p p WHERE p.id = l.id) ORDER BY ALL SETTINGS join_algorithm = 'grace_hash';
SELECT l.id, r.name FROM t_l l JOIN t_r r ON r.id = (SELECT p.owner FROM t_p p WHERE p.id = l.id) ORDER BY ALL SETTINGS join_algorithm = 'full_sorting_merge';
SELECT l.id, r.name FROM t_l l JOIN t_r r ON r.id = (SELECT p.owner FROM t_p p WHERE p.id = l.id) ORDER BY ALL SETTINGS join_algorithm = 'partial_merge';
SELECT l.id, r.name FROM t_l l JOIN t_r r ON r.id = (SELECT p.owner FROM t_p p WHERE p.id = l.id) ORDER BY ALL SETTINGS join_algorithm = 'auto';
SELECT '-';
SELECT l.id, r.name FROM t_l l LEFT JOIN t_r r ON l.id = (SELECT p.owner FROM t_p p WHERE p.id = r.id) ORDER BY ALL SETTINGS join_algorithm = 'hash';
SELECT l.id, r.name FROM t_l l LEFT JOIN t_r r ON l.id = (SELECT p.owner FROM t_p p WHERE p.id = r.id) ORDER BY ALL SETTINGS join_algorithm = 'parallel_hash';
SELECT l.id, r.name FROM t_l l LEFT JOIN t_r r ON l.id = (SELECT p.owner FROM t_p p WHERE p.id = r.id) ORDER BY ALL SETTINGS join_algorithm = 'grace_hash';
SELECT l.id, r.name FROM t_l l LEFT JOIN t_r r ON l.id = (SELECT p.owner FROM t_p p WHERE p.id = r.id) ORDER BY ALL SETTINGS join_algorithm = 'full_sorting_merge';
SELECT l.id, r.name FROM t_l l LEFT JOIN t_r r ON l.id = (SELECT p.owner FROM t_p p WHERE p.id = r.id) ORDER BY ALL SETTINGS join_algorithm = 'partial_merge';
SELECT l.id, r.name FROM t_l l LEFT JOIN t_r r ON l.id = (SELECT p.owner FROM t_p p WHERE p.id = r.id) ORDER BY ALL SETTINGS join_algorithm = 'auto';

SELECT 'decorrelation settings';
SELECT l.id, r.name FROM t_l l LEFT JOIN t_r r ON r.id = (SELECT p.owner FROM t_p p WHERE p.id = l.id) ORDER BY ALL
SETTINGS correlated_subqueries_use_in_memory_buffer = 0, correlated_subqueries_default_join_kind = 'left';
SELECT l.id, r.name FROM t_l l LEFT JOIN t_r r ON r.id = (SELECT p.owner FROM t_p p WHERE p.id = l.id) ORDER BY ALL
SETTINGS correlated_subqueries_use_in_memory_buffer = 0, correlated_subqueries_default_join_kind = 'right';
SELECT l.id, r.name FROM t_l l LEFT JOIN t_r r ON r.id = (SELECT p.owner FROM t_p p WHERE p.id = l.id) ORDER BY ALL
SETTINGS correlated_subqueries_use_in_memory_buffer = 1, correlated_subqueries_default_join_kind = 'left';
SELECT l.id, r.name FROM t_l l LEFT JOIN t_r r ON r.id = (SELECT p.owner FROM t_p p WHERE p.id = l.id) ORDER BY ALL
SETTINGS correlated_subqueries_use_in_memory_buffer = 1, correlated_subqueries_default_join_kind = 'right';
SELECT '-';
SELECT l.id, r.name FROM t_l l LEFT JOIN t_r r ON l.id = (SELECT p.owner FROM t_p p WHERE p.id = r.id) ORDER BY ALL
SETTINGS correlated_subqueries_use_in_memory_buffer = 0, correlated_subqueries_default_join_kind = 'left';
SELECT l.id, r.name FROM t_l l LEFT JOIN t_r r ON l.id = (SELECT p.owner FROM t_p p WHERE p.id = r.id) ORDER BY ALL
SETTINGS correlated_subqueries_use_in_memory_buffer = 0, correlated_subqueries_default_join_kind = 'right';
SELECT l.id, r.name FROM t_l l LEFT JOIN t_r r ON l.id = (SELECT p.owner FROM t_p p WHERE p.id = r.id) ORDER BY ALL
SETTINGS correlated_subqueries_use_in_memory_buffer = 1, correlated_subqueries_default_join_kind = 'left';
SELECT l.id, r.name FROM t_l l LEFT JOIN t_r r ON l.id = (SELECT p.owner FROM t_p p WHERE p.id = r.id) ORDER BY ALL
SETTINGS correlated_subqueries_use_in_memory_buffer = 1, correlated_subqueries_default_join_kind = 'right';

SELECT 'not supported';
-- References columns of both sides.
SELECT l.id, r.name FROM t_l l JOIN t_r r ON r.id = l.owner AND EXISTS (SELECT 1 FROM t_p p WHERE p.id = l.id AND p.owner = r.id); -- { serverError NOT_IMPLEMENTED }
-- References a column of an outer query.
SELECT (SELECT count() FROM t_l l JOIN t_r r ON r.id = (SELECT max(p.owner) FROM t_p p WHERE p.id = o.id)) FROM t_l o; -- { serverError NOT_IMPLEMENTED }

-- Compare with the same JOIN where the subquery is decorrelated by hand, on generated data with
-- duplicate and missing keys.
DROP TABLE IF EXISTS g_l;
DROP TABLE IF EXISTS g_r;
DROP TABLE IF EXISTS g_p;
CREATE TABLE g_l (id UInt64, k UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE g_r (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE g_p (id UInt64, k UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO g_l SELECT number, number % 37 FROM numbers(1000);
INSERT INTO g_r SELECT number % 101, number FROM numbers(500);
INSERT INTO g_p SELECT number * 3 % 997, number % 53 FROM numbers(700);

SELECT 'generated data, subquery over the left table';
SELECT kind, (SELECT count(), sum(cityHash64(l.id, r.v)) FROM g_l l INNER JOIN g_r r ON r.id = (SELECT max(p.k) FROM g_p p WHERE p.id = l.id))
    = (SELECT count(), sum(cityHash64(l.id, r.v)) FROM (SELECT l.*, pp.m FROM g_l l LEFT JOIN (SELECT id, toNullable(max(k)) AS m FROM g_p GROUP BY id) pp ON pp.id = l.id) l INNER JOIN g_r r ON r.id = l.m)
FROM (SELECT 'inner' AS kind);
SELECT kind, (SELECT count(), sum(cityHash64(l.id, r.v)) FROM g_l l LEFT JOIN g_r r ON r.id = (SELECT max(p.k) FROM g_p p WHERE p.id = l.id))
    = (SELECT count(), sum(cityHash64(l.id, r.v)) FROM (SELECT l.*, pp.m FROM g_l l LEFT JOIN (SELECT id, toNullable(max(k)) AS m FROM g_p GROUP BY id) pp ON pp.id = l.id) l LEFT JOIN g_r r ON r.id = l.m)
FROM (SELECT 'left' AS kind);
SELECT kind, (SELECT count(), sum(cityHash64(l.id, r.v)) FROM g_l l RIGHT JOIN g_r r ON r.id = (SELECT max(p.k) FROM g_p p WHERE p.id = l.id))
    = (SELECT count(), sum(cityHash64(l.id, r.v)) FROM (SELECT l.*, pp.m FROM g_l l LEFT JOIN (SELECT id, toNullable(max(k)) AS m FROM g_p GROUP BY id) pp ON pp.id = l.id) l RIGHT JOIN g_r r ON r.id = l.m)
FROM (SELECT 'right' AS kind);
SELECT kind, (SELECT count(), sum(cityHash64(l.id, r.v)) FROM g_l l FULL JOIN g_r r ON r.id = (SELECT max(p.k) FROM g_p p WHERE p.id = l.id))
    = (SELECT count(), sum(cityHash64(l.id, r.v)) FROM (SELECT l.*, pp.m FROM g_l l LEFT JOIN (SELECT id, toNullable(max(k)) AS m FROM g_p GROUP BY id) pp ON pp.id = l.id) l FULL JOIN g_r r ON r.id = l.m)
FROM (SELECT 'full' AS kind);

SELECT 'generated data, subquery over the right table';
SELECT kind, (SELECT count(), sum(cityHash64(l.id, r.v)) FROM g_l l INNER JOIN g_r r ON l.k = (SELECT max(p.k) FROM g_p p WHERE p.id = r.id))
    = (SELECT count(), sum(cityHash64(l.id, r.v)) FROM g_l l INNER JOIN (SELECT r.*, pp.m FROM g_r r LEFT JOIN (SELECT id, toNullable(max(k)) AS m FROM g_p GROUP BY id) pp ON pp.id = r.id) r ON l.k = r.m)
FROM (SELECT 'inner' AS kind);
SELECT kind, (SELECT count(), sum(cityHash64(l.id, r.v)) FROM g_l l LEFT JOIN g_r r ON l.k = (SELECT max(p.k) FROM g_p p WHERE p.id = r.id))
    = (SELECT count(), sum(cityHash64(l.id, r.v)) FROM g_l l LEFT JOIN (SELECT r.*, pp.m FROM g_r r LEFT JOIN (SELECT id, toNullable(max(k)) AS m FROM g_p GROUP BY id) pp ON pp.id = r.id) r ON l.k = r.m)
FROM (SELECT 'left' AS kind);
SELECT kind, (SELECT count(), sum(cityHash64(l.id, r.v)) FROM g_l l RIGHT JOIN g_r r ON l.k = (SELECT max(p.k) FROM g_p p WHERE p.id = r.id))
    = (SELECT count(), sum(cityHash64(l.id, r.v)) FROM g_l l RIGHT JOIN (SELECT r.*, pp.m FROM g_r r LEFT JOIN (SELECT id, toNullable(max(k)) AS m FROM g_p GROUP BY id) pp ON pp.id = r.id) r ON l.k = r.m)
FROM (SELECT 'right' AS kind);
SELECT kind, (SELECT count(), sum(cityHash64(l.id, r.v)) FROM g_l l FULL JOIN g_r r ON l.k = (SELECT max(p.k) FROM g_p p WHERE p.id = r.id))
    = (SELECT count(), sum(cityHash64(l.id, r.v)) FROM g_l l FULL JOIN (SELECT r.*, pp.m FROM g_r r LEFT JOIN (SELECT id, toNullable(max(k)) AS m FROM g_p GROUP BY id) pp ON pp.id = r.id) r ON l.k = r.m)
FROM (SELECT 'full' AS kind);

SELECT 'generated data, exists';
SELECT (SELECT count(), sum(cityHash64(l.id, r.v)) FROM g_l l LEFT JOIN g_r r ON r.id = l.k AND EXISTS (SELECT 1 FROM g_p p WHERE p.id = l.id))
    = (SELECT count(), sum(cityHash64(l.id, r.v)) FROM g_l l LEFT JOIN g_r r ON r.id = l.k AND l.id IN (SELECT id FROM g_p));

DROP TABLE g_l;
DROP TABLE g_r;
DROP TABLE g_p;
DROP TABLE t_rj;
DROP TABLE t_l;
DROP TABLE t_r;
DROP TABLE t_p;
