-- With `join_use_nulls`, a predicate on the join key of the side an outer join makes `Nullable` is inferred
-- on the other table through the equi-join condition, once the join is converted so that this side has
-- no unmatched rows. `force_primary_key` checks that both tables use their primary key.

SET enable_analyzer = 1;
SET join_use_nulls = 1;
SET query_plan_filter_push_down = 1;
SET query_plan_convert_outer_join_to_inner_join = 1;
SET query_plan_optimize_join_order_randomize = 0;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;
DROP TABLE IF EXISTS r32;
DROP TABLE IF EXISTS ls;
DROP TABLE IF EXISTS rs;

CREATE TABLE l (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
CREATE TABLE r (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
CREATE TABLE r32 (k UInt32) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
CREATE TABLE ls (s LowCardinality(String)) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 1;
CREATE TABLE rs (s LowCardinality(String)) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 1;

-- Unmatched rows on both sides: `l` has 0..9, the others have 5..14.
INSERT INTO l SELECT number FROM numbers(10);
INSERT INTO r SELECT number + 5 FROM numbers(10);
INSERT INTO r32 SELECT number + 5 FROM numbers(10);
INSERT INTO ls SELECT toString(number) FROM numbers(10);
INSERT INTO rs SELECT toString(number + 5) FROM numbers(10);

SELECT '-- LEFT';
SELECT l.k, r.k, toTypeName(r.k) FROM l LEFT JOIN r ON l.k = r.k WHERE r.k = 7 SETTINGS force_primary_key = 1;
SELECT l.k, r.k FROM l LEFT JOIN r ON l.k = r.k WHERE r.k BETWEEN 3 AND 6 ORDER BY ALL SETTINGS force_primary_key = 1;

SELECT '-- RIGHT';
SELECT l.k, toTypeName(l.k), r.k FROM l RIGHT JOIN r ON l.k = r.k WHERE l.k = 7 SETTINGS force_primary_key = 1;

SELECT '-- FULL';
SELECT l.k, r.k FROM l FULL JOIN r ON l.k = r.k WHERE r.k = 7 SETTINGS force_primary_key = 1;
SELECT l.k, r.k FROM l FULL JOIN r ON l.k = r.k WHERE l.k = 7 SETTINGS force_primary_key = 1;

SELECT '-- different key types';
SELECT r32.k, l.k, toTypeName(l.k) FROM r32 LEFT JOIN l ON r32.k = l.k WHERE l.k = 7 SETTINGS force_primary_key = 1;

SELECT '-- LowCardinality';
SELECT ls.s, rs.s, toTypeName(rs.s) FROM ls LEFT JOIN rs ON ls.s = rs.s WHERE rs.s = '7' SETTINGS force_primary_key = 1;

SELECT '-- predicate below one input';
SELECT l.k, r.k FROM (SELECT k FROM l WHERE k = 7) AS l LEFT JOIN r ON l.k = r.k SETTINGS force_primary_key = 1;
SELECT l.k, r.k FROM l RIGHT JOIN (SELECT k FROM r WHERE k = 7) AS r ON l.k = r.k SETTINGS force_primary_key = 1;

SELECT '-- not inferred while the join keeps unmatched rows';
SELECT l.k, r.k FROM l LEFT JOIN r ON l.k = r.k WHERE r.k IS NULL ORDER BY ALL;
SELECT l.k, r.k FROM l LEFT JOIN r ON l.k = r.k WHERE r.k = 7 OR r.k IS NULL ORDER BY ALL;
SELECT l.k, r.k FROM l LEFT JOIN r ON l.k = r.k WHERE coalesce(r.k, 7) = 7 ORDER BY ALL;
SELECT l.k, r.k FROM l FULL JOIN r ON l.k = r.k WHERE r.k = 7 OR l.k = 2 ORDER BY ALL;

DROP TABLE l;
DROP TABLE r;
DROP TABLE r32;
DROP TABLE ls;
DROP TABLE rs;
