-- `ALL JOIN ... WHERE key IS NULL` is converted to `ANTI JOIN`, because the `ON` condition proves the key not NULL in a matched row.

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET query_plan_convert_any_join_to_semi_or_anti_join = 1;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET join_algorithm = 'hash';

CREATE TABLE l (k UInt64) ENGINE = Memory;
CREATE TABLE r (k UInt64) ENGINE = Memory;
CREATE TABLE ln (k Nullable(UInt64), x Int64) ENGINE = Memory;
CREATE TABLE rn (k Nullable(UInt64)) ENGINE = Memory;
CREATE TABLE rd (k UInt64, d Dynamic) ENGINE = Memory;

-- Keys 0..4 of `l` match two rows of `r` each, keys 5..9 match nothing.
INSERT INTO l SELECT number FROM numbers(10);
INSERT INTO r SELECT number % 5 FROM numbers(10);
INSERT INTO ln SELECT if(number = 0, NULL, number), number FROM numbers(10);
INSERT INTO rn SELECT if(number < 5, number, NULL) FROM numbers(10);
INSERT INTO rd VALUES (1, NULL), (2, 42);

SET join_use_nulls = 1;

SELECT '-- LEFT ALL JOIN';
SELECT trimBoth(explain) FROM (EXPLAIN actions = 1 SELECT l.k FROM l LEFT JOIN r ON l.k = r.k WHERE r.k IS NULL) WHERE explain ILIKE '%Strictness:%';
SELECT groupArray(k) FROM (SELECT l.k AS k FROM l LEFT JOIN r ON l.k = r.k WHERE r.k IS NULL ORDER BY k);

SELECT '-- RIGHT ALL JOIN';
SELECT trimBoth(explain) FROM (EXPLAIN actions = 1 SELECT l.k FROM r RIGHT JOIN l ON r.k = l.k WHERE r.k IS NULL) WHERE explain ILIKE '%Strictness:%';
SELECT groupArray(k) FROM (SELECT l.k AS k FROM r RIGHT JOIN l ON r.k = l.k WHERE r.k IS NULL ORDER BY k);

SELECT '-- IS NULL AND another condition';
SELECT trimBoth(explain) FROM (EXPLAIN actions = 1 SELECT l.k FROM l LEFT JOIN r ON l.k = r.k WHERE r.k IS NULL AND l.k > 6) WHERE explain ILIKE '%Strictness:%';
SELECT groupArray(k) FROM (SELECT l.k AS k FROM l LEFT JOIN r ON l.k = r.k WHERE r.k IS NULL AND l.k > 6 ORDER BY k);

SELECT '-- IS NOT NULL is not converted to SEMI: ALL JOIN keeps every match';
SELECT trimBoth(explain) FROM (EXPLAIN actions = 1 SELECT l.k FROM l LEFT JOIN r ON l.k = r.k WHERE r.k IS NOT NULL) WHERE explain ILIKE '%Strictness:%';
SELECT groupArray(k) FROM (SELECT l.k AS k FROM l LEFT JOIN r ON l.k = r.k WHERE r.k IS NOT NULL ORDER BY k);

SET join_use_nulls = 0;

SELECT '-- Nullable keys';
SELECT trimBoth(explain) FROM (EXPLAIN actions = 1 SELECT ln.x FROM ln LEFT JOIN rn ON ln.k = rn.k WHERE rn.k IS NULL) WHERE explain ILIKE '%Strictness:%';
SELECT groupArray(x) FROM (SELECT ln.x AS x FROM ln LEFT JOIN rn ON ln.k = rn.k WHERE rn.k IS NULL ORDER BY x);

SELECT '-- <=> matches NULL keys, so it is not converted';
SELECT trimBoth(explain) FROM (EXPLAIN actions = 1 SELECT ln.x FROM ln LEFT JOIN rn ON ln.k <=> rn.k WHERE rn.k IS NULL) WHERE explain ILIKE '%Strictness:%';
SELECT groupArray(x) FROM (SELECT ln.x AS x FROM ln LEFT JOIN rn ON ln.k <=> rn.k WHERE rn.k IS NULL ORDER BY x);

SELECT '-- Dynamic holds NULL without being Nullable, so it is not converted';
SELECT trimBoth(explain) FROM (EXPLAIN actions = 1 SELECT l.k FROM l LEFT JOIN rd ON l.k = rd.k WHERE rd.d IS NULL) WHERE explain ILIKE '%Strictness:%';
SELECT groupArray(k) FROM (SELECT l.k AS k FROM l LEFT JOIN rd ON l.k = rd.k WHERE rd.d IS NULL ORDER BY k);
