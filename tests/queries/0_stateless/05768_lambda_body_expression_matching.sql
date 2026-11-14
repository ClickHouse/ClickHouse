-- Two lambdas with the same signature and the same captured columns are one calculation only if
-- their bodies are, whether or not JIT compiled them. Each case pins its optimization on the
-- outermost query.

DROP TABLE IF EXISTS r;
CREATE TABLE r (a Array(UInt64), b UInt64) ENGINE = MergeTree ORDER BY arrayMap(x -> x % b, a);
INSERT INTO r SELECT [number], 10 FROM numbers(30);

-- Case 1: read in order with a different body.
SELECT groupArray(a[1]) FROM (SELECT a FROM r ORDER BY arrayMap(x -> x + b, a) DESC LIMIT 5) SETTINGS optimize_read_in_order = 1;
SELECT groupArray(a[1]) FROM (SELECT a FROM r ORDER BY arrayMap(x -> x + b, a) LIMIT 5) SETTINGS optimize_read_in_order = 1;
SELECT groupArray(a[1]) FROM (SELECT a FROM r ORDER BY arrayMap(x -> x * b, a) DESC LIMIT 5) SETTINGS optimize_read_in_order = 1;

-- Case 2: the same lambda still reads in order.
SELECT count() FROM (EXPLAIN actions = 1 SELECT a FROM r ORDER BY arrayMap(x -> x % b, a) DESC LIMIT 5 SETTINGS optimize_read_in_order = 1, enable_parallel_replicas = 0)
WHERE explain ILIKE '%Read type: InReverseOrder%';

-- Case 3: two arguments are paired by position.
DROP TABLE IF EXISTS s;
CREATE TABLE s (a Array(Int64), c Array(Int64), b Int64) ENGINE = MergeTree ORDER BY arrayMap((x, y) -> (x - y) * b, a, c);
INSERT INTO s SELECT [number], [30 - number], 1 FROM numbers(30);
SELECT groupArray(a[1]) FROM (SELECT a FROM s ORDER BY arrayMap((y, x) -> (x - y) * b, a, c) LIMIT 5) SETTINGS optimize_read_in_order = 1;
SELECT count() FROM (EXPLAIN actions = 1 SELECT a FROM s ORDER BY arrayMap((x, y) -> (x - y) * b, a, c) LIMIT 5 SETTINGS optimize_read_in_order = 1, enable_parallel_replicas = 0)
WHERE explain ILIKE '%Read type: InOrder%';

-- Case 3b: a renamed argument is visible to formatRowNoNewline.
DROP TABLE IF EXISTS p_fmt;
CREATE TABLE p_fmt
(
    a Array(UInt64),
    b UInt64,
    PROJECTION pr (SELECT arrayMap(x -> concat(formatRowNoNewline('JSONEachRow', x), toString(b)), a), count() GROUP BY arrayMap(x -> concat(formatRowNoNewline('JSONEachRow', x), toString(b)), a))
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO p_fmt SELECT [number], 10 FROM numbers(3);
SELECT arrayMap(y -> concat(formatRowNoNewline('JSONEachRow', y), toString(b)), a) AS k, count() FROM p_fmt GROUP BY k ORDER BY k SETTINGS optimize_use_projections = 1;

-- Case 4: a nested lambda.
DROP TABLE IF EXISTS n;
CREATE TABLE n (a Array(UInt64)) ENGINE = MergeTree ORDER BY arrayMap(x -> arrayCount(y -> y > x, a), a);
INSERT INTO n VALUES ([1, 2, 3]), ([1, 3, 2]), ([2, 1, 3]), ([2, 3, 1]), ([3, 1, 2]), ([3, 2, 1]);
SELECT groupArray(a) FROM (SELECT a FROM n ORDER BY arrayMap(x -> arrayCount(y -> y < x, a), a) DESC) SETTINGS optimize_read_in_order = 1;
SELECT count() FROM (EXPLAIN actions = 1 SELECT a FROM n ORDER BY arrayMap(x -> arrayCount(y -> y > x, a), a) DESC SETTINGS optimize_read_in_order = 1, enable_parallel_replicas = 0)
WHERE explain ILIKE '%Read type: InReverseOrder%';

-- Case 5: arrayFilter.
DROP TABLE IF EXISTS f;
CREATE TABLE f (a Array(UInt64), b UInt64) ENGINE = MergeTree ORDER BY arrayFilter(x -> x > b, a);
INSERT INTO f SELECT [number, 100 - number], 50 FROM numbers(30);
SELECT groupArray(a[1]) FROM (SELECT a FROM f ORDER BY arrayFilter(x -> x < b, a) DESC LIMIT 5) SETTINGS optimize_read_in_order = 1;

-- Case 6: aggregation in order.
SELECT count() FROM (SELECT arrayMap(x -> x + b, a) AS k, count() FROM r GROUP BY k) SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1;

-- Case 7: an explicit aggregate projection.
DROP TABLE IF EXISTS p;
CREATE TABLE p
(
    a Array(UInt64),
    b UInt64,
    PROJECTION pr (SELECT arrayMap(x -> x % b, a), count() GROUP BY arrayMap(x -> x % b, a))
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO p SELECT [number], 10 FROM numbers(30);
SELECT arrayMap(x -> x + b, a) AS k, count() FROM p GROUP BY k ORDER BY k LIMIT 3 SETTINGS optimize_use_projections = 1;
SELECT arrayMap(x -> x % b, a) AS k, count() FROM p GROUP BY k ORDER BY k LIMIT 3 SETTINGS optimize_use_projections = 1, force_optimize_projection = 1, enable_parallel_replicas = 0;

-- Case 8: the implicit _minmax_count_projection of the partition key.
DROP TABLE IF EXISTS pp;
CREATE TABLE pp (a Array(UInt64), b UInt64) ENGINE = MergeTree PARTITION BY arrayMap(x -> x % b, a) ORDER BY tuple();
INSERT INTO pp SELECT [number % 3], 10 FROM numbers(30);
INSERT INTO pp SELECT [number % 3 + 10], 10 FROM numbers(30);
SELECT arrayMap(x -> x * b, a) AS k, count() FROM pp GROUP BY k ORDER BY k SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1;

-- Case 9: the same lambda matches whether JIT compiled the query side or not. The key is built
-- with the server's settings, so one of the two queries is compiled on one side only.
SET compile_expressions = 1, min_count_to_compile_expression = 0;
SELECT count() FROM (EXPLAIN actions = 1 SELECT a FROM s ORDER BY arrayMap((x, y) -> (x - y) * b, a, c) LIMIT 5 SETTINGS optimize_read_in_order = 1, enable_parallel_replicas = 0)
WHERE explain ILIKE '%Read type: InOrder%';
SET compile_expressions = 0;
SELECT count() FROM (EXPLAIN actions = 1 SELECT a FROM s ORDER BY arrayMap((x, y) -> (x - y) * b, a, c) LIMIT 5 SETTINGS optimize_read_in_order = 1, enable_parallel_replicas = 0)
WHERE explain ILIKE '%Read type: InOrder%';

-- Case 10: different bodies whose JIT-compiled forms look the same.
DROP TABLE IF EXISTS d;
SET compile_expressions = 1, min_count_to_compile_expression = 0;
CREATE TABLE d (a Array(Int64), c Array(Int64), b Int64) ENGINE = MergeTree ORDER BY arrayMap((x, y) -> x * x + y + b, a, c);
INSERT INTO d SELECT [number], [(number * 7) % 30], 0 FROM numbers(30);
SELECT groupArray(v[1]) FROM (SELECT arrayMap((x, y) -> x * y + x + b, a, c) AS v FROM d ORDER BY arrayMap((x, y) -> x * y + x + b, a, c) LIMIT 8) SETTINGS optimize_read_in_order = 1;
SELECT count() FROM (EXPLAIN actions = 1 SELECT a FROM d ORDER BY arrayMap((x, y) -> x * y + x + b, a, c) LIMIT 8 SETTINGS optimize_read_in_order = 1, enable_parallel_replicas = 0)
WHERE explain ILIKE '%Read type: InOrder%';
