-- Random settings limits: query_plan_optimize_join_order_randomize=(0, 0); query_plan_optimize_join_order_limit=(1, None)

-- A hash join compares the sides by their row estimates, and an array join input is estimated
-- as 100 rows per row of its input.

SET enable_parallel_replicas = 0, join_algorithm = 'hash', query_plan_join_swap_table = 'auto';

DROP TABLE IF EXISTS aj_l;
DROP TABLE IF EXISTS aj_s;
DROP TABLE IF EXISTS aj_r;

CREATE TABLE aj_l (id UInt64, arr Array(UInt64)) ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = '';
CREATE TABLE aj_s (id UInt64, arr Array(UInt64)) ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = '';
CREATE TABLE aj_r (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = '';

INSERT INTO aj_l SELECT number, range(number, number + 20) FROM numbers(100);
INSERT INTO aj_s SELECT number, [number, number + 1] FROM numbers(5);
INSERT INTO aj_r SELECT number FROM numbers(1000);

-- Each line counts the plans whose join reads `aj_r` first (`s` is the build side) and those reading `s` first.

SELECT 'arrayJoin, empty input', countIf(explain ILIKE '%Join: aj_r%'), countIf(explain ILIKE '%Join: s%')
FROM (EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM (SELECT arrayJoin(arr) AS k FROM aj_l WHERE id > 1000) AS s INNER JOIN aj_r ON aj_r.id = s.k
    SETTINGS query_plan_lower_array_join_function = 0)
WHERE explain ILIKE '%Join:%';

SELECT 'lowered arrayJoin, empty input, LEFT JOIN', countIf(explain ILIKE '%Join: aj_r%'), countIf(explain ILIKE '%Join: s%')
FROM (EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM (SELECT arrayJoin(arr) AS k FROM aj_l WHERE id > 1000) AS s LEFT JOIN aj_r ON aj_r.id = s.k
    SETTINGS query_plan_lower_array_join_function = 1)
WHERE explain ILIKE '%Join:%';

SELECT 'ARRAY JOIN, empty input', countIf(explain ILIKE '%Join: aj_r%'), countIf(explain ILIKE '%Join: s%')
FROM (EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM (SELECT k FROM aj_l ARRAY JOIN arr AS k WHERE id > 1000) AS s INNER JOIN aj_r ON aj_r.id = s.k)
WHERE explain ILIKE '%Join:%';

-- 5 input rows are estimated as 500 rows, fewer than the 1000 rows of `aj_r`.
SELECT 'lowered arrayJoin, 5 rows', countIf(explain ILIKE '%Join: aj_r%'), countIf(explain ILIKE '%Join: s%')
FROM (EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM (SELECT arrayJoin(arr) AS k FROM aj_s) AS s INNER JOIN aj_r ON aj_r.id = s.k
    SETTINGS query_plan_lower_array_join_function = 1)
WHERE explain ILIKE '%Join:%';

-- 100 input rows are estimated as 10000 rows (2000 in fact), more than the 1000 rows of `aj_r`.
SELECT 'arrayJoin, 100 rows', countIf(explain ILIKE '%Join: aj_r%'), countIf(explain ILIKE '%Join: s%')
FROM (EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM (SELECT arrayJoin(arr) AS k FROM aj_l) AS s INNER JOIN aj_r ON aj_r.id = s.k
    SETTINGS query_plan_lower_array_join_function = 0)
WHERE explain ILIKE '%Join:%';

SELECT 'ARRAY JOIN, 100 rows', countIf(explain ILIKE '%Join: aj_r%'), countIf(explain ILIKE '%Join: s%')
FROM (EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM (SELECT k FROM aj_l ARRAY JOIN arr AS k) AS s INNER JOIN aj_r ON aj_r.id = s.k)
WHERE explain ILIKE '%Join:%';

SELECT count() FROM (SELECT arrayJoin(arr) AS k FROM aj_l WHERE id > 1000) AS s LEFT JOIN aj_r ON aj_r.id = s.k;
SELECT count() FROM (SELECT arrayJoin(arr) AS k FROM aj_s) AS s INNER JOIN aj_r ON aj_r.id = s.k;
SELECT count() FROM (SELECT k FROM aj_l ARRAY JOIN arr AS k) AS s INNER JOIN aj_r ON aj_r.id = s.k;

DROP TABLE aj_l;
DROP TABLE aj_s;
DROP TABLE aj_r;
