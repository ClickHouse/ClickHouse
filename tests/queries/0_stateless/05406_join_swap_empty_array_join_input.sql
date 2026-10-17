-- Random settings limits: query_plan_optimize_join_order_randomize=(0, 0); query_plan_optimize_join_order_limit=(1, None)

-- A hash join builds from an array join input whose read selects no rows,
-- and keeps the orientation when that input is not proven empty.

SET enable_parallel_replicas = 0, join_algorithm = 'hash', query_plan_join_swap_table = 'auto';

DROP TABLE IF EXISTS aj_l;
DROP TABLE IF EXISTS aj_r;
DROP TABLE IF EXISTS aj_big;

CREATE TABLE aj_l (id UInt64, arr Array(UInt64)) ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = '';
CREATE TABLE aj_r (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = '';
CREATE TABLE aj_big (id UInt64, arr Array(UInt64)) ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = '';

INSERT INTO aj_l SELECT number, [number, number + 1] FROM numbers(100);
INSERT INTO aj_r SELECT number FROM numbers(1000);
INSERT INTO aj_big SELECT 1, range(10000);

-- Each line counts the plans whose join reads `aj_r` first (`s` is the build side) and those reading `s` first.

SELECT 'arrayJoin function', countIf(explain ILIKE '%Join: aj_r%'), countIf(explain ILIKE '%Join: s%')
FROM (EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM (SELECT arrayJoin(arr) AS k FROM aj_l WHERE id > 1000) AS s INNER JOIN aj_r ON aj_r.id = s.k
    SETTINGS query_plan_lower_array_join_function = 0)
WHERE explain ILIKE '%Join:%';

SELECT 'lowered arrayJoin, LEFT JOIN', countIf(explain ILIKE '%Join: aj_r%'), countIf(explain ILIKE '%Join: s%')
FROM (EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM (SELECT arrayJoin(arr) AS k FROM aj_l WHERE id > 1000) AS s LEFT JOIN aj_r ON aj_r.id = s.k
    SETTINGS query_plan_lower_array_join_function = 1)
WHERE explain ILIKE '%Join:%';

SELECT 'LEFT ARRAY JOIN', countIf(explain ILIKE '%Join: aj_r%'), countIf(explain ILIKE '%Join: s%')
FROM (EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM (SELECT k FROM aj_l LEFT ARRAY JOIN arr AS k WHERE id > 1000) AS s INNER JOIN aj_r ON aj_r.id = s.k)
WHERE explain ILIKE '%Join:%';

SELECT 'non-empty input, arrayJoin function', countIf(explain ILIKE '%Join: aj_r%'), countIf(explain ILIKE '%Join: s%')
FROM (EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM (SELECT arrayJoin(arr) AS k FROM aj_big) AS s INNER JOIN aj_r ON aj_r.id = s.k
    SETTINGS query_plan_lower_array_join_function = 0)
WHERE explain ILIKE '%Join:%';

SELECT 'non-empty input, lowered arrayJoin', countIf(explain ILIKE '%Join: aj_r%'), countIf(explain ILIKE '%Join: s%')
FROM (EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM (SELECT arrayJoin(arr) AS k FROM aj_big) AS s INNER JOIN aj_r ON aj_r.id = s.k
    SETTINGS query_plan_lower_array_join_function = 1)
WHERE explain ILIKE '%Join:%';

-- A keyless aggregation over an empty read is estimated at 0 rows but returns one row.
SELECT 'aggregation estimated at 0 rows', countIf(explain ILIKE '%Join: aj_r%'), countIf(explain ILIKE '%Join: s%')
FROM (EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM (SELECT c AS k FROM (SELECT count() AS c FROM aj_l WHERE id > 1000)) AS s INNER JOIN aj_r ON aj_r.id = s.k)
WHERE explain ILIKE '%Join:%';

SELECT 'arrayJoin over that aggregation', countIf(explain ILIKE '%Join: aj_r%'), countIf(explain ILIKE '%Join: s%')
FROM (EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM (SELECT arrayJoin(range(10000)) AS k FROM (SELECT count() AS c FROM aj_l WHERE id > 1000)) AS s INNER JOIN aj_r ON aj_r.id = s.k)
WHERE explain ILIKE '%Join:%';

SELECT count() FROM (SELECT arrayJoin(arr) AS k FROM aj_l WHERE id > 1000) AS s LEFT JOIN aj_r ON aj_r.id = s.k;
SELECT count() FROM (SELECT arrayJoin(arr) AS k FROM aj_big) AS s INNER JOIN aj_r ON aj_r.id = s.k;
SELECT count() FROM (SELECT arrayJoin(range(10000)) AS k FROM (SELECT count() AS c FROM aj_l WHERE id > 1000)) AS s INNER JOIN aj_r ON aj_r.id = s.k;

DROP TABLE aj_l;
DROP TABLE aj_r;
DROP TABLE aj_big;
