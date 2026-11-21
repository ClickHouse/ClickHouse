-- Random settings limits: query_plan_optimize_join_order_randomize=(0, 0); query_plan_optimize_join_order_limit=(1, None)

-- A hash join compares the sides by their row estimates. A condition on the rows of a `Memory` table
-- leaves its row count unknown, so its total row count must not make the other side the build side.

SET enable_parallel_replicas = 0, join_algorithm = 'hash', query_plan_join_swap_table = 'auto';

DROP TABLE IF EXISTS mem_r;
DROP TABLE IF EXISTS mem_s;
DROP TABLE IF EXISTS mem_p;
DROP TABLE IF EXISTS mem_e;

CREATE TABLE mem_r (id UInt64) ENGINE = Memory;
CREATE TABLE mem_s (id UInt64, v UInt64) ENGINE = Memory;
CREATE TABLE mem_p (id UInt64, v UInt64) ENGINE = Memory;
CREATE TABLE mem_e (id UInt64, v UInt64) ENGINE = Memory;
CREATE TEMPORARY TABLE tmp_s (id UInt64, v UInt64);

INSERT INTO mem_r SELECT number FROM numbers(100);
INSERT INTO mem_s SELECT number, number FROM numbers(1000);
INSERT INTO mem_p SELECT number, number FROM numbers(1000);
INSERT INTO tmp_s SELECT number, number FROM numbers(1000);

DROP ROW POLICY IF EXISTS policy_05497 ON mem_p;
CREATE ROW POLICY policy_05497 ON mem_p FOR SELECT USING v = 5 TO CURRENT_USER;

-- `s` keeps 1 of its 1000 rows. Each line counts the plans whose join reads `mem_r` first (`s` is the build side),
-- those reading `s` first, and those showing no row estimate for `s`.

SELECT 'WHERE', countIf(explain ILIKE '%Join: mem_r%'), countIf(explain ILIKE '%Join: s%'), countIf(explain ILIKE '%s[no_stats~?]%')
FROM (EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM mem_r INNER JOIN (SELECT id FROM mem_s WHERE v = 5) AS s ON mem_r.id = s.id)
WHERE explain ILIKE '%Join:%';

SELECT 'PREWHERE', countIf(explain ILIKE '%Join: mem_r%'), countIf(explain ILIKE '%Join: s%'), countIf(explain ILIKE '%s[no_stats~?]%')
FROM (EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM mem_r INNER JOIN (SELECT id FROM mem_s PREWHERE v = 5) AS s ON mem_r.id = s.id)
WHERE explain ILIKE '%Join:%';

SELECT 'row policy', countIf(explain ILIKE '%Join: mem_r%'), countIf(explain ILIKE '%Join: s%'), countIf(explain ILIKE '%s[no_stats~?]%')
FROM (EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM mem_r INNER JOIN (SELECT id FROM mem_p) AS s ON mem_r.id = s.id)
WHERE explain ILIKE '%Join:%';

SELECT 'temporary table', countIf(explain ILIKE '%Join: mem_r%'), countIf(explain ILIKE '%Join: s%'), countIf(explain ILIKE '%s[no_stats~?]%')
FROM (EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM mem_r INNER JOIN (SELECT id FROM tmp_s WHERE v = 5) AS s ON mem_r.id = s.id)
WHERE explain ILIKE '%Join:%';

-- Without a condition, `s` is estimated at its 1000 rows and `mem_r` is the build side.
SELECT 'no condition', countIf(explain ILIKE '%Join: mem_r%'), countIf(explain ILIKE '%Join: s%'), countIf(explain ILIKE '%s[no_stats~?]%')
FROM (EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM mem_r INNER JOIN (SELECT id FROM mem_s) AS s ON mem_r.id = s.id)
WHERE explain ILIKE '%Join:%';

-- A condition on an empty table keeps its estimate of 0 rows.
SELECT 'empty table', countIf(explain ILIKE '%Join: mem_r%'), countIf(explain ILIKE '%Join: s%'), countIf(explain ILIKE '%s[0]%')
FROM (EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM (SELECT id FROM mem_e WHERE v = 5) AS s INNER JOIN mem_r ON mem_r.id = s.id)
WHERE explain ILIKE '%Join:%';

-- An aggregation without keys returns one row, so it is estimated at one row and is the build side even with a condition below it.
SELECT 'keyless aggregate', countIf(explain ILIKE '%Join: mem_r%'), countIf(explain ILIKE '%Join: s%'), countIf(explain ILIKE '%s[no_stats~1]%')
FROM (EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM (SELECT count() AS id FROM mem_s WHERE v = 5) AS s INNER JOIN mem_r ON mem_r.id = s.id)
WHERE explain ILIKE '%Join:%';

SELECT count() FROM mem_r INNER JOIN (SELECT id FROM mem_s WHERE v = 5) AS s ON mem_r.id = s.id;
SELECT count() FROM mem_r INNER JOIN (SELECT id FROM mem_s PREWHERE v = 5) AS s ON mem_r.id = s.id;
SELECT count() FROM mem_r INNER JOIN (SELECT id FROM mem_p) AS s ON mem_r.id = s.id;

DROP ROW POLICY policy_05497 ON mem_p;
DROP TABLE mem_r;
DROP TABLE mem_s;
DROP TABLE mem_p;
DROP TABLE mem_e;
DROP TEMPORARY TABLE tmp_s;
