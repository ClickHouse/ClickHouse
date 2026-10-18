-- Random settings limits: query_plan_optimize_join_order_randomize=(0, 0); query_plan_optimize_join_order_limit=(1, None); use_statistics=(1, None); materialize_statistics_on_insert=(1, None)

-- A hash join builds from an array join input whose read selects no rows,
-- and keeps the orientation when that input is not proven empty.

SET enable_parallel_replicas = 0, join_algorithm = 'hash', query_plan_join_swap_table = 'auto';

DROP TABLE IF EXISTS aj_l;
DROP TABLE IF EXISTS aj_r;
DROP TABLE IF EXISTS aj_big;
DROP TABLE IF EXISTS aj_rep;
DROP TABLE IF EXISTS aj_sum;
DROP TABLE IF EXISTS aj_ren;
DROP TABLE IF EXISTS aj_alt;

CREATE TABLE aj_l (id UInt64, arr Array(UInt64)) ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = '';
CREATE TABLE aj_r (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = '';
CREATE TABLE aj_big (id UInt64, arr Array(UInt64)) ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = '';

INSERT INTO aj_l SELECT number, [number, number + 1] FROM numbers(100);
INSERT INTO aj_r SELECT number FROM numbers(1000);
INSERT INTO aj_big SELECT 1, range(10000);

CREATE TABLE aj_rep (id UInt64, ts UInt64, arr Array(UInt64), ver UInt64) ENGINE = ReplacingMergeTree(ver) ORDER BY id SETTINGS auto_statistics_types = 'minmax';
INSERT INTO aj_rep SELECT number, number, [number, number + 1], 1 FROM numbers(100);

CREATE TABLE aj_sum (id UInt64, v UInt64) ENGINE = SummingMergeTree ORDER BY id SETTINGS auto_statistics_types = 'minmax';
SYSTEM STOP MERGES aj_sum;
INSERT INTO aj_sum SETTINGS async_insert = 0 VALUES (1, 3);
INSERT INTO aj_sum SETTINGS async_insert = 0 VALUES (1, 4);

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

-- Column statistics prove that no row has `ts > 1000`, also without part pruning and under FINAL of ReplacingMergeTree.
SELECT 'statistics without part pruning', countIf(explain ILIKE '%Join: aj_r%'), countIf(explain ILIKE '%Join: s%')
FROM (EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM (SELECT arrayJoin(arr) AS k FROM aj_rep WHERE ts > 1000) AS s INNER JOIN aj_r ON aj_r.id = s.k
    SETTINGS use_statistics_for_part_pruning = 0, query_plan_lower_array_join_function = 0)
WHERE explain ILIKE '%Join:%';

SELECT 'statistics, ReplacingMergeTree FINAL', countIf(explain ILIKE '%Join: aj_r%'), countIf(explain ILIKE '%Join: s%')
FROM (EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM (SELECT arrayJoin(arr) AS k FROM aj_rep FINAL WHERE ts > 1000) AS s INNER JOIN aj_r ON aj_r.id = s.k
    SETTINGS query_plan_lower_array_join_function = 1)
WHERE explain ILIKE '%Join:%';

-- FINAL of SummingMergeTree returns (1, 7), although no stored row has `v >= 5`.
SELECT 'SummingMergeTree FINAL estimated at 0 rows', countIf(explain ILIKE '%Join: aj_r%'), countIf(explain ILIKE '%Join: s%')
FROM (EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM (SELECT id AS k FROM aj_sum FINAL WHERE v >= 5) AS s INNER JOIN aj_r ON aj_r.id = s.k)
WHERE explain ILIKE '%Join:%';

SELECT 'arrayJoin over SummingMergeTree FINAL', countIf(explain ILIKE '%Join: aj_r%'), countIf(explain ILIKE '%Join: s%')
FROM (EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM (SELECT arrayJoin(range(10000)) AS k FROM aj_sum FINAL WHERE v >= 5) AS s INNER JOIN aj_r ON aj_r.id = s.k)
WHERE explain ILIKE '%Join:%';

-- The statistics of a part with a pending RENAME or a killed MODIFY COLUMN describe other values than the ones read.
CREATE TABLE aj_ren (id UInt64, a Int64, b Int64, arr Array(UInt64)) ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = 'minmax';
INSERT INTO aj_ren SELECT 1, -1, 500, range(10000);
SYSTEM STOP MERGES aj_ren;
ALTER TABLE aj_ren RENAME COLUMN a TO tmp, RENAME COLUMN b TO a SETTINGS mutations_sync = 0, alter_sync = 0;

CREATE TABLE aj_alt (id UInt64, ts Int64, arr Array(UInt64)) ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = 'minmax';
INSERT INTO aj_alt SELECT 1, -1, range(10000);
SYSTEM STOP MERGES aj_alt;
ALTER TABLE aj_alt MODIFY COLUMN ts UInt8 SETTINGS mutations_sync = 0, alter_sync = 0;
KILL MUTATION WHERE database = currentDatabase() AND table = 'aj_alt' SYNC FORMAT Null;

SELECT 'pending RENAME estimated at 0 rows', countIf(explain ILIKE '%Join: aj_r%'), countIf(explain ILIKE '%Join: s%')
FROM (EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM (SELECT id AS k FROM aj_ren WHERE a > 200) AS s INNER JOIN aj_r ON aj_r.id = s.k
    SETTINGS use_statistics_for_part_pruning = 0)
WHERE explain ILIKE '%Join:%';

SELECT 'arrayJoin over pending RENAME', countIf(explain ILIKE '%Join: aj_r%'), countIf(explain ILIKE '%Join: s%')
FROM (EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM (SELECT arrayJoin(arr) AS k FROM aj_ren WHERE a > 200) AS s INNER JOIN aj_r ON aj_r.id = s.k
    SETTINGS use_statistics_for_part_pruning = 0)
WHERE explain ILIKE '%Join:%';

SELECT 'killed MODIFY COLUMN estimated at 0 rows', countIf(explain ILIKE '%Join: aj_r%'), countIf(explain ILIKE '%Join: s%')
FROM (EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM (SELECT id AS k FROM aj_alt WHERE ts > 200) AS s INNER JOIN aj_r ON aj_r.id = s.k
    SETTINGS use_statistics_for_part_pruning = 0)
WHERE explain ILIKE '%Join:%';

SELECT 'arrayJoin over killed MODIFY COLUMN', countIf(explain ILIKE '%Join: aj_r%'), countIf(explain ILIKE '%Join: s%')
FROM (EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM (SELECT arrayJoin(arr) AS k FROM aj_alt WHERE ts > 200) AS s INNER JOIN aj_r ON aj_r.id = s.k
    SETTINGS use_statistics_for_part_pruning = 0)
WHERE explain ILIKE '%Join:%';

SELECT count() FROM (SELECT arrayJoin(arr) AS k FROM aj_l WHERE id > 1000) AS s LEFT JOIN aj_r ON aj_r.id = s.k;
SELECT count() FROM (SELECT arrayJoin(arr) AS k FROM aj_big) AS s INNER JOIN aj_r ON aj_r.id = s.k;
SELECT count() FROM (SELECT arrayJoin(range(10000)) AS k FROM (SELECT count() AS c FROM aj_l WHERE id > 1000)) AS s INNER JOIN aj_r ON aj_r.id = s.k;
SELECT count() FROM (SELECT arrayJoin(arr) AS k FROM aj_rep WHERE ts > 1000) AS s INNER JOIN aj_r ON aj_r.id = s.k;
SELECT count() FROM (SELECT arrayJoin(arr) AS k FROM aj_rep FINAL WHERE ts > 1000) AS s INNER JOIN aj_r ON aj_r.id = s.k;
SELECT count() FROM (SELECT id AS k FROM aj_sum FINAL WHERE v >= 5) AS s INNER JOIN aj_r ON aj_r.id = s.k;
SELECT count() FROM (SELECT arrayJoin(range(10000)) AS k FROM aj_sum FINAL WHERE v >= 5) AS s INNER JOIN aj_r ON aj_r.id = s.k;
SELECT count() FROM (SELECT arrayJoin(arr) AS k FROM aj_ren WHERE a > 200) AS s INNER JOIN aj_r ON aj_r.id = s.k SETTINGS use_statistics_for_part_pruning = 0;
SELECT count() FROM (SELECT arrayJoin(arr) AS k FROM aj_alt WHERE ts > 200) AS s INNER JOIN aj_r ON aj_r.id = s.k SETTINGS use_statistics_for_part_pruning = 0;

DROP TABLE aj_l;
DROP TABLE aj_r;
DROP TABLE aj_big;
DROP TABLE aj_rep;
DROP TABLE aj_sum;
DROP TABLE aj_ren;
DROP TABLE aj_alt;
