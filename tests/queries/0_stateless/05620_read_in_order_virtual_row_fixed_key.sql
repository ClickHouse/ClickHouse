-- A key column fixed by the filter (`WHERE key = constant`) does not determine the direction of reading and may be absent from
-- `ORDER BY`. The virtual row of reading in order is an index entry, which bounds the rows of its part in the order of the
-- sorting key, so the merge compares such a column too: as it is in `ORDER BY` if the read goes in its direction, as a hidden
-- column with the fixed value otherwise. Reading in order through an `INNER JOIN` needs the virtual row.

DROP TABLE IF EXISTS t_fixed_key;
DROP TABLE IF EXISTS t_fixed_key_3;
DROP TABLE IF EXISTS t_fixed_key_expr;
DROP TABLE IF EXISTS t_fixed_key_ts;
DROP TABLE IF EXISTS t_fixed_key_right;
DROP TABLE IF EXISTS t_fixed_key_final;

SET optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, read_in_order_use_virtual_row_per_block = 0;
SET read_in_order_two_level_merge_threshold = 100, max_threads = 1, max_block_size = 8, enable_parallel_replicas = 0;
SET max_bytes_ratio_before_external_join = 0, max_bytes_before_external_join = 0, query_plan_read_in_order_through_join = 1, join_runtime_filter_min_probe_rows = 0;
SET query_plan_optimize_join_order_limit = 1, query_plan_optimize_join_order_randomize = 0, query_plan_join_swap_table = 0;

CREATE TABLE t_fixed_key (a UInt32, b UInt32, x UInt32) ENGINE = MergeTree ORDER BY (a, b) SETTINGS index_granularity = 8;
SYSTEM STOP MERGES t_fixed_key;

-- The granule where `a = 1` begins starts with `a = 0`, the one where it ends is followed by `a = 2`.
INSERT INTO t_fixed_key SELECT intDiv(number, 100) AS a, if(a = 1, 10 * (number % 100) + 1, 100000 + number) AS b, b % 4 FROM numbers(300);
-- Begins with `a = 1`.
INSERT INTO t_fixed_key SELECT if(number < 80, 1, 2) AS a, if(a = 1, 10 * number + 2, 200000 + number) AS b, b % 4 FROM numbers(120);
-- Ends with `a = 1`.
INSERT INTO t_fixed_key SELECT if(number < 44, 0, 1) AS a, if(a = 1, 10 * (number - 44) + 5, 400000 + number) AS b, b % 4 FROM numbers(128);
-- No rows with `a = 1`, but the granule where `a` changes from 0 to 2 is selected.
INSERT INTO t_fixed_key SELECT if(number < 50, 0, 2), 300000 + number AS b, b % 4 FROM numbers(100);
-- Only `a = 1`, far from both ends of the result.
INSERT INTO t_fixed_key SELECT 1, 5000 + number AS b, b % 4 FROM numbers(64);
INSERT INTO t_fixed_key SELECT 1, 6000 + number AS b, b % 4 FROM numbers(64);
INSERT INTO t_fixed_key SELECT 1, 7000 + number AS b, b % 4 FROM numbers(64);
INSERT INTO t_fixed_key SELECT 1, 8000 + number AS b, b % 4 FROM numbers(64);

SELECT '--- key (a, b): plan';

SELECT '-- WHERE a = 1 ORDER BY a, b';
SELECT extract(explain, '(?:Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0 SELECT b FROM t_fixed_key WHERE a = 1 ORDER BY a, b LIMIT 5) WHERE e != '';

SELECT '-- WHERE a = 1 ORDER BY a DESC, b DESC';
SELECT extract(explain, '(?:Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0 SELECT b FROM t_fixed_key WHERE a = 1 ORDER BY a DESC, b DESC LIMIT 5) WHERE e != '';

SELECT '-- WHERE a IN (1) ORDER BY a, b';
SELECT extract(explain, '(?:Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0 SELECT b FROM t_fixed_key WHERE a IN (1) ORDER BY a, b LIMIT 5) WHERE e != '';

SELECT '-- WHERE a = 1 ORDER BY a DESC, b: the read does not go in the direction of the fixed column';
SELECT extract(explain, '(?:Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0 SELECT b FROM t_fixed_key WHERE a = 1 ORDER BY a DESC, b LIMIT 5) WHERE e != '';

SELECT '-- WHERE a = 1 ORDER BY b: the fixed key column is not listed';
SELECT extract(explain, '(?:Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0 SELECT b FROM t_fixed_key WHERE a = 1 ORDER BY b LIMIT 5) WHERE e != '';

SELECT '-- WHERE a = 1 ORDER BY b DESC';
SELECT extract(explain, '(?:Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0 SELECT b FROM t_fixed_key WHERE a = 1 ORDER BY b DESC LIMIT 5) WHERE e != '';

SELECT '-- WHERE x = 1 ORDER BY x, a, b: a fixed column which is not in the sorting key';
SELECT extract(explain, '(?:Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0 SELECT a, b FROM t_fixed_key WHERE x = 1 ORDER BY x, a, b LIMIT 5) WHERE e != '';

SELECT '-- WHERE toString(a) = \'1\' ORDER BY b: the fixed value is not known, no virtual row';
SELECT extract(explain, '(?:Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0 SELECT b FROM t_fixed_key WHERE toString(a) = '1' ORDER BY b LIMIT 5) WHERE e != '';

SELECT '--- key (a, b): result';

SELECT b FROM t_fixed_key WHERE a = 1 ORDER BY a, b LIMIT 5;
SELECT b FROM t_fixed_key WHERE a = 1 ORDER BY a DESC, b DESC LIMIT 5;

SELECT (SELECT groupArray(b) FROM (SELECT b FROM t_fixed_key WHERE a = 1 ORDER BY a, b LIMIT 40))
     = (SELECT arraySlice(arraySort(groupArray(b)), 1, 40) FROM t_fixed_key WHERE a = 1);
SELECT (SELECT groupArray(b) FROM (SELECT b FROM t_fixed_key WHERE a = 1 ORDER BY a, b))
     = (SELECT arraySort(groupArray(b)) FROM t_fixed_key WHERE a = 1);
SELECT (SELECT groupArray(b) FROM (SELECT b FROM t_fixed_key WHERE a = 1 ORDER BY a DESC, b DESC LIMIT 40))
     = (SELECT arraySlice(arrayReverseSort(groupArray(b)), 1, 40) FROM t_fixed_key WHERE a = 1);
SELECT (SELECT groupArray(b) FROM (SELECT b FROM t_fixed_key WHERE a = 1 ORDER BY a DESC, b DESC))
     = (SELECT arrayReverseSort(groupArray(b)) FROM t_fixed_key WHERE a = 1);
SELECT (SELECT groupArray(b) FROM (SELECT b FROM t_fixed_key WHERE a IN (1) ORDER BY a, b LIMIT 40))
     = (SELECT arraySlice(arraySort(groupArray(b)), 1, 40) FROM t_fixed_key WHERE a = 1);

SELECT '-- the fixed column in the other direction, or not listed';
SELECT (SELECT groupArray(b) FROM (SELECT b FROM t_fixed_key WHERE a = 1 ORDER BY a DESC, b LIMIT 40))
     = (SELECT arraySlice(arraySort(groupArray(b)), 1, 40) FROM t_fixed_key WHERE a = 1);
SELECT (SELECT groupArray(b) FROM (SELECT b FROM t_fixed_key WHERE a = 1 ORDER BY a, b DESC LIMIT 40))
     = (SELECT arraySlice(arrayReverseSort(groupArray(b)), 1, 40) FROM t_fixed_key WHERE a = 1);
SELECT (SELECT groupArray(b) FROM (SELECT b FROM t_fixed_key WHERE a = 1 ORDER BY b LIMIT 40))
     = (SELECT arraySlice(arraySort(groupArray(b)), 1, 40) FROM t_fixed_key WHERE a = 1);
SELECT (SELECT groupArray(b) FROM (SELECT b FROM t_fixed_key WHERE a = 1 ORDER BY b))
     = (SELECT arraySort(groupArray(b)) FROM t_fixed_key WHERE a = 1);
SELECT (SELECT groupArray(b) FROM (SELECT b FROM t_fixed_key WHERE a = 1 ORDER BY b DESC LIMIT 40))
     = (SELECT arraySlice(arrayReverseSort(groupArray(b)), 1, 40) FROM t_fixed_key WHERE a = 1);
SELECT (SELECT groupArray(b) FROM (SELECT b FROM t_fixed_key WHERE a = 1 ORDER BY b DESC))
     = (SELECT arrayReverseSort(groupArray(b)) FROM t_fixed_key WHERE a = 1);
SELECT (SELECT groupArray(b) FROM (SELECT b FROM t_fixed_key WHERE toString(a) = '1' ORDER BY b LIMIT 40))
     = (SELECT arraySlice(arraySort(groupArray(b)), 1, 40) FROM t_fixed_key WHERE a = 1);

SELECT '-- a fixed column which is not in the sorting key';
SELECT (SELECT groupArray((a, b)) FROM (SELECT a, b FROM t_fixed_key WHERE x = 1 ORDER BY x, a, b LIMIT 40))
     = (SELECT arraySlice(arraySort(groupArray((a, b))), 1, 40) FROM t_fixed_key WHERE x = 1);
SELECT (SELECT groupArray(b) FROM (SELECT b FROM t_fixed_key WHERE a = 1 AND x = 1 ORDER BY x, b))
     = (SELECT arraySort(groupArray(b)) FROM t_fixed_key WHERE a = 1 AND x = 1);

SELECT '-- a virtual row after each block, several threads';
SELECT (SELECT groupArray(b) FROM (SELECT b FROM t_fixed_key WHERE a = 1 ORDER BY b LIMIT 40))
     = (SELECT arraySlice(arraySort(groupArray(b)), 1, 40) FROM t_fixed_key WHERE a = 1)
SETTINGS read_in_order_use_virtual_row_per_block = 1;
SELECT (SELECT groupArray(b) FROM (SELECT b FROM t_fixed_key WHERE a = 1 ORDER BY a DESC, b))
     = (SELECT arraySort(groupArray(b)) FROM t_fixed_key WHERE a = 1)
SETTINGS max_threads = 4;
SELECT (SELECT groupArray(b) FROM (SELECT b FROM t_fixed_key WHERE a = 1 ORDER BY a, b LIMIT 40))
     = (SELECT arraySlice(arraySort(groupArray(b)), 1, 40) FROM t_fixed_key WHERE a = 1)
SETTINGS read_in_order_use_virtual_row_per_block = 1;
SELECT (SELECT groupArray(b) FROM (SELECT b FROM t_fixed_key WHERE a = 1 ORDER BY a DESC, b DESC LIMIT 40))
     = (SELECT arraySlice(arrayReverseSort(groupArray(b)), 1, 40) FROM t_fixed_key WHERE a = 1)
SETTINGS read_in_order_use_virtual_row_per_block = 1;
SELECT (SELECT groupArray(b) FROM (SELECT b FROM t_fixed_key WHERE a = 1 ORDER BY a, b))
     = (SELECT arraySort(groupArray(b)) FROM t_fixed_key WHERE a = 1)
SETTINGS max_threads = 4;
SELECT (SELECT groupArray(b) FROM (SELECT b FROM t_fixed_key WHERE a = 1 ORDER BY a DESC, b DESC))
     = (SELECT arrayReverseSort(groupArray(b)) FROM t_fixed_key WHERE a = 1)
SETTINGS max_threads = 4, read_in_order_use_virtual_row_per_block = 1;

SELECT '--- key (a, b): the parts which begin with `a = 1` far from the result are not read';

SELECT b FROM t_fixed_key WHERE a = 1 ORDER BY a, b LIMIT 5 FORMAT Null SETTINGS log_comment = '05620_virtual_row';
SELECT b FROM t_fixed_key WHERE a = 1 ORDER BY a, b LIMIT 5 FORMAT Null SETTINGS read_in_order_use_virtual_row = 0, log_comment = '05620_no_virtual_row';
SYSTEM FLUSH LOGS query_log;

SELECT
    (SELECT read_rows FROM system.query_log
     WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05620_virtual_row' AND event_date >= yesterday()
     ORDER BY event_time_microseconds DESC LIMIT 1)
  < (SELECT read_rows FROM system.query_log
     WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05620_no_virtual_row' AND event_date >= yesterday()
     ORDER BY event_time_microseconds DESC LIMIT 1);

SELECT '--- key (a, b, c)';

CREATE TABLE t_fixed_key_3 (a UInt32, b UInt32, c UInt32) ENGINE = MergeTree ORDER BY (a, b, c) SETTINGS index_granularity = 8;
SYSTEM STOP MERGES t_fixed_key_3;

INSERT INTO t_fixed_key_3 SELECT intDiv(number, 40), intDiv(number, 10) % 4, ((number * 37) % 120) * 10 + 1 FROM numbers(120);
INSERT INTO t_fixed_key_3 SELECT 1 + intDiv(number, 40), intDiv(number, 10) % 4, ((number * 37) % 80) * 10 + 2 FROM numbers(80);
INSERT INTO t_fixed_key_3 SELECT intDiv(number, 30), 1 + intDiv(number, 10) % 3, ((number * 37) % 90) * 10 + 3 FROM numbers(90);
INSERT INTO t_fixed_key_3 SELECT if(number < 32, 1, 2), if(number < 16, 2, 3), ((number * 37) % 64) * 10 + 4 FROM numbers(64);
INSERT INTO t_fixed_key_3 SELECT if(number < 40, 0, 2), intDiv(number, 10) % 4, ((number * 37) % 80) * 10 + 5 FROM numbers(80);

SELECT '-- fixed middle key: WHERE b = 1 ORDER BY a, b, c';
SELECT extract(explain, '(?:Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0 SELECT a, c FROM t_fixed_key_3 WHERE b = 1 ORDER BY a, b, c LIMIT 5) WHERE e != '';

SELECT (SELECT groupArray((a, c)) FROM (SELECT a, c FROM t_fixed_key_3 WHERE b = 1 ORDER BY a, b, c LIMIT 20))
     = (SELECT arraySlice(arraySort(groupArray((a, c))), 1, 20) FROM t_fixed_key_3 WHERE b = 1);
SELECT (SELECT groupArray((a, c)) FROM (SELECT a, c FROM t_fixed_key_3 WHERE b = 1 ORDER BY a DESC, b DESC, c DESC))
     = (SELECT arrayReverseSort(groupArray((a, c))) FROM t_fixed_key_3 WHERE b = 1);
SELECT (SELECT groupArray((a, c)) FROM (SELECT a, c FROM t_fixed_key_3 WHERE b = 1 ORDER BY a, b DESC, c))
     = (SELECT arraySort(groupArray((a, c))) FROM t_fixed_key_3 WHERE b = 1);

SELECT '-- fixed middle key, not listed: WHERE b = 1 ORDER BY a, c';
SELECT extract(explain, '(?:Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0 SELECT a, c FROM t_fixed_key_3 WHERE b = 1 ORDER BY a, c LIMIT 5) WHERE e != '';

SELECT (SELECT groupArray((a, c)) FROM (SELECT a, c FROM t_fixed_key_3 WHERE b = 1 ORDER BY a, c LIMIT 20))
     = (SELECT arraySlice(arraySort(groupArray((a, c))), 1, 20) FROM t_fixed_key_3 WHERE b = 1);
SELECT (SELECT groupArray((a, c)) FROM (SELECT a, c FROM t_fixed_key_3 WHERE b = 1 ORDER BY a DESC, c DESC))
     = (SELECT arrayReverseSort(groupArray((a, c))) FROM t_fixed_key_3 WHERE b = 1);

SELECT '-- two fixed keys, not listed: WHERE a = 1 AND b = 2 ORDER BY c';
SELECT extract(explain, '(?:Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0 SELECT c FROM t_fixed_key_3 WHERE a = 1 AND b = 2 ORDER BY c LIMIT 5) WHERE e != '';

SELECT (SELECT groupArray(c) FROM (SELECT c FROM t_fixed_key_3 WHERE a = 1 AND b = 2 ORDER BY c))
     = (SELECT arraySort(groupArray(c)) FROM t_fixed_key_3 WHERE a = 1 AND b = 2);
SELECT (SELECT groupArray(c) FROM (SELECT c FROM t_fixed_key_3 WHERE a = 1 AND b = 2 ORDER BY c DESC LIMIT 20))
     = (SELECT arraySlice(arrayReverseSort(groupArray(c)), 1, 20) FROM t_fixed_key_3 WHERE a = 1 AND b = 2);

SELECT '-- two fixed keys: WHERE a = 1 AND b = 2 ORDER BY a, b, c';
SELECT extract(explain, '(?:Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0 SELECT c FROM t_fixed_key_3 WHERE a = 1 AND b = 2 ORDER BY a, b, c LIMIT 5) WHERE e != '';

SELECT (SELECT groupArray(c) FROM (SELECT c FROM t_fixed_key_3 WHERE a = 1 AND b = 2 ORDER BY a, b, c LIMIT 20))
     = (SELECT arraySlice(arraySort(groupArray(c)), 1, 20) FROM t_fixed_key_3 WHERE a = 1 AND b = 2);
SELECT (SELECT groupArray(c) FROM (SELECT c FROM t_fixed_key_3 WHERE a = 1 AND b = 2 ORDER BY a DESC, b, c DESC))
     = (SELECT arrayReverseSort(groupArray(c)) FROM t_fixed_key_3 WHERE a = 1 AND b = 2);

SELECT '--- key (toDate(ts), b)';

CREATE TABLE t_fixed_key_expr (ts DateTime('UTC'), b UInt32) ENGINE = MergeTree ORDER BY (toDate(ts), b) SETTINGS index_granularity = 8;
SYSTEM STOP MERGES t_fixed_key_expr;

INSERT INTO t_fixed_key_expr SELECT toDateTime('2024-01-01 00:00:00', 'UTC') + intDiv(number, 60) * 86400 + number % 60 * 600, if(intDiv(number, 60) = 1, 10 * (number % 60) + 1, 100000 + number) FROM numbers(180);
INSERT INTO t_fixed_key_expr SELECT toDateTime('2024-01-02 00:00:00', 'UTC') + intDiv(number, 48) * 86400 + number % 48 * 600, if(intDiv(number, 48) = 0, 10 * (number % 48) + 2, 200000 + number) FROM numbers(96);
INSERT INTO t_fixed_key_expr SELECT toDateTime('2024-01-02 00:00:00', 'UTC') + number * 600, 5000 + number FROM numbers(64);

SELECT extract(explain, '(?:Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0 SELECT b FROM t_fixed_key_expr WHERE toDate(ts) = '2024-01-02' ORDER BY toDate(ts), b LIMIT 5) WHERE e != '';

SELECT (SELECT groupArray(b) FROM (SELECT b FROM t_fixed_key_expr WHERE toDate(ts) = '2024-01-02' ORDER BY toDate(ts), b LIMIT 20))
     = (SELECT arraySlice(arraySort(groupArray(b)), 1, 20) FROM t_fixed_key_expr WHERE toDate(ts) = '2024-01-02');
SELECT (SELECT groupArray(b) FROM (SELECT b FROM t_fixed_key_expr WHERE toDate(ts) = '2024-01-02' ORDER BY toDate(ts) DESC, b))
     = (SELECT arraySort(groupArray(b)) FROM t_fixed_key_expr WHERE toDate(ts) = '2024-01-02');

SELECT '-- not listed';
SELECT extract(explain, '(?:Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0 SELECT b FROM t_fixed_key_expr WHERE toDate(ts) = '2024-01-02' ORDER BY b LIMIT 5) WHERE e != '';

SELECT (SELECT groupArray(b) FROM (SELECT b FROM t_fixed_key_expr WHERE toDate(ts) = '2024-01-02' ORDER BY b))
     = (SELECT arraySort(groupArray(b)) FROM t_fixed_key_expr WHERE toDate(ts) = '2024-01-02');

SELECT '--- key (ts, b): `toDate(ts) = ...` fixes `toDate(ts)` but not the key column `ts`, which still determines the order';

CREATE TABLE t_fixed_key_ts (ts DateTime('UTC'), b UInt32) ENGINE = MergeTree ORDER BY (ts, b) SETTINGS index_granularity = 8;
SYSTEM STOP MERGES t_fixed_key_ts;

INSERT INTO t_fixed_key_ts SELECT toDateTime('2024-01-01 00:00:00', 'UTC') + number * 3600, 1000 - number FROM numbers(60);
INSERT INTO t_fixed_key_ts SELECT toDateTime('2024-01-01 00:30:00', 'UTC') + number * 3600, 2000 - number FROM numbers(60);

SELECT b FROM t_fixed_key_ts WHERE toDate(ts) = '2024-01-01' ORDER BY toDate(ts), b LIMIT 5;
SELECT b FROM t_fixed_key_ts WHERE toDate(ts) = '2024-01-01' ORDER BY toDate(ts) DESC, b LIMIT 5;

SELECT '-- the key column is fixed and listed through a function';
SELECT extract(explain, '(?:Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0 SELECT b FROM t_fixed_key_ts WHERE ts = '2024-01-01 05:00:00' ORDER BY toDate(ts), b LIMIT 5) WHERE e != '';
SELECT b FROM t_fixed_key_ts WHERE ts = '2024-01-01 05:00:00' ORDER BY toDate(ts), b LIMIT 5;

SELECT '--- INNER JOIN: reading in order needs the virtual row';

CREATE TABLE t_fixed_key_right (b UInt32, w UInt32) ENGINE = MergeTree ORDER BY b;
INSERT INTO t_fixed_key_right SELECT number, number * 3 FROM numbers(9000) WHERE number % 7 = 0;

SELECT extract(explain, '(?:Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0
    SELECT l.b, r.w FROM t_fixed_key AS l INNER JOIN t_fixed_key_right AS r ON l.b = r.b WHERE l.a = 1 ORDER BY l.a, l.b LIMIT 5) WHERE e != '';
SELECT extract(explain, '(?:Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0
    SELECT l.b, r.w FROM t_fixed_key AS l INNER JOIN t_fixed_key_right AS r ON l.b = r.b WHERE l.a = 1 ORDER BY l.a DESC, l.b DESC LIMIT 5) WHERE e != '';

SELECT l.b, r.w FROM t_fixed_key AS l INNER JOIN t_fixed_key_right AS r ON l.b = r.b WHERE l.a = 1 ORDER BY l.a, l.b LIMIT 5;
SELECT l.b, r.w FROM t_fixed_key AS l INNER JOIN t_fixed_key_right AS r ON l.b = r.b WHERE l.a = 1 ORDER BY l.a DESC, l.b DESC LIMIT 5;

SELECT '-- the fixed key column is not listed, or is listed in the other direction';
SELECT extract(explain, '(?:Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0
    SELECT l.b, r.w FROM t_fixed_key AS l INNER JOIN t_fixed_key_right AS r ON l.b = r.b WHERE l.a = 1 ORDER BY l.b LIMIT 5) WHERE e != '';
SELECT extract(explain, '(?:Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0
    SELECT l.b, r.w FROM t_fixed_key AS l INNER JOIN t_fixed_key_right AS r ON l.b = r.b WHERE l.a = 1 ORDER BY l.a DESC, l.b LIMIT 5) WHERE e != '';

SELECT l.b, r.w FROM t_fixed_key AS l INNER JOIN t_fixed_key_right AS r ON l.b = r.b WHERE l.a = 1 ORDER BY l.b LIMIT 5;
SELECT l.b, r.w FROM t_fixed_key AS l INNER JOIN t_fixed_key_right AS r ON l.b = r.b WHERE l.a = 1 ORDER BY l.b DESC LIMIT 5;
SELECT l.b, r.w FROM t_fixed_key AS l INNER JOIN t_fixed_key_right AS r ON l.b = r.b WHERE l.a = 1 ORDER BY l.a DESC, l.b LIMIT 5;

SELECT '-- with FINAL';

CREATE TABLE t_fixed_key_final (a UInt32, b UInt32, ver UInt32) ENGINE = ReplacingMergeTree(ver) ORDER BY (a, b) SETTINGS index_granularity = 8;
SYSTEM STOP MERGES t_fixed_key_final;

INSERT INTO t_fixed_key_final SELECT intDiv(number, 100) AS a, if(a = 1, 7 * (number % 100), 100000 + number), 1 FROM numbers(300);
INSERT INTO t_fixed_key_final SELECT if(number < 44, 0, 1) AS a, if(a = 1, 7 * (number - 44), 400000 + number), 2 FROM numbers(128);
INSERT INTO t_fixed_key_final SELECT 1, 7 * number + 3500, 3 FROM numbers(64);

SELECT extract(explain, '(?:Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0
    SELECT l.b, l.ver, r.w FROM t_fixed_key_final AS l FINAL INNER JOIN t_fixed_key_right AS r ON l.b = r.b WHERE l.a = 1 ORDER BY l.a, l.b LIMIT 5) WHERE e != '';

SELECT l.b, l.ver, r.w FROM t_fixed_key_final AS l FINAL INNER JOIN t_fixed_key_right AS r ON l.b = r.b WHERE l.a = 1 ORDER BY l.a, l.b LIMIT 5;
SELECT (SELECT groupArray((b, ver)) FROM (SELECT l.b AS b, l.ver AS ver FROM t_fixed_key_final AS l FINAL INNER JOIN t_fixed_key_right AS r ON l.b = r.b WHERE l.a = 1 ORDER BY l.a, l.b))
     = (SELECT arraySort(groupArray((b, ver))) FROM (SELECT l.b AS b, l.ver AS ver FROM t_fixed_key_final AS l FINAL INNER JOIN t_fixed_key_right AS r ON l.b = r.b WHERE l.a = 1));

SELECT extract(explain, '(?:Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0
    SELECT l.b, l.ver, r.w FROM t_fixed_key_final AS l FINAL INNER JOIN t_fixed_key_right AS r ON l.b = r.b WHERE l.a = 1 ORDER BY l.b LIMIT 5) WHERE e != '';
SELECT (SELECT groupArray((b, ver)) FROM (SELECT l.b AS b, l.ver AS ver FROM t_fixed_key_final AS l FINAL INNER JOIN t_fixed_key_right AS r ON l.b = r.b WHERE l.a = 1 ORDER BY l.b))
     = (SELECT arraySort(groupArray((b, ver))) FROM (SELECT l.b AS b, l.ver AS ver FROM t_fixed_key_final AS l FINAL INNER JOIN t_fixed_key_right AS r ON l.b = r.b WHERE l.a = 1));

DROP TABLE t_fixed_key;
DROP TABLE t_fixed_key_3;
DROP TABLE t_fixed_key_expr;
DROP TABLE t_fixed_key_ts;
DROP TABLE t_fixed_key_right;
DROP TABLE t_fixed_key_final;
