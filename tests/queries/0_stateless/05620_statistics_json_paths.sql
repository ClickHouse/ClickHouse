-- Statistics declared on a `JSON` column are built for every path: typed, dynamic and in shared data.
-- Reproducer of https://github.com/ClickHouse/ClickHouse/issues/122437 (scaled down), where every filter is on a JSON path.

SET explain_query_plan_default = 'legacy';
SET enable_parallel_replicas = 0;
SET use_statistics = 1;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_algorithm = 'greedy';
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'auto';
SET use_hash_table_stats_for_join_reordering = 0;
SET join_algorithm = 'hash';

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t1 (r String, j JSON(k Array(String), sd Bool, ui Bool, rs Array(String), `a.t` String, `a.n` String, `a.ty` String, tid String) MATERIALIZED r STATISTICS(uniq_v2), Id String, INDEX i1 j.k TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY (j.sd, j.ui, j.a.ty, j.tid, Id) SETTINGS auto_statistics_types = 'minmax, uniq';
CREATE TABLE t2 (r String, j JSON(dn String, f1 Nullable(DateTime64(3)), f2 Nullable(DateTime64(3)), s Array(String), tid String, k Array(String)) MATERIALIZED r STATISTICS(uniq_v2), Id String, INDEX i2 j.k TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY (j.dn, Id) SETTINGS auto_statistics_types = 'minmax, uniq';
CREATE TABLE t3 (r String, j JSON(k Array(String)) MATERIALIZED r STATISTICS(uniq_v2), Id String, INDEX i3 j.k TYPE set(100) GRANULARITY 1)
ENGINE = ReplacingMergeTree ORDER BY Id SETTINGS auto_statistics_types = 'minmax, uniq';

INSERT INTO t1 (r, Id) SELECT concat('{"k":', if(number % 20 = 0, '["A","B"]', '["B"]'), ',"sd":false,"ui":true,"rs":["s1"],"a":{"t":"x', toString(number % 7), '","n":"n', toString(number % 1000), '","ty":"', if(number % 20 = 0, 'y0', 'y1'), '"},"tid":"z"}'), concat('e', toString(number)) FROM numbers(3000);
INSERT INTO t2 (r, Id) SELECT concat('{"eid":"e', toString((number % 30) * 20), '","k":["C"],"s":["s1"],"st":"o","vid":"v', toString(number % 1500), '","f1":"2024-01-01 00:00:00","rv":["1.', toString(number % 9), '"],"f2":null,"dn":"d', toString(number % 50), '","tid":"z"}'), concat('p', toString(number)) FROM numbers(30000);
INSERT INTO t3 (r, Id) SELECT concat('{"k":["D"],"w":{"sev":"', ['l','m','h','c'][number % 4 + 1], '","s2":', toString((number % 100) / 10), ',"s3":', toString((number % 90) / 10), '},"sv":"h"}'), concat('v', toString(number)) FROM numbers(1500);

-- Statistics are materialized explicitly, whatever `materialize_statistics_on_insert` is.
ALTER TABLE t1 MATERIALIZE STATISTICS ALL SETTINGS mutations_sync = 2;
ALTER TABLE t2 MATERIALIZE STATISTICS ALL SETTINGS mutations_sync = 2;
ALTER TABLE t3 MATERIALIZE STATISTICS ALL SETTINGS mutations_sync = 2;

-- The row counts after `~` are dropped: they depend on the index granularity.
SELECT replaceRegexpAll(trimLeft(explain), '~[0-9?]+\\]', ']') FROM
(
    EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT x0.Id, x1.Id, x2.Id, x3.Id
    FROM t1 AS x0
    INNER JOIN t2 AS x1 ON (CAST(x1.j.eid, 'Nullable(String)') = x0.Id) AND has(CAST(x1.j.k, 'Array(String)'), 'C') AND hasAny(x1.j.s, ['s0', 'all', 's1']) AND (x1.j.st = 'o')
    INNER JOIN t3 AS x2 ON (CAST(x1.j.vid, 'Nullable(String)') = x2.Id) AND has(CAST(x2.j.k, 'Array(String)'), 'D') AND has(['m', 'h', 'c'], x2.j.w.sev)
    INNER JOIN t1 AS x3 ON (CAST(x1.j.eid, 'Nullable(String)') = x3.Id) AND has(CAST(x3.j.k, 'Array(String)'), 'B') AND (x3.j.sd = false) AND (x3.j.ui = true) AND hasAny(x3.j.rs, ['s0', 'all', 's1'])
    WHERE has(CAST(x0.j.k, 'Array(String)'), 'A') AND (x0.j.sd = false) AND (x0.j.ui = true) AND hasAny(x0.j.rs, ['s0', 's1', 'all'])
)
WHERE explain LIKE '%Join:%';

SELECT count()
FROM t1 AS x0
INNER JOIN t2 AS x1 ON (CAST(x1.j.eid, 'Nullable(String)') = x0.Id) AND has(CAST(x1.j.k, 'Array(String)'), 'C') AND hasAny(x1.j.s, ['s0', 'all', 's1']) AND (x1.j.st = 'o')
INNER JOIN t3 AS x2 ON (CAST(x1.j.vid, 'Nullable(String)') = x2.Id) AND has(CAST(x2.j.k, 'Array(String)'), 'D') AND has(['m', 'h', 'c'], x2.j.w.sev)
INNER JOIN t1 AS x3 ON (CAST(x1.j.eid, 'Nullable(String)') = x3.Id) AND has(CAST(x3.j.k, 'Array(String)'), 'B') AND (x3.j.sd = false) AND (x3.j.ui = true) AND hasAny(x3.j.rs, ['s0', 'all', 's1'])
WHERE has(CAST(x0.j.k, 'Array(String)'), 'A') AND (x0.j.sd = false) AND (x0.j.ui = true) AND hasAny(x0.j.rs, ['s0', 's1', 'all']);

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;

-- A path in shared data, and a dynamic path that one part does not have, before and after a merge.
-- `j.p = 'x'` holds in 500 rows of `ts` and of the first part of `tm`; the second part of `tm` has no `p`.
CREATE TABLE ts (k UInt64, j JSON(max_dynamic_paths = 0) STATISTICS(uniq_v2)) ENGINE = MergeTree ORDER BY k SETTINGS materialize_statistics_on_merge = 1;
CREATE TABLE tm (k UInt64 STATISTICS(basic), j JSON STATISTICS(uniq_v2)) ENGINE = MergeTree ORDER BY k SETTINGS materialize_statistics_on_merge = 1, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
CREATE TABLE u (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO u SELECT number FROM numbers(100) SETTINGS materialize_statistics_on_insert = 1;
-- Statistics are built on insert here: stopped merges would block the mutation of `MATERIALIZE STATISTICS`.
INSERT INTO ts SELECT number, if(number % 2 = 0, '{"p":"x"}', '{"q":"y"}') FROM numbers(1000) SETTINGS materialize_statistics_on_insert = 1;
SYSTEM STOP MERGES tm;
INSERT INTO tm SELECT number, if(number % 2 = 0, '{"p":"x"}', '{"q":"y"}') FROM numbers(1000) SETTINGS materialize_statistics_on_insert = 1;
INSERT INTO tm SELECT number, '{"q":"y"}' FROM numbers(9000) SETTINGS materialize_statistics_on_insert = 1;

SELECT replaceRegexpAll(trimLeft(explain), '~[0-9?]+\\]', ']') FROM (EXPLAIN keep_logical_steps = 1, actions = 1 SELECT 1 FROM ts AS a INNER JOIN u AS b ON a.k = b.k WHERE a.j.p = 'x') WHERE explain LIKE '%Join:%';
SELECT replaceRegexpAll(trimLeft(explain), '~[0-9?]+\\]', ']') FROM (EXPLAIN keep_logical_steps = 1, actions = 1 SELECT 1 FROM tm AS a INNER JOIN u AS b ON a.k = b.k WHERE a.j.p = 'x') WHERE explain LIKE '%Join:%';
SYSTEM START MERGES tm;
OPTIMIZE TABLE tm FINAL;
SELECT replaceRegexpAll(trimLeft(explain), '~[0-9?]+\\]', ']') FROM (EXPLAIN keep_logical_steps = 1, actions = 1 SELECT 1 FROM tm AS a INNER JOIN u AS b ON a.k = b.k WHERE a.j.p = 'x') WHERE explain LIKE '%Join:%';
-- A mutation of another column of a wide part writes the statistics of `j` as they were read.
ALTER TABLE tm MATERIALIZE STATISTICS k SETTINGS mutations_sync = 2;
SELECT replaceRegexpAll(trimLeft(explain), '~[0-9?]+\\]', ']') FROM (EXPLAIN keep_logical_steps = 1, actions = 1 SELECT 1 FROM tm AS a INNER JOIN u AS b ON a.k = b.k WHERE a.j.p = 'x') WHERE explain LIKE '%Join:%';

-- Literals of two types on one `Dynamic` path.
CREATE TABLE td (k UInt64, j JSON STATISTICS(uniq_v2)) ENGINE = MergeTree ORDER BY k;
INSERT INTO td SELECT number, concat('{"d":"2024-01-', leftPad(toString(number % 28 + 1), 2, '0'), ' 10:00:00.000"}') FROM numbers(1000) SETTINGS materialize_statistics_on_insert = 1;
SELECT dynamicType(j.d) FROM td LIMIT 1;
SELECT sum(k) FROM td WHERE j.d >= toDateTime64('2024-01-01 00:00:00', 3) AND j.d < '2024-01-15 00:00:00' SETTINGS optimize_move_to_prewhere = 1, min_marks_per_part_to_load_statistics = 0;
SELECT count() FROM td AS a INNER JOIN u AS b ON a.k = b.k WHERE a.j.d >= toDateTime64('2024-01-01 00:00:00', 3) AND a.j.d < '2024-01-15 00:00:00' SETTINGS min_marks_per_part_to_load_statistics = 0;
SELECT count() FROM td AS a INNER JOIN u AS b ON a.k = b.k WHERE a.j.d = toDateTime64('2024-01-15 10:00:00', 3) OR a.j.d = '2024-01-16 10:00:00' SETTINGS min_marks_per_part_to_load_statistics = 0;
DROP TABLE td;

-- `uniq` on a `Dynamic` column does not count `NULL` as a value.
CREATE TABLE tdn (k UInt64, d Dynamic STATISTICS(basic, uniq)) ENGINE = MergeTree ORDER BY k;
INSERT INTO tdn SELECT number, if(number % 2 = 0, 'x', NULL) FROM numbers(1000) SETTINGS materialize_statistics_on_insert = 1;
SELECT replaceRegexpAll(trimLeft(explain), '~[0-9?]+\\]', ']') FROM (EXPLAIN keep_logical_steps = 1, actions = 1 SELECT 1 FROM tdn AS a INNER JOIN u AS b ON a.k = b.k WHERE a.d = 'x') WHERE explain LIKE '%Join:%';
DROP TABLE tdn;

-- `IS NULL` on a typed path, rewritten to the subcolumn `j.n.null`.
CREATE TABLE tn (k UInt64, j JSON(n Nullable(UInt64)) STATISTICS(uniq_v2)) ENGINE = MergeTree ORDER BY k;
INSERT INTO tn SELECT number, if(number % 4 = 0, '{"n":1}', '{}') FROM numbers(1000) SETTINGS materialize_statistics_on_insert = 1;
SET optimize_functions_to_subcolumns = 1;
SELECT replaceRegexpAll(trimLeft(explain), '~[0-9?]+\\]', ']') FROM (EXPLAIN keep_logical_steps = 1, actions = 1 SELECT 1 FROM tn AS a INNER JOIN u AS b ON a.k = b.k WHERE a.j.n IS NULL) WHERE explain LIKE '%Join:%';
SELECT replaceRegexpAll(trimLeft(explain), '~[0-9?]+\\]', ']') FROM (EXPLAIN keep_logical_steps = 1, actions = 1 SELECT 1 FROM tn AS a INNER JOIN u AS b ON a.k = b.k WHERE a.j.n IS NOT NULL) WHERE explain LIKE '%Join:%';
DROP TABLE tn;

DROP TABLE ts;
DROP TABLE tm;
DROP TABLE u;
