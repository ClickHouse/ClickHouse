-- A JOIN runtime filter computes its key on rows that are removed before its join, so it is not built on a key that can throw.

SET enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, query_plan_optimize_join_order_randomize = 0;

-- No query has read these tables yet, so PREWHERE orders the two filters by column size.
CREATE TABLE probe (a UInt64, b UInt8) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO probe SELECT number % 1000 AS a, if(a >= 500, 0, 1) FROM numbers(100000);
CREATE TABLE r1 (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO r1 SELECT number FROM numbers(500);
CREATE TABLE r2 (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO r2 VALUES (100000);
SELECT count() FROM probe JOIN r1 ON probe.a = r1.k JOIN r2 ON intDiv(100000, probe.b) = r2.k SETTINGS query_plan_optimize_join_order_limit = 0;

CREATE TABLE p (id UInt64, id32 UInt32, b UInt8, s String, i8 Int8, d Decimal64(2)) ENGINE = MergeTree ORDER BY id;
INSERT INTO p SELECT number, number, number % 2 AS b, if(b = 0, 'bad', '1.2.3.4'), if(b = 0, -128, 1), if(b = 0, 60000000000000000, 1) FROM numbers(100000);
CREATE TABLE g (id UInt64, flag UInt8, ok Bool) ENGINE = MergeTree ORDER BY id;
INSERT INTO g SELECT number, number % 2, number % 2 FROM numbers(100000);
CREATE TABLE r (k Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO r VALUES (100), (-1), (16909060), (1);

-- The filter between the joins removes the rows on which the key throws.
SELECT count() FROM p JOIN g ON p.id = g.id JOIN r ON intDiv(100, p.b) = r.k WHERE p.b + g.flag = 2;
SELECT count() FROM p JOIN g ON p.id = g.id JOIN r ON intDiv(p.i8, -1) = r.k WHERE p.b + g.flag = 2;
SELECT count() FROM p JOIN g ON p.id = g.id JOIN r ON IPv4StringToNum(p.s) = r.k WHERE p.b + g.flag = 2;
SELECT count() FROM p JOIN g ON p.id = g.id JOIN r ON toInt64(toBool(if(p.b = 1, 'true', p.s))) = r.k WHERE p.b + g.flag = 2;
SELECT count() FROM p JOIN g ON p.id = g.id JOIN r ON p.d * 100 = r.k WHERE p.b + g.flag = 2;
SELECT count() FROM p JOIN g ON p.id = g.id JOIN r ON IPv4StringToNum(p.s) = r.k WHERE p.b + g.flag = 2 SETTINGS query_plan_merge_filters = 0;
SELECT count() FROM p LEFT JOIN g ON p.id = g.id JOIN r ON intDiv(100, p.b) = r.k WHERE p.b + g.flag = 2;
SELECT count() FROM p LEFT JOIN g ON p.id = g.id JOIN r ON IPv4StringToNum(p.s) = r.k WHERE g.ok SETTINGS query_plan_convert_outer_join_to_inner_join = 0;

CREATE TABLE ra (k UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO ra VALUES (16909060);
SELECT count() FROM p JOIN g ON p.id = g.id LEFT ANTI JOIN ra ON IPv4StringToNum(nullIf(p.s, if(p.id % 4 = 1, '1.2.3.4', ''))) = ra.k WHERE p.b + g.flag = 2;

-- The filter of the lower join passes 80 % of the rows, so it switches itself off.
CREATE TABLE w (k UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO w SELECT number FROM numbers(800);
CREATE TABLE d (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO d VALUES (100000);
CREATE TABLE mt (a UInt32, c UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO mt SELECT number % 1000 AS a, a < 800 FROM numbers(200000);
SELECT count() FROM mt JOIN w ON mt.a = w.k JOIN d ON intDiv(100000, mt.c) = d.k SETTINGS query_plan_optimize_join_order_limit = 0;

-- The key reads the probe side, then the build side, of the lower join.
CREATE TABLE gb (id UInt64, flag UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO gb SELECT number, number < 100000 FROM numbers(200000);
SELECT count() FROM gb JOIN p ON gb.id = p.id JOIN r ON intDiv(100, gb.flag) = r.k SETTINGS query_plan_optimize_join_order_limit = 0;
SELECT count() FROM p JOIN gb ON p.id = gb.id JOIN r ON intDiv(100, gb.flag) = r.k SETTINGS query_plan_optimize_join_order_limit = 0;

-- ARRAY JOIN removes the rows with an empty array.
CREATE TABLE t (x UInt8, arr Array(UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t SELECT number % 2 AS x, if(x = 0, [], [1, 2]) FROM numbers(100000);
SELECT count() FROM (SELECT x, a FROM t ARRAY JOIN arr AS a) AS q JOIN r ON intDiv(100, q.x) = r.k;
-- The same, read through a `Merge` table over a view.
CREATE VIEW v_aj AS SELECT x, a FROM t ARRAY JOIN arr AS a;
CREATE TABLE m_aj (x UInt8, a UInt8) ENGINE = Merge(currentDatabase(), '^v_aj$');
SELECT count() FROM m_aj JOIN r ON intDiv(100, m_aj.x) = r.k;

-- A single join with a WHERE below it.
CREATE TABLE p2 (id UInt64, b UInt8, s String) ENGINE = MergeTree ORDER BY id;
INSERT INTO p2 SELECT number, number % 10 != 0 AS b, if(b = 0, 'bad', '1.2.3.4') FROM numbers(100000);
CREATE TABLE r32 (k UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO r32 VALUES (16909060);
SELECT count() FROM p2 JOIN r32 ON IPv4StringToNum(p2.s) = r32.k WHERE p2.b = 1 SETTINGS query_plan_merge_filters = 0;
SELECT count() FROM p2 JOIN r32 ON IPv4StringToNum(p2.s) = r32.k WHERE p2.b SETTINGS optimize_move_to_prewhere = 0;

-- A key that cannot throw still reaches the scan below the lower join (`Runtime filters:` of its PREWHERE).
CREATE TABLE r64 (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO r64 VALUES (5);
SELECT countIf(explain LIKE '%Runtime filters:%_CAST(id32%') FROM (EXPLAIN SELECT count() FROM p JOIN g ON p.id = g.id JOIN r64 ON p.id32 = r64.k WHERE p.b + g.flag = 2 SETTINGS optimize_move_to_prewhere = 1, enable_parallel_replicas = 0);

-- The key-unification cast of a `UInt32 = Int64` key cannot throw, so the key still gets its runtime filter.
SELECT countIf(explain LIKE '%Runtime filters:%_CAST(id32%') FROM (EXPLAIN SELECT count() FROM p JOIN g ON p.id = g.id JOIN r ON p.id32 = r.k WHERE p.b + g.flag = 2 SETTINGS optimize_move_to_prewhere = 1, enable_parallel_replicas = 0, query_plan_optimize_join_order_limit = 0, query_plan_join_swap_table = 'false');

-- A cast that can throw: `Date` to `DateTime` under `date_time_overflow_behavior = 'throw'`.
CREATE TABLE pd (id UInt64, b UInt8, dt Date) ENGINE = MergeTree ORDER BY id;
INSERT INTO pd SELECT number, number % 2 AS b, if(b = 0, toDate('2149-06-06'), toDate('2020-01-01')) FROM numbers(100000);
CREATE TABLE rdt (k DateTime('UTC')) ENGINE = MergeTree ORDER BY k;
INSERT INTO rdt VALUES ('2020-01-01 00:00:00');
SELECT count() FROM pd JOIN g ON pd.id = g.id JOIN rdt ON CAST(pd.dt AS DateTime('UTC')) = rdt.k WHERE pd.b + g.flag = 2 SETTINGS date_time_overflow_behavior = 'throw';

-- Two keys: the one that cannot throw keeps its runtime filter.
CREATE TABLE r2k (a UInt64, k Int64) ENGINE = MergeTree ORDER BY a;
INSERT INTO r2k VALUES (1, 100), (2, 100), (3, 100);
SELECT count() FROM p JOIN g ON p.id = g.id JOIN r2k ON p.id = r2k.a AND intDiv(100, p.b) = r2k.k WHERE p.b + g.flag = 2;
SELECT countIf(explain LIKE '%Build runtime join filter on%') FROM (EXPLAIN SELECT count() FROM p JOIN g ON p.id = g.id JOIN r2k ON p.id = r2k.a AND intDiv(100, p.b) = r2k.k WHERE p.b + g.flag = 2 SETTINGS enable_parallel_replicas = 0, query_plan_optimize_join_order_limit = 0, query_plan_join_swap_table = 'false');

-- LEFT ANTI JOIN builds one filter on all keys, so a key that can throw leaves it without one.
CREATE TABLE ra2 (a UInt64, k UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO ra2 VALUES (1, 16909060), (3, 0);
SELECT count() FROM p JOIN g ON p.id = g.id LEFT ANTI JOIN ra2 ON p.id = ra2.a AND IPv4StringToNum(p.s) = ra2.k WHERE p.b + g.flag = 2;

-- A conversion function to an integer type of a `String` parses it, so it can throw.
CREATE TABLE pn (id UInt64, b UInt8, n String) ENGINE = MergeTree ORDER BY id;
INSERT INTO pn SELECT number, number % 2 AS b, if(b = 0, 'bad', '100') FROM numbers(100000);
SELECT count() FROM pn JOIN g ON pn.id = g.id JOIN r ON toInt64(pn.n) = r.k WHERE pn.b + g.flag = 2;

-- The cast that unifies the key types can throw too.
CREATE TABLE pdec (id UInt64, b UInt8, d Decimal64(0)) ENGINE = MergeTree ORDER BY id;
INSERT INTO pdec SELECT number, number % 2 AS b, if(b = 0, 900000000000000000, 1) FROM numbers(100000);
CREATE TABLE rdec (k Decimal64(9)) ENGINE = MergeTree ORDER BY k;
INSERT INTO rdec VALUES (1);
SELECT count() FROM pdec JOIN g ON pdec.id = g.id JOIN rdec ON pdec.d = rdec.k WHERE pdec.b + g.flag = 2;

-- The key is a column that a subquery computes.
SELECT count() FROM (SELECT p.b AS b, intDiv(100, p.b) AS k FROM p JOIN g ON p.id = g.id WHERE p.b + g.flag = 2) AS x JOIN r ON x.k = r.k;
SELECT count() FROM (SELECT mt.c AS c, intDiv(100000, mt.c) AS k FROM mt JOIN w ON mt.a = w.k) AS x JOIN d ON x.k = d.k SETTINGS query_plan_optimize_join_order_limit = 0;
CREATE TABLE probe3 (a UInt64, b UInt8) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO probe3 SELECT number % 1000 AS a, if(a >= 500, 0, 1) FROM numbers(100000);
CREATE TABLE r13 (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO r13 SELECT number FROM numbers(500);
CREATE TABLE r23 (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO r23 VALUES (100000);
SELECT count() FROM (SELECT probe3.b AS b, intDiv(100000, probe3.b) AS k FROM probe3 JOIN r13 ON probe3.a = r13.k) AS x JOIN r23 ON x.k = r23.k SETTINGS query_plan_optimize_join_order_limit = 0;

-- The key is a column that a view computes.
CREATE VIEW v_k AS SELECT p.b AS b, intDiv(100, p.b) AS k FROM p JOIN g ON p.id = g.id WHERE p.b + g.flag = 2;
SELECT count() FROM v_k JOIN r ON v_k.k = r.k;

-- The cast of an `Int32` key to `Float64` cannot throw, so the key keeps its runtime filter.
CREATE TABLE fi (k Int32) ENGINE = MergeTree ORDER BY k;
INSERT INTO fi SELECT number FROM numbers(10);
CREATE TABLE ff (k Float64) ENGINE = MergeTree ORDER BY k;
INSERT INTO ff SELECT number FROM numbers(10);
SELECT countIf(explain LIKE '%Build runtime join filter on%') FROM (EXPLAIN SELECT count() FROM fi JOIN ff ON fi.k = ff.k SETTINGS enable_parallel_replicas = 0, query_plan_optimize_join_order_limit = 0, query_plan_join_swap_table = 'false');
