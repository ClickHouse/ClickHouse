-- A JOIN runtime filter whose key can throw is not pushed below, or merged into, a step that removes rows before its join.

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

-- Nothing removes rows below the join, so the filter still reaches the scan.
SELECT countIf(explain LIKE '%Runtime filters:%intDiv%') FROM (EXPLAIN SELECT sum(p.id) FROM p JOIN r ON intDiv(100, p.b) = r.k SETTINGS optimize_move_to_prewhere = 1, enable_parallel_replicas = 0, query_plan_optimize_join_order_limit = 0, query_plan_join_swap_table = 'false');
-- The filter is built but stays above the filter between the joins.
SELECT countIf(explain LIKE '%Build runtime join filter on%'), countIf(explain LIKE '%Runtime filters:%intDiv%') FROM (EXPLAIN SELECT count() FROM p JOIN g ON p.id = g.id JOIN r ON intDiv(100, p.b) = r.k WHERE p.b + g.flag = 2 SETTINGS optimize_move_to_prewhere = 1, enable_parallel_replicas = 0, query_plan_optimize_join_order_limit = 0, query_plan_join_swap_table = 'false');

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
-- LEFT ARRAY JOIN with a condition on the elements removes rows too.
SELECT count() FROM (SELECT x, a FROM t LEFT ARRAY JOIN arr AS a WHERE a = 1) AS q JOIN r ON intDiv(100, q.x) = r.k;
SELECT count() FROM (SELECT x, a FROM t LEFT ARRAY JOIN arr AS a WHERE a = 1) AS q JOIN r ON intDiv(100, q.x) = r.k SETTINGS query_plan_fuse_filter_into_array_join = 0;
-- The same with the `arrayJoin` function, also when it stays in an expression.
SELECT count() FROM (SELECT x, arrayJoin(arr) AS a FROM t) AS q JOIN r ON intDiv(100, q.x) = r.k;
SELECT count() FROM (SELECT x, arrayJoin(arr) AS a FROM t) AS q JOIN r ON intDiv(100, q.x) = r.k SETTINGS query_plan_lower_array_join_function = 0;
-- There the filter stays in a Filter step above it; a key that cannot throw still reaches PREWHERE.
SELECT countIf(explain LIKE '%Filter column: __applyFilter%'), countIf(explain LIKE '%Prewhere filter column: __applyFilter%') FROM (EXPLAIN actions = 1, pretty = 0 SELECT count() FROM (SELECT x, arrayJoin(arr) AS a FROM t) AS q JOIN r ON intDiv(100, q.x) = r.k SETTINGS query_plan_lower_array_join_function = 0, optimize_move_to_prewhere = 1, enable_parallel_replicas = 0, query_plan_join_swap_table = 'false');
SELECT countIf(explain LIKE '%Filter column: __applyFilter%'), countIf(explain LIKE '%Prewhere filter column: __applyFilter%') FROM (EXPLAIN actions = 1, pretty = 0 SELECT count() FROM (SELECT x, arrayJoin(arr) AS a FROM t) AS q JOIN r ON q.x = r.k SETTINGS query_plan_lower_array_join_function = 0, optimize_move_to_prewhere = 1, enable_parallel_replicas = 0, query_plan_join_swap_table = 'false');

-- A single join with a WHERE below it.
CREATE TABLE p2 (id UInt64, b UInt8, s String) ENGINE = MergeTree ORDER BY id;
INSERT INTO p2 SELECT number, number % 10 != 0 AS b, if(b = 0, 'bad', '1.2.3.4') FROM numbers(100000);
CREATE TABLE r32 (k UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO r32 VALUES (16909060);
SELECT count() FROM p2 JOIN r32 ON IPv4StringToNum(p2.s) = r32.k WHERE p2.b = 1 SETTINGS query_plan_merge_filters = 0;
SELECT count() FROM p2 JOIN r32 ON IPv4StringToNum(p2.s) = r32.k WHERE p2.b SETTINGS optimize_move_to_prewhere = 0;
-- A HAVING below the join removes groups.
SELECT count() FROM (SELECT b FROM p GROUP BY b HAVING min(id) = 1) AS q JOIN r ON intDiv(100, q.b) = r.k SETTINGS query_plan_join_swap_table = 0;

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

-- Two keys: the filter of the one that cannot throw stays above the filter between the joins with the other one.
CREATE TABLE r2k (a UInt64, k Int64) ENGINE = MergeTree ORDER BY a;
INSERT INTO r2k VALUES (1, 100), (2, 100), (3, 100);
SELECT count() FROM p JOIN g ON p.id = g.id JOIN r2k ON p.id = r2k.a AND intDiv(100, p.b) = r2k.k WHERE p.b + g.flag = 2;
SELECT countIf(explain LIKE '%Runtime filters:%(id, a from%') FROM (EXPLAIN SELECT count() FROM p JOIN g ON p.id = g.id JOIN r2k ON p.id = r2k.a AND intDiv(100, p.b) = r2k.k WHERE p.b + g.flag = 2 SETTINGS optimize_move_to_prewhere = 1, enable_parallel_replicas = 0, query_plan_optimize_join_order_limit = 0, query_plan_join_swap_table = 'false');

-- A LEFT ANTI JOIN filter on two keys, one of which can throw.
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

-- `CAST` pairs the elements of named tuples by name, so this key parses a `String`.
CREATE TABLE pt (id UInt64, b UInt8, t Tuple(a UInt8, b String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO pt SELECT number, number % 2 AS b, (1, if(b = 0, 'bad', '5')) FROM numbers(100000);
CREATE TABLE rt (k Tuple(b UInt8, a String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO rt VALUES ((5, '1'));
SELECT count() FROM pt JOIN g ON pt.id = g.id JOIN rt ON CAST(pt.t, 'Tuple(b UInt8, a String)') = rt.k WHERE pt.b + g.flag = 2;

-- A `String` vs `FixedString` tuple key is cast to the common type, which can throw on its other elements.
CREATE TABLE ptup (id UInt64, b UInt8, t Tuple(String, Decimal64(0))) ENGINE = MergeTree ORDER BY id;
INSERT INTO ptup SELECT number, number % 2 AS b, tuple(if(b = 0, 'bad', 'ok'), if(b = 0, 900000000000000000, 1)) FROM numbers(100000);
CREATE TABLE rtup (k Tuple(FixedString(3), Decimal64(9))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO rtup VALUES (('ok', 1));
SELECT count() FROM ptup JOIN g ON ptup.id = g.id JOIN rtup ON ptup.t = rtup.k WHERE ptup.b + g.flag = 2;


-- An `Enum` converted to an integer keeps its value or wraps around, so the key keeps its runtime filter.
CREATE TABLE pe (id UInt64, e Enum8('a' = 1, 'b' = 2)) ENGINE = MergeTree ORDER BY id;
INSERT INTO pe SELECT number, if(number % 2 = 0, 'a', 'b') FROM numbers(10);
CREATE TABLE ri16 (k Int16) ENGINE = MergeTree ORDER BY k;
INSERT INTO ri16 VALUES (1);
SELECT countIf(explain LIKE '%Runtime filters:%_CAST(e, %Int16%') FROM (EXPLAIN SELECT count() FROM pe JOIN g ON pe.id = g.id JOIN ri16 ON pe.e = ri16.k SETTINGS optimize_move_to_prewhere = 1, enable_parallel_replicas = 0, query_plan_optimize_join_order_limit = 0, query_plan_join_swap_table = 'false');

-- `CAST` converts the values of a `Map` as such, so a named tuple in it is paired by name and this key parses a `String`.
CREATE TABLE pm (id UInt64, b UInt8, m Map(String, Tuple(a UInt8, b String))) ENGINE = MergeTree ORDER BY id;
INSERT INTO pm SELECT number, number % 2 AS b, map('x', (1, if(b = 0, 'bad', '5'))) FROM numbers(100000);
CREATE TABLE rm (k Map(String, Tuple(b UInt8, a String))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO rm VALUES (map('x', (5, '1')));
SELECT count() FROM pm JOIN g ON pm.id = g.id JOIN rm ON CAST(pm.m, 'Map(String, Tuple(b UInt8, a String))') = rm.k WHERE pm.b + g.flag = 2;
CREATE TABLE rm16 (k Map(String, Tuple(a UInt16, b String))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO rm16 VALUES (map('x', (1, '5')));
SELECT countIf(explain LIKE '%Runtime filters:%CAST(m, %') FROM (EXPLAIN SELECT count() FROM pm JOIN g ON pm.id = g.id JOIN rm16 ON CAST(pm.m, 'Map(String, Tuple(a UInt16, b String))') = rm16.k SETTINGS optimize_move_to_prewhere = 1, enable_parallel_replicas = 0, query_plan_optimize_join_order_limit = 0, query_plan_join_swap_table = 'false');

-- `CAST` converts the elements of a `QBit` as numbers, so a `nan` element cannot become an `Int8`.
CREATE TABLE pq (id UInt64, b UInt8, q QBit(Float32, 1)) ENGINE = MergeTree ORDER BY id;
INSERT INTO pq SELECT number, number % 2 AS b, [if(b = 0, nan, 1)] FROM numbers(100000);
CREATE TABLE rq (k QBit(Int8, 1)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO rq VALUES ([1]);
SELECT count() FROM pq JOIN g ON pq.id = g.id JOIN rq ON CAST(pq.q, 'QBit(Int8, 1)') = rq.k WHERE pq.b + g.flag = 2;
CREATE TABLE rq64 (k QBit(Float64, 1)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO rq64 VALUES ([1]);
SELECT countIf(explain LIKE '%Runtime filters:%CAST(q, %') FROM (EXPLAIN SELECT count() FROM pq JOIN g ON pq.id = g.id JOIN rq64 ON CAST(pq.q, 'QBit(Float64, 1)') = rq64.k SETTINGS optimize_move_to_prewhere = 1, enable_parallel_replicas = 0, query_plan_optimize_join_order_limit = 0, query_plan_join_swap_table = 'false');

-- The cast of an `Int32` key to `Float64` cannot throw, so the key keeps its runtime filter.
CREATE TABLE fi (id UInt64, k Int32) ENGINE = MergeTree ORDER BY k;
INSERT INTO fi SELECT number, number FROM numbers(10);
CREATE TABLE ff (k Float64) ENGINE = MergeTree ORDER BY k;
INSERT INTO ff SELECT number FROM numbers(10);
SELECT countIf(explain LIKE '%Runtime filters:%_CAST(k, %Float64%') FROM (EXPLAIN SELECT count() FROM fi JOIN g ON fi.id = g.id JOIN ff ON fi.k = ff.k SETTINGS optimize_move_to_prewhere = 1, enable_parallel_replicas = 0, query_plan_optimize_join_order_limit = 0, query_plan_join_swap_table = 'false');

-- A conversion to a float only rounds, also of an `Enum` or a `Decimal`, so these keys keep their runtime filter.
SELECT countIf(explain LIKE '%Runtime filters:%_CAST(e, %Float64%') FROM (EXPLAIN SELECT count() FROM pe JOIN g ON pe.id = g.id JOIN ff ON pe.e = ff.k SETTINGS optimize_move_to_prewhere = 1, enable_parallel_replicas = 0, query_plan_optimize_join_order_limit = 0, query_plan_join_swap_table = 'false');
SELECT countIf(explain LIKE '%Runtime filters:%toFloat64(k)%') FROM (EXPLAIN SELECT count() FROM fi JOIN g ON fi.id = g.id JOIN ff ON toFloat64(fi.k) = ff.k SETTINGS optimize_move_to_prewhere = 1, enable_parallel_replicas = 0, query_plan_optimize_join_order_limit = 0, query_plan_join_swap_table = 'false');
SELECT countIf(explain LIKE '%Runtime filters:%toFloat64(d)%') FROM (EXPLAIN SELECT count() FROM pdec JOIN g ON pdec.id = g.id JOIN ff ON toFloat64(pdec.d) = ff.k SETTINGS optimize_move_to_prewhere = 1, enable_parallel_replicas = 0, query_plan_optimize_join_order_limit = 0, query_plan_join_swap_table = 'false');

-- A conversion function to a float of a `String` parses it, so it can throw.
SELECT count() FROM pn JOIN g ON pn.id = g.id JOIN r ON toFloat64(pn.n) = toFloat64(r.k) WHERE pn.b + g.flag = 2;

-- An explicit PREWHERE, evaluated in one step with the filter moved into it.
SELECT count(), sum(q.id) FROM (SELECT id, s FROM p2 PREWHERE b = 1) AS q JOIN r32 ON IPv4StringToNum(q.s) = r32.k SETTINGS optimize_prewhere_after_pushdown = 1, enable_multiple_prewhere_read_steps = 0, optimize_move_to_prewhere = 1;

-- FINAL removes the rows of a collapsed group.
CREATE TABLE fc (k Int32, v UInt64, sign Int8) ENGINE = CollapsingMergeTree(sign) ORDER BY k;
SYSTEM STOP MERGES fc;
INSERT INTO fc VALUES (1, 5, 1), (0, 6, 1);
INSERT INTO fc VALUES (0, 6, -1);
CREATE TABLE rk (k Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO rk VALUES (100);
SELECT sum(fc.v) FROM fc FINAL JOIN rk ON intDiv(100, fc.k) = rk.k SETTINGS optimize_move_to_prewhere = 1, optimize_move_to_prewhere_if_final = 1;

-- SAMPLE removes its rows after PREWHERE.
CREATE TABLE ps (id UInt64, b UInt8) ENGINE = MergeTree ORDER BY intHash32(id) SAMPLE BY intHash32(id);
INSERT INTO ps SELECT number, intHash32(number) < 2147483648 FROM numbers(100000);
SELECT count(), sum(ps.id) FROM ps SAMPLE 1/2 JOIN rk ON intDiv(100, ps.b) = rk.k SETTINGS optimize_move_to_prewhere = 1;

-- Lazy FINAL builds its set of keys from a read without FINAL.
CREATE TABLE rl (k UInt64, b UInt8) ENGINE = ReplacingMergeTree ORDER BY k;
SYSTEM STOP MERGES rl;
INSERT INTO rl SELECT number, 0 FROM numbers(100000);
INSERT INTO rl SELECT number, 1 FROM numbers(100000);
SELECT count() FROM rl FINAL JOIN rk ON intDiv(100, rl.b) = rk.k SETTINGS query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0;

-- A projection runs the filters between it and the join as one PREWHERE.
CREATE TABLE pj (id UInt64, b UInt8, c UInt8, s String, PROJECTION pc (SELECT * ORDER BY c)) ENGINE = MergeTree ORDER BY id;
INSERT INTO pj SELECT number, number % 20 = 0 AS b, number % 10 = 0 AS c, if(b, '1.2.3.4', 'bad') FROM numbers(100000);
SELECT count(), sum(pj.id) FROM pj JOIN r ON intDiv(100, pj.b) = r.k WHERE pj.c = 1 AND pj.b = 1 SETTINGS optimize_move_to_prewhere = 1, optimize_use_projections = 1, query_plan_join_swap_table = 'false', enable_multiple_prewhere_read_steps = 0;
SELECT count(), sum(pj.id) FROM pj JOIN r32 ON IPv4StringToNum(pj.s) = r32.k WHERE pj.c = 1 AND pj.b = 1 SETTINGS optimize_move_to_prewhere = 1, optimize_use_projections = 1, query_plan_join_swap_table = 'false', enable_multiple_prewhere_read_steps = 0;
