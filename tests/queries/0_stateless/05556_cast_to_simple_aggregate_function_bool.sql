-- A CAST into SimpleAggregateFunction(f, Bool) maps every nonzero value to 1, as a CAST into Bool does.

SELECT 'scalar';
SELECT CAST(toUInt8(2) AS SimpleAggregateFunction(any, Bool)) AS x, toUInt8(x), x = true;
SELECT CAST(300 AS SimpleAggregateFunction(any, Bool)) AS x, toUInt8(x), x = true;
SELECT CAST(-1 AS SimpleAggregateFunction(any, Bool)) AS x, toUInt8(x), x = true;
SELECT CAST(0.5 AS SimpleAggregateFunction(any, Bool)) AS x, toUInt8(x), x = true;
SELECT CAST(toDecimal32(0.5, 2) AS SimpleAggregateFunction(any, Bool)) AS x, toUInt8(x), x = true;
SELECT CAST(0 AS SimpleAggregateFunction(any, Bool)) AS x, toUInt8(x), x = true;
SELECT CAST(2 AS SimpleAggregateFunction(max, Bool)) AS x, toUInt8(x), x = true;
SELECT accurateCast(2, 'SimpleAggregateFunction(any, Bool)') AS x, toUInt8(x), x = true;
SELECT accurateCastOrNull(300, 'SimpleAggregateFunction(any, Bool)') AS x, toUInt8(x), x = true;

-- The aggregate result of these is not Bool, so their values keep the UInt8 range.
SELECT 'not bool';
SELECT toUInt8(CAST(2 AS SimpleAggregateFunction(max, UInt8)));
SELECT toUInt8(CAST(toUInt16(2) AS SimpleAggregateFunction(sumWithOverflow, Bool)));
SELECT toUInt8(CAST(toUInt16(2) AS SimpleAggregateFunction(groupBitOr, Bool)));
DROP TABLE IF EXISTS so;
CREATE TABLE so (k UInt8, c SimpleAggregateFunction(sumWithOverflow, Bool)) ENGINE = AggregatingMergeTree ORDER BY k;
INSERT INTO so SELECT 1, toUInt16(2);
INSERT INTO so SELECT 1, toUInt16(2);
OPTIMIZE TABLE so FINAL;
SELECT k, toUInt8(c) FROM so FINAL;
DROP TABLE so;

SELECT 'containers';
SELECT CAST([toUInt16(2)] AS Array(SimpleAggregateFunction(any, Bool))) AS a, toUInt8(a[1]);
SELECT CAST(tuple(toInt32(2)) AS Tuple(SimpleAggregateFunction(any, Bool))) AS t, toUInt8(t.1);

SELECT 'jit';
SELECT toUInt8(CAST(number + 2 AS SimpleAggregateFunction(any, Bool))) FROM numbers(2) SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;

SELECT 'column';
DROP TABLE IF EXISTS t;
CREATE TABLE t (k UInt8, v SimpleAggregateFunction(any, Bool) DEFAULT 2) ENGINE = AggregatingMergeTree ORDER BY k;
INSERT INTO t SELECT 1, toInt32(2);
INSERT INTO t (k) VALUES (2);
INSERT INTO t VALUES (3, 300);
INSERT INTO t SETTINGS input_format_values_deduce_templates_of_expressions = 0 VALUES (4, 1 + 1);
SELECT k, v, toUInt8(v) FROM t ORDER BY k;
SELECT count() FROM t WHERE v = true;
DROP TABLE t;

SELECT 'values';
SELECT v, toUInt8(v) FROM values('v SimpleAggregateFunction(any, Bool)', 300);

SELECT 'sets';
DROP TABLE IF EXISTS s;
CREATE TABLE s (k UInt8, b Bool, v SimpleAggregateFunction(any, Bool)) ENGINE = MergeTree ORDER BY k;
INSERT INTO s VALUES (1, true, true), (2, false, false);
-- Separate queries: within one query the set built for `b` would be reused for `v`.
SELECT countIf(b IN (2)), countIf(b NOT IN (2)), countIf(b IN (2, 300)), countIf(b IN [2]), countIf((b, k) IN [(2, 1)]), countIf(b IN (1)) FROM s;
SELECT countIf(v IN (2)), countIf(v NOT IN (2)), countIf(v IN (2, 300)), countIf(v IN [2]), countIf((v, k) IN [(2, 1)]), countIf(v IN (1)) FROM s;
DROP TABLE s;
DROP TABLE IF EXISTS tk;
CREATE TABLE tk (u UInt8) ENGINE = MergeTree ORDER BY u SETTINGS index_granularity = 1;
INSERT INTO tk VALUES (0), (1), (2), (3);
SELECT count() FROM tk WHERE CAST(u AS SimpleAggregateFunction(any, Bool)) IN (2);
SELECT count() FROM tk WHERE CAST(u AS SimpleAggregateFunction(any, Bool)) NOT IN (2);
SELECT count() FROM tk WHERE CAST(u AS SimpleAggregateFunction(any, Bool)) IN (1);
DROP TABLE tk;

SELECT 'in order';
DROP TABLE IF EXISTS o;
CREATE TABLE o (u UInt8, k UInt8) ENGINE = MergeTree ORDER BY (u, k);
INSERT INTO o VALUES (1, 0), (1, 100), (2, 0);
SELECT CAST(u, 'SimpleAggregateFunction(any, Bool)') AS c, k FROM o ORDER BY c, k LIMIT 2 SETTINGS optimize_read_in_order = 1;
SELECT DISTINCT CAST(u, 'SimpleAggregateFunction(any, Bool)') AS c, k FROM o ORDER BY c, k SETTINGS optimize_distinct_in_order = 1;
SELECT CAST(u, 'SimpleAggregateFunction(any, Nullable(Bool))') AS c, k FROM o ORDER BY c, k LIMIT 2 SETTINGS optimize_read_in_order = 1;
SELECT DISTINCT CAST(u, 'SimpleAggregateFunction(any, Nullable(Bool))') AS c, k FROM o ORDER BY c, k SETTINGS optimize_distinct_in_order = 1;
SELECT CAST(u, 'Bool') AS c, k FROM o ORDER BY c, k LIMIT 2 SETTINGS optimize_read_in_order = 1;
SELECT DISTINCT CAST(u, 'Bool') AS c, k FROM o ORDER BY c, k SETTINGS optimize_distinct_in_order = 1;
DROP TABLE o;

SELECT 'same as bool';
SELECT groupArray(number) FROM numbers(4) WHERE number IN (SELECT true);
SELECT groupArray(number) FROM numbers(4) WHERE number IN (SELECT anySimpleState(true));
DROP TABLE IF EXISTS jb;
DROP TABLE IF EXISTS js;
DROP TABLE IF EXISTS ju;
CREATE TABLE jb (v Nullable(Bool)) ENGINE = Memory;
CREATE TABLE js (v Nullable(SimpleAggregateFunction(any, Bool))) ENGINE = Memory;
CREATE TABLE ju (v UInt8) ENGINE = Memory;
INSERT INTO jb VALUES (true);
INSERT INTO js VALUES (true);
INSERT INTO ju VALUES (2);
SELECT count() FROM jb INNER JOIN ju ON jb.v = ju.v;
SELECT count() FROM js INNER JOIN ju ON js.v = ju.v;
SELECT count() FROM ju INNER JOIN jb ON jb.v = ju.v;
SELECT count() FROM ju INNER JOIN js ON js.v = ju.v;
DROP TABLE jb;
DROP TABLE js;
DROP TABLE ju;
SELECT toUInt8(JSONExtract('{"a":2}', 'a', 'Bool')), toUInt8(JSONExtract('{"a":2}', 'a', 'SimpleAggregateFunction(any, Bool)'));
SELECT toUInt8(JSONExtract('{"a":2}', 'a', 'LowCardinality(Bool)')), toUInt8(JSONExtract('{"a":2}', 'a', 'LowCardinality(SimpleAggregateFunction(any, Bool))'));

-- An INSERT from UInt8 stores 2 as is; the correlated equality must not depend on the substitution.
SELECT 'correlated';
DROP TABLE IF EXISTS ov;
DROP TABLE IF EXISTS i8;
DROP TABLE IF EXISTS i16;
DROP TABLE IF EXISTS im;
DROP TABLE IF EXISTS ob;
CREATE TABLE ov (k UInt8, v SimpleAggregateFunction(any, Bool)) ENGINE = MergeTree ORDER BY k;
CREATE TABLE i8 (u UInt8) ENGINE = MergeTree ORDER BY u;
CREATE TABLE i16 (u UInt16) ENGINE = MergeTree ORDER BY u;
CREATE TABLE im (k UInt8, m SimpleAggregateFunction(max, Bool)) ENGINE = MergeTree ORDER BY k;
CREATE TABLE ob (k UInt8, b Bool) ENGINE = MergeTree ORDER BY k;
INSERT INTO ov SELECT 1, toUInt8(2);
INSERT INTO i8 VALUES (2);
INSERT INTO i16 VALUES (2);
INSERT INTO im SELECT 1, toUInt8(2);
INSERT INTO ob SELECT 1, toUInt8(2);
SELECT count() FROM ov WHERE EXISTS (SELECT 1 FROM i8 WHERE i8.u = ov.v);
SELECT count() FROM ov WHERE EXISTS (SELECT 1 FROM i8 WHERE i8.u = ov.v) SETTINGS correlated_subqueries_substitute_equivalent_expressions = 0;
SELECT count() FROM ov WHERE EXISTS (SELECT 1 FROM i16 WHERE i16.u = ov.v);
SELECT count() FROM ov WHERE EXISTS (SELECT 1 FROM i16 WHERE i16.u = ov.v) SETTINGS correlated_subqueries_substitute_equivalent_expressions = 0;
SELECT count() FROM ov WHERE EXISTS (SELECT 1 FROM im WHERE im.m = ov.v);
SELECT count() FROM ov WHERE EXISTS (SELECT 1 FROM im WHERE im.m = ov.v) SETTINGS correlated_subqueries_substitute_equivalent_expressions = 0;
SELECT count() FROM ov WHERE EXISTS (SELECT 1 FROM ob WHERE ob.b = ov.v);
SELECT count() FROM ov WHERE EXISTS (SELECT 1 FROM ob WHERE ob.b = ov.v) SETTINGS correlated_subqueries_substitute_equivalent_expressions = 0;
SELECT count() FROM ob WHERE EXISTS (SELECT 1 FROM im WHERE im.m = ob.b);
SELECT count() FROM ob WHERE EXISTS (SELECT 1 FROM im WHERE im.m = ob.b) SETTINGS correlated_subqueries_substitute_equivalent_expressions = 0;
DROP TABLE ov;
DROP TABLE i8;
DROP TABLE i16;
DROP TABLE im;
DROP TABLE ob;
