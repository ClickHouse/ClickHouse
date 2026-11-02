-- https://github.com/ClickHouse/ClickHouse/issues/120901
-- A projection which uses a subcolumn inside an expression is calculated on INSERT.

SET optimize_aggregation_in_order = 0, parallel_replicas_local_plan = 1, parallel_replicas_support_projection = 1;

DROP TABLE IF EXISTS t_json;
DROP TABLE IF EXISTS t_json_dynamic_path;
DROP TABLE IF EXISTS t_tuple;
DROP TABLE IF EXISTS t_nullable;
DROP TABLE IF EXISTS t_dynamic;
DROP TABLE IF EXISTS t_where;

SELECT 'JSON typed path';
CREATE TABLE t_json (id UInt64, j JSON(a UInt64)) ENGINE = MergeTree ORDER BY id;
SYSTEM STOP MERGES t_json;
INSERT INTO t_json SELECT number, '{"a":' || toString(number % 3) || '}' FROM numbers(10);
ALTER TABLE t_json ADD PROJECTION p (SELECT j.a::UInt64 AS k, count() GROUP BY k);
ALTER TABLE t_json ADD PROJECTION p_sum (SELECT id % 2 AS g, sum(j.a) GROUP BY g);
INSERT INTO t_json SELECT number + 100, '{"a":' || toString(number % 3) || '}' FROM numbers(5);
SELECT j.a::UInt64 AS k, count() FROM t_json GROUP BY k ORDER BY k SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT j.a::UInt64 AS k, count() FROM t_json GROUP BY k ORDER BY k SETTINGS optimize_use_projections = 0;
SELECT id % 2 AS g, sum(j.a) FROM t_json GROUP BY g ORDER BY g SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT id % 2 AS g, sum(j.a) FROM t_json GROUP BY g ORDER BY g SETTINGS optimize_use_projections = 0;

SELECT 'JSON dynamic path';
CREATE TABLE t_json_dynamic_path (id UInt64, j JSON) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_json_dynamic_path ADD PROJECTION p (SELECT j.b::UInt64 AS k, count() GROUP BY k);
INSERT INTO t_json_dynamic_path SELECT number, '{"b":' || toString(number % 3) || '}' FROM numbers(10);
SELECT j.b::UInt64 AS k, count() FROM t_json_dynamic_path GROUP BY k ORDER BY k SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT j.b::UInt64 AS k, count() FROM t_json_dynamic_path GROUP BY k ORDER BY k SETTINGS optimize_use_projections = 0;

SELECT 'Tuple element';
CREATE TABLE t_tuple (id UInt64, t Tuple(a UInt32, b String), PROJECTION p (SELECT t.a + 1 AS k, count() GROUP BY k)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_tuple SELECT number, tuple(number % 4, 'x') FROM numbers(10);
SELECT t.a + 1 AS k, count() FROM t_tuple GROUP BY k ORDER BY k SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT t.a + 1 AS k, count() FROM t_tuple GROUP BY k ORDER BY k SETTINGS optimize_use_projections = 0;

SELECT 'Nullable null';
CREATE TABLE t_nullable (id UInt64, n Nullable(UInt32)) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_nullable ADD PROJECTION p (SELECT toUInt8(n.null) AS k, count() GROUP BY k);
INSERT INTO t_nullable SELECT number, if(number % 3 = 0, NULL, number) FROM numbers(10);
SELECT toUInt8(n.null) AS k, count() FROM t_nullable GROUP BY k ORDER BY k SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT toUInt8(n.null) AS k, count() FROM t_nullable GROUP BY k ORDER BY k SETTINGS optimize_use_projections = 0;

SELECT 'Dynamic';
CREATE TABLE t_dynamic (id UInt64, d Dynamic, PROJECTION p (SELECT (d.`Tuple(x Int64)`).x AS px, count() GROUP BY px)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_dynamic SELECT number, tuple(number % 2)::Tuple(x Int64) FROM numbers(5);
SELECT (d.`Tuple(x Int64)`).x AS px, count() FROM t_dynamic GROUP BY px ORDER BY px SETTINGS optimize_use_projections = 0;

SELECT 'WHERE with the parent column';
CREATE TABLE t_where (id UInt64, j JSON(a UInt64)) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_where ADD PROJECTION p (SELECT id, j WHERE j.a > 6 ORDER BY id);
INSERT INTO t_where SELECT number, '{"a":' || toString(number) || '}' FROM numbers(10);
SELECT id, j FROM t_where WHERE j.a > 6 ORDER BY id SETTINGS optimize_use_projections = 0;

SELECT 'Projection parts';
SELECT table, name, rows FROM system.projection_parts WHERE database = currentDatabase() AND active ORDER BY table, name;

DROP TABLE t_json;
DROP TABLE t_json_dynamic_path;
DROP TABLE t_tuple;
DROP TABLE t_nullable;
DROP TABLE t_dynamic;
DROP TABLE t_where;
