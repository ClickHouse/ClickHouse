-- Tags: distributed

-- A column read from a view (or a Merge table) has the declared type even when it differs from the
-- source type only by the time zone or by a custom name such as `Bool`.

SET session_timezone = 'Asia/Kolkata';
SET explain_query_plan_default = 'legacy';
SET enable_parallel_replicas = 0;
SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS t_src;
DROP TABLE IF EXISTS t_order;
DROP TABLE IF EXISTS t_local;
DROP TABLE IF EXISTS t_dist;
DROP TABLE IF EXISTS t_bool;
DROP TABLE IF EXISTS t_dst;
DROP TABLE IF EXISTS t_merge;

CREATE TABLE t_src (ts DateTime, ts64 DateTime64(3), u UInt8) ENGINE = Memory;
INSERT INTO t_src VALUES (1767225600, 1767225600.123, 2);

CREATE VIEW v (ts DateTime('UTC'), ts64 DateTime64(3, 'UTC'), u Bool, a Array(DateTime('UTC'))) AS SELECT ts, ts64, u, [ts] AS a FROM t_src;
CREATE VIEW v_wrap (m Map(String, DateTime('UTC')), t Tuple(a DateTime('UTC')), nb Nullable(Bool)) AS SELECT map('k', ts) AS m, tuple(ts)::Tuple(a DateTime) AS t, toNullable(u) AS nb FROM t_src;
CREATE VIEW v_cast (ts DateTime('UTC')) AS SELECT CAST(ts AS DateTime('UTC')) AS ts FROM t_src;
CREATE VIEW v_same (ts DateTime, u UInt8) AS SELECT ts, u FROM t_src;

SELECT 'view';
SELECT ts, toHour(ts), toString(ts), ts64, toHour(ts64), u, toString(u), u = true, a FROM v;
SELECT count() FROM v WHERE toHour(ts) = 0;
SELECT m, t, nb, nb = true FROM v_wrap;

SELECT 'inlined view';
SELECT toTypeName(ts), ts, toHour(ts), toTypeName(u), u, a FROM v SETTINGS analyzer_inline_views = 1;

SELECT 'explicit CAST and same types';
SELECT ts, toHour(ts) FROM v_cast;
SELECT ts, u FROM v_same;
SELECT count() FROM (EXPLAIN actions = 1 SELECT ts, u FROM v_same) WHERE explain ILIKE '%\_CAST%';

SELECT 'view over Distributed';
CREATE TABLE t_local (ts DateTime, u UInt8) ENGINE = MergeTree ORDER BY ts;
CREATE TABLE t_dist AS t_local ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_local);
INSERT INTO t_local VALUES (1767225600, 2);
CREATE VIEW v_dist (ts DateTime('UTC'), u Bool) AS SELECT ts, u FROM t_dist;
SELECT ts, toHour(ts), u FROM v_dist SETTINGS optimize_trivial_view_pushdown_to_distributed = 1;
SELECT ts, toHour(ts), u FROM v_dist SETTINGS optimize_trivial_view_pushdown_to_distributed = 0;

SELECT 'ORDER BY LIMIT pushed into view';
CREATE TABLE t_order (u UInt8, k UInt8) ENGINE = Memory;
INSERT INTO t_order VALUES (1, 100), (2, 0);
CREATE VIEW v_order (u Bool, k UInt8) AS SELECT u, k FROM t_order;
CREATE VIEW v_order_same (u UInt8, k UInt8) AS SELECT u, k FROM t_order;
SELECT u, k FROM v_order ORDER BY u, k LIMIT 1;
SELECT countIf(explain LIKE '%Sorting (Sorting for ORDER BY)%')
FROM (EXPLAIN SELECT u, k FROM v_order_same ORDER BY u, k LIMIT 1 SETTINGS query_plan_optimize_lazy_materialization = 0);

SELECT 'INSERT SELECT into Bool';
CREATE TABLE t_bool (b Bool) ENGINE = Memory;
INSERT INTO t_bool SELECT 2::UInt8;
SELECT b, b = true, toUInt8(b) FROM t_bool;

SELECT 'Merge';
CREATE TABLE t_dst (x DateTime) ENGINE = Memory;
INSERT INTO t_dst VALUES (1767225600);
CREATE TABLE t_merge (x DateTime('UTC')) ENGINE = Merge(currentDatabase(), '^t_dst$');
SELECT x, toHour(x) FROM t_merge;

DROP VIEW v;
DROP VIEW v_wrap;
DROP VIEW v_cast;
DROP VIEW v_same;
DROP VIEW v_dist;
DROP VIEW v_order;
DROP VIEW v_order_same;
DROP TABLE t_src;
DROP TABLE t_order;
DROP TABLE t_local;
DROP TABLE t_dist;
DROP TABLE t_bool;
DROP TABLE t_merge;
DROP TABLE t_dst;
