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
DROP TABLE IF EXISTS t_var;
DROP TABLE IF EXISTS t_var_dst;
DROP TABLE IF EXISTS t_vaf;
DROP TABLE IF EXISTS t_aaf;
DROP TABLE IF EXISTS t_mlocal;
DROP TABLE IF EXISTS t_mdist;
DROP TABLE IF EXISTS t_merge_dist;

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
SELECT countIf(explain LIKE '%VIEW subquery%') = 0 AS inlined, countIf(explain LIKE '%\_CAST%') > 0 AS has_cast FROM (EXPLAIN actions = 1 SELECT ts FROM v SETTINGS analyzer_inline_views = 1);

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
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed FROM (EXPLAIN SELECT ts, u FROM v_dist SETTINGS optimize_trivial_view_pushdown_to_distributed = 1);

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
CREATE TABLE t_dst (x DateTime) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_dst VALUES (1767225600);
CREATE TABLE t_merge (x DateTime('UTC')) ENGINE = Merge(currentDatabase(), '^t_dst$');
SELECT x, toHour(x) FROM t_merge;
SELECT count() FROM t_merge WHERE toHour(x) = 0;
SELECT count() FROM t_merge PREWHERE toHour(x) = 0; -- { serverError ILLEGAL_PREWHERE }

SELECT 'Merge over Distributed';
CREATE TABLE t_mlocal (ts DateTime, u UInt8) ENGINE = MergeTree ORDER BY ts;
INSERT INTO t_mlocal VALUES (1767225600, 1), (1767225600, 2);
CREATE TABLE t_mdist AS t_mlocal ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_mlocal);
CREATE TABLE t_merge_dist (ts DateTime('UTC'), u Bool) ENGINE = Merge(currentDatabase(), '^t_mdist$');
SELECT toHour(ts), count() FROM t_merge_dist GROUP BY 1;
SELECT count() FROM t_merge_dist WHERE toHour(ts) = 0;
-- A remote shard's UInt8 is relabelled as Bool without normalization (castColumn), so the local replica is read.
SELECT DISTINCT u FROM t_merge_dist SETTINGS prefer_localhost_replica = 1;

SELECT 'Variant';
CREATE TABLE t_var (x Variant(DateTime, String)) ENGINE = Memory;
INSERT INTO t_var SELECT toDateTime(1767225600)::Variant(DateTime, String);
CREATE VIEW v_var (x Variant(DateTime('UTC'), String)) AS SELECT x FROM t_var;
SELECT x, variantType(x) FROM v_var;
SELECT x FROM v_var SETTINGS analyzer_inline_views = 1;
CREATE TABLE t_var_dst (x Variant(DateTime('UTC'), String)) ENGINE = Memory;
INSERT INTO t_var_dst SELECT x FROM t_var;
SELECT x FROM t_var_dst;
SELECT CAST(x AS Variant(DateTime('UTC'), String)) FROM t_var;

SELECT 'AggregateFunction state version';
CREATE TABLE t_vaf (x Variant(AggregateFunction(0, uniq, UInt64), String)) ENGINE = Memory;
INSERT INTO t_vaf SELECT uniqState(number)::AggregateFunction(0, uniq, UInt64)::Variant(AggregateFunction(0, uniq, UInt64), String) FROM numbers(10);
CREATE VIEW v_vaf (x Variant(AggregateFunction(1, uniq, UInt64), String)) AS SELECT x FROM t_vaf;
SELECT uniqMerge(s) FROM (SELECT arrayJoin(groupArray(variantElement(x, 'AggregateFunction(1, uniq, UInt64)'))) AS s FROM v_vaf);
SELECT uniqMerge(s) FROM (SELECT arrayJoin(groupArray(variantElement(CAST(x AS Variant(AggregateFunction(1, uniq, UInt64), String)), 'AggregateFunction(1, uniq, UInt64)'))) AS s FROM t_vaf);
CREATE TABLE t_aaf (a Array(AggregateFunction(0, uniq, UInt64))) ENGINE = Memory;
INSERT INTO t_aaf SELECT [uniqState(number)::AggregateFunction(0, uniq, UInt64)] FROM numbers(10);
CREATE VIEW v_aaf (a Array(AggregateFunction(1, uniq, UInt64))) AS SELECT a FROM t_aaf;
SELECT uniqMerge(s) FROM (SELECT arrayJoin(groupArray(arrayJoin(a))) AS s FROM v_aaf);

DROP VIEW v;
DROP VIEW v_wrap;
DROP VIEW v_cast;
DROP VIEW v_same;
DROP VIEW v_dist;
DROP VIEW v_order;
DROP VIEW v_order_same;
DROP VIEW v_var;
DROP VIEW v_vaf;
DROP VIEW v_aaf;
DROP TABLE t_src;
DROP TABLE t_order;
DROP TABLE t_local;
DROP TABLE t_dist;
DROP TABLE t_bool;
DROP TABLE t_merge;
DROP TABLE t_dst;
DROP TABLE t_var;
DROP TABLE t_var_dst;
DROP TABLE t_vaf;
DROP TABLE t_aaf;
DROP TABLE t_merge_dist;
DROP TABLE t_mdist;
DROP TABLE t_mlocal;
