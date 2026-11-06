-- Tags: distributed

-- A column read from a view (or a Merge table) has the declared type even when it differs from the
-- source type only by the time zone.

SET session_timezone = 'Asia/Kolkata';
SET explain_query_plan_default = 'legacy';
SET enable_parallel_replicas = 0;
SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS t_src;
DROP TABLE IF EXISTS t_local;
DROP TABLE IF EXISTS t_dist;
DROP TABLE IF EXISTS t_mt;
DROP TABLE IF EXISTS t_dst;
DROP TABLE IF EXISTS t_merge;
DROP TABLE IF EXISTS t_var;
DROP TABLE IF EXISTS t_var_dst;
DROP TABLE IF EXISTS t_vaf;
DROP TABLE IF EXISTS t_aaf;
DROP TABLE IF EXISTS t_any_mixed;
DROP TABLE IF EXISTS t_qaf_nested;
DROP TABLE IF EXISTS t_vaf_nested;
DROP TABLE IF EXISTS t_mlocal;
DROP TABLE IF EXISTS t_mdist;
DROP TABLE IF EXISTS t_merge_dist;

CREATE TABLE t_src (ts DateTime, ts64 DateTime64(3), u UInt8) ENGINE = Memory;
INSERT INTO t_src VALUES (1767225600, 1767225600.123, 2);

CREATE VIEW v (ts DateTime('UTC'), ts64 DateTime64(3, 'UTC'), a Array(DateTime('UTC'))) AS SELECT ts, ts64, [ts] AS a FROM t_src;
CREATE VIEW v_wrap (m Map(String, DateTime('UTC')), t Tuple(a DateTime('UTC'))) AS SELECT map('k', ts) AS m, tuple(ts)::Tuple(a DateTime) AS t FROM t_src;
CREATE VIEW v_cast (ts DateTime('UTC')) AS SELECT CAST(ts AS DateTime('UTC')) AS ts FROM t_src;
CREATE VIEW v_same (ts DateTime, u UInt8) AS SELECT ts, u FROM t_src;
CREATE VIEW v_bool (u Bool) AS SELECT u FROM t_src;

SELECT 'view';
SELECT ts, toHour(ts), toString(ts), ts64, toHour(ts64), a FROM v;
SELECT count() FROM v WHERE toHour(ts) = 0;
SELECT m, t FROM v_wrap;

SELECT 'inlined view';
SELECT toTypeName(ts), ts, toHour(ts), a FROM v SETTINGS analyzer_inline_views = 1;
SELECT countIf(explain LIKE '%VIEW subquery%') = 0 AS inlined, countIf(explain LIKE '%\_CAST%') > 0 AS has_cast FROM (EXPLAIN actions = 1 SELECT ts FROM v SETTINGS analyzer_inline_views = 1);

SELECT 'explicit CAST and same types';
SELECT ts, toHour(ts) FROM v_cast;
SELECT ts, u FROM v_same;
SELECT count() FROM (EXPLAIN actions = 1 SELECT ts, u FROM v_same) WHERE explain ILIKE '%\_CAST%';
-- A cast to `Bool` changes values, so it is not added for a column that differs only by `Bool`.
SELECT count() FROM (EXPLAIN actions = 1 SELECT u FROM v_bool) WHERE explain ILIKE '%\_CAST%';

SELECT 'view over MergeTree';
CREATE TABLE t_mt (ts DateTime) ENGINE = MergeTree ORDER BY ts SETTINGS index_granularity = 1;
INSERT INTO t_mt VALUES (1767225600), (1767229200);
CREATE VIEW v_mt (ts DateTime('UTC')) AS SELECT ts FROM t_mt;
SELECT count() FROM v_mt WHERE ts = '2026-01-01 00:00:00';
SELECT ts FROM v_mt ORDER BY ts LIMIT 1;
SELECT count() FROM v_mt WHERE toHour(ts) = 0;

SELECT 'view over Distributed';
CREATE TABLE t_local (ts DateTime) ENGINE = MergeTree ORDER BY ts;
CREATE TABLE t_dist AS t_local ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_local);
INSERT INTO t_local VALUES (1767225600);
CREATE VIEW v_dist (ts DateTime('UTC')) AS SELECT ts FROM t_dist;
SELECT ts, toHour(ts) FROM v_dist SETTINGS optimize_trivial_view_pushdown_to_distributed = 1;
SELECT ts, toHour(ts) FROM v_dist SETTINGS optimize_trivial_view_pushdown_to_distributed = 0;
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed FROM (EXPLAIN SELECT ts FROM v_dist SETTINGS optimize_trivial_view_pushdown_to_distributed = 1);

SELECT 'Merge';
CREATE TABLE t_dst (x DateTime) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_dst VALUES (1767225600);
CREATE TABLE t_merge (x DateTime('UTC')) ENGINE = Merge(currentDatabase(), '^t_dst$');
SELECT x, toHour(x) FROM t_merge;
SELECT count() FROM t_merge WHERE toHour(x) = 0;
SELECT count() FROM t_merge PREWHERE toHour(x) = 0; -- { serverError ILLEGAL_PREWHERE }

SELECT 'Merge over Distributed';
CREATE TABLE t_mlocal (ts DateTime) ENGINE = MergeTree ORDER BY ts;
INSERT INTO t_mlocal VALUES (1767225600), (1767225600);
CREATE TABLE t_mdist AS t_mlocal ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_mlocal);
CREATE TABLE t_merge_dist (ts DateTime('UTC')) ENGINE = Merge(currentDatabase(), '^t_mdist$');
SELECT toHour(ts), count() FROM t_merge_dist GROUP BY 1;
SELECT count() FROM t_merge_dist WHERE toHour(ts) = 0;

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
-- Equal Variants whose alternatives differ in more than a relabelling still need a conversion.
SELECT CAST(quantilesState(0.9)(number)::Variant(AggregateFunction(quantiles(0.9), UInt64), String) AS Variant(AggregateFunction(quantiles(0.1), UInt64), String)) FROM numbers(100); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST([2::UInt8]::Variant(Array(UInt8), String) AS Variant(Array(Bool), String)); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST(CAST(quantilesState(0.9)(number), 'Variant(AggregateFunction(0, quantiles(0.9), UInt64), String)') AS Variant(AggregateFunction(1, quantile, UInt64), String)) FROM numbers(5); -- { serverError CANNOT_CONVERT_TYPE }

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

SELECT 'AggregateFunction over a time zone';
CREATE VIEW v_any_tz (s AggregateFunction(any, DateTime('UTC'))) AS SELECT anyState(toDateTime(1767225600)) AS s;
SELECT toHour(finalizeAggregation(s)) FROM v_any_tz;
SELECT toHour(finalizeAggregation(s)) FROM v_any_tz SETTINGS analyzer_inline_views = 1;
CREATE TABLE t_any_mixed (s AggregateFunction(any, Tuple(DateTime, AggregateFunction(uniq, UInt64)))) ENGINE = Memory;
INSERT INTO t_any_mixed SELECT anyState(tuple(toDateTime(1767225600), u)) FROM (SELECT uniqState(number) AS u FROM numbers(10));
CREATE VIEW v_any_mixed (s AggregateFunction(any, Tuple(DateTime('UTC'), AggregateFunction(uniq, UInt64)))) AS SELECT s FROM t_any_mixed;
SELECT toHour(finalizeAggregation(s).1) FROM v_any_mixed SETTINGS analyzer_inline_views = 1;

SELECT 'AggregateFunction of another function with the same state';
-- States of another function, or nested in a state, are not relabelled: the view fails to finalize them, the inlined view keeps the source type.
CREATE VIEW v_qaf (s AggregateFunction(quantile, UInt64)) AS SELECT quantilesState(0.9)(number) AS s FROM numbers(5);
SELECT finalizeAggregation(s) FROM v_qaf; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT finalizeAggregation(s) FROM v_qaf SETTINGS analyzer_inline_views = 1;
CREATE TABLE t_qaf_nested (s AggregateFunction(any, AggregateFunction(quantiles(0.9), UInt64))) ENGINE = Memory;
INSERT INTO t_qaf_nested SELECT anyState(q) FROM (SELECT quantilesState(0.9)(number) AS q FROM numbers(5));
CREATE VIEW v_qaf_nested (s AggregateFunction(any, AggregateFunction(quantile, UInt64))) AS SELECT s FROM t_qaf_nested;
SELECT finalizeAggregation(finalizeAggregation(s)) FROM v_qaf_nested SETTINGS analyzer_inline_views = 1;
CREATE TABLE t_vaf_nested (s AggregateFunction(any, AggregateFunction(0, uniq, UInt64))) ENGINE = Memory;
INSERT INTO t_vaf_nested SELECT anyState(u) FROM (SELECT uniqState(number)::AggregateFunction(0, uniq, UInt64) AS u FROM numbers(10));
CREATE VIEW v_vaf_nested (s AggregateFunction(any, AggregateFunction(1, uniq, UInt64))) AS SELECT s FROM t_vaf_nested;
SELECT uniqMerge(u) FROM (SELECT arrayJoin(groupArray(finalizeAggregation(s))) AS u FROM v_vaf_nested SETTINGS analyzer_inline_views = 1);

DROP VIEW v;
DROP VIEW v_wrap;
DROP VIEW v_cast;
DROP VIEW v_same;
DROP VIEW v_bool;
DROP VIEW v_mt;
DROP VIEW v_dist;
DROP VIEW v_var;
DROP VIEW v_vaf;
DROP VIEW v_aaf;
DROP VIEW v_any_tz;
DROP VIEW v_any_mixed;
DROP VIEW v_qaf;
DROP VIEW v_qaf_nested;
DROP VIEW v_vaf_nested;
DROP TABLE t_src;
DROP TABLE t_mt;
DROP TABLE t_local;
DROP TABLE t_dist;
DROP TABLE t_merge;
DROP TABLE t_dst;
DROP TABLE t_var;
DROP TABLE t_var_dst;
DROP TABLE t_vaf;
DROP TABLE t_aaf;
DROP TABLE t_any_mixed;
DROP TABLE t_qaf_nested;
DROP TABLE t_vaf_nested;
DROP TABLE t_merge_dist;
DROP TABLE t_mdist;
DROP TABLE t_mlocal;
