-- A CAST into Bool maps every nonzero value to true: in-order optimizations must not treat it as strictly
-- monotonic, and the primary key analysis must not treat its DateTime64 version as monotonic across the epoch.

DROP TABLE IF EXISTS t_cast_bool;
CREATE TABLE t_cast_bool (u UInt8, k UInt8, b Bool ALIAS u) ENGINE = MergeTree ORDER BY (u, k);
INSERT INTO t_cast_bool VALUES (1, 0), (1, 100), (2, 0);

SELECT CAST(u, 'Bool') AS c, k FROM t_cast_bool ORDER BY c, k LIMIT 2 SETTINGS optimize_read_in_order = 1;
SELECT b, k FROM t_cast_bool ORDER BY b, k LIMIT 2 SETTINGS optimize_read_in_order = 1;
SELECT k FROM t_cast_bool WHERE CAST(u, 'Bool') = true ORDER BY k LIMIT 2 SETTINGS optimize_read_in_order = 1;
SELECT CAST(u, 'Bool') AS c, k, count() FROM t_cast_bool GROUP BY c, k ORDER BY c, k SETTINGS optimize_aggregation_in_order = 1;
SELECT DISTINCT CAST(u, 'Bool') AS c, k FROM t_cast_bool ORDER BY c, k SETTINGS optimize_distinct_in_order = 1;
SELECT CAST(u, 'Bool') AS c, k FROM t_cast_bool ORDER BY c, k LIMIT 1 BY c, k SETTINGS optimize_read_in_order = 1;
SELECT CAST(u, 'Bool') AS c, k FROM (SELECT u, k FROM t_cast_bool ORDER BY u, k) ORDER BY c, k
SETTINGS optimize_read_in_order = 0, query_plan_remove_redundant_sorting = 0, optimize_sorting_by_input_stream_properties = 1;

SELECT count() FROM (EXPLAIN actions = 1 SELECT CAST(u, 'Bool') AS c, k FROM t_cast_bool ORDER BY c, k LIMIT 2 SETTINGS optimize_read_in_order = 1)
WHERE explain ILIKE '%Prefix sort description:%' AND explain NOT ILIKE '%,%';

DROP TABLE t_cast_bool;

DROP TABLE IF EXISTS t_cast_bool_dt64;
CREATE TABLE t_cast_bool_dt64 (ts DateTime64(0, 'UTC')) ENGINE = MergeTree ORDER BY CAST(ts AS Bool) SETTINGS index_granularity = 1;
INSERT INTO t_cast_bool_dt64 VALUES ('1969-12-31 23:59:59'), ('1970-01-01 00:00:00'), ('1970-01-01 00:00:01');
SELECT ts FROM t_cast_bool_dt64 WHERE ts <= toDateTime64('1970-01-01 00:00:00', 0, 'UTC') ORDER BY ts;
DROP TABLE t_cast_bool_dt64;
