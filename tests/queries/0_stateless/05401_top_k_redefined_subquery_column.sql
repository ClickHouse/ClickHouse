-- Random settings limits: use_top_k_dynamic_filtering=(1, 1); use_skip_indexes_for_top_k=(1, 1); query_plan_max_limit_for_top_k_optimization=(1000, None)

-- `ORDER BY k LIMIT n` over a subquery that redefines `k` as an expression of the table column `k`:
-- the top-K must not be applied to the table column.

DROP TABLE IF EXISTS t_mt;
DROP TABLE IF EXISTS t_mt_minmax;
DROP TABLE IF EXISTS t_u32;
DROP TABLE IF EXISTS t_memory;

CREATE TABLE t_mt (k UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_mt_minmax (k UInt64, INDEX i k TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1024;
CREATE TABLE t_u32 (k UInt32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_memory (k UInt64) ENGINE = Memory;

INSERT INTO t_mt SELECT number FROM numbers(200000);
INSERT INTO t_mt_minmax SELECT number FROM numbers(200000);
INSERT INTO t_u32 SELECT number FROM numbers(200000);
INSERT INTO t_memory SELECT number FROM numbers(200000);

SELECT k FROM (SELECT toUInt64(200000 - k) AS k FROM t_mt) ORDER BY k LIMIT 3 SETTINGS max_threads = 1;
SELECT k FROM (SELECT k * 10 AS k FROM t_mt) ORDER BY k DESC LIMIT 3 SETTINGS max_threads = 1;
SELECT k FROM (SELECT -toInt64(k) AS k FROM t_u32) ORDER BY k LIMIT 3 SETTINGS max_threads = 1;
SELECT k FROM (SELECT toUInt64(200000 - k) AS k FROM t_mt_minmax) ORDER BY k LIMIT 3 SETTINGS max_threads = 1, use_top_k_dynamic_filtering = 0;
SELECT k FROM (SELECT toUInt64(200000 - k) AS k FROM t_memory) ORDER BY k LIMIT 3 SETTINGS max_threads = 1;

SELECT replaceRegexpOne(explain, '^[ │├└─]*', '') FROM (EXPLAIN actions = 1 SELECT k FROM (SELECT k FROM t_memory) ORDER BY k LIMIT 3) WHERE explain LIKE '%TopN filter%';
SELECT replaceRegexpOne(explain, '^[ │├└─]*', '') FROM (EXPLAIN actions = 1 SELECT x FROM (SELECT k AS x FROM t_memory WHERE k % 2 = 0) ORDER BY x LIMIT 3) WHERE explain LIKE '%TopN filter%';
SELECT replaceRegexpOne(explain, '^[ │├└─]*', '') FROM (EXPLAIN actions = 1 SELECT k FROM (SELECT toUInt64(200000 - k) AS k FROM t_memory) ORDER BY k LIMIT 3) WHERE explain LIKE '%TopN filter%';

DROP TABLE t_mt;
DROP TABLE t_mt_minmax;
DROP TABLE t_u32;
DROP TABLE t_memory;
