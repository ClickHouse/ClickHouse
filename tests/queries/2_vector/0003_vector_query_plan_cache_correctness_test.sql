-- Tags: no-parallel, no-fasttest
-- no-fasttest: vector search needs the usearch 3rd party library
-- no-parallel: the vector query plan cache and its entries metric are global
--
-- Correctness of the vector query plan cache in plan-only mode:
-- a single normalized template serves many literal values, LIMIT participates
-- in the cache key, subqueries are not cached, and a single entry is created
-- per template. Both `L2Distance` and `cosineDistance` are exercised; the
-- distance function is part of the normalized template, so the two metrics
-- form independent cache keys.
--
-- Each block runs every cache key with the same FORMAT, because
-- the FORMAT clause is part of the normalized string used as the cache key.
-- For the same reason a block keeps one `log_comment` while it executes its
-- cache keys: the key includes all changed settings, and `log_comment` is one
-- of them.
--
-- The `hits, misses, result cache hits` assertions read ProfileEvents from
-- `system.query_log` for the queries of the current block only, selected by
-- `log_comment` and by the start time of this run. `QueryCacheHits` must stay
-- 0 so that every hit is attributed to the plan cache, not the result cache.
-- Assertion queries run under a `<case>_assert` comment, so they are not
-- counted even when a case asserts twice.

DROP TABLE IF EXISTS vector_plan_cache_run_state;
DROP TABLE IF EXISTS vector_plan_cache_correctness;

CREATE TABLE vector_plan_cache_correctness
(
    id UInt32,
    vector Array(Float32)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO vector_plan_cache_correctness
SELECT number, [toFloat32(number), toFloat32(number + 3), toFloat32(number + 1)]
FROM numbers(100);

SET use_query_cache = 0;
SET vector_query_plan_cache = 1;
SET vector_only_cache_query_plan = 1;
SET enable_vector_performance_test = 1;

-- Run-scoped start time: assertions only count queries logged after it.
CREATE TABLE vector_plan_cache_run_state (started DateTime64(6)) ENGINE = Memory;
INSERT INTO vector_plan_cache_run_state SELECT now64(6);

-- C01: one template, 100 different query vectors -> 1 miss, 99 hits, 1 entry.
SET log_comment = 'vpc_correctness_c01';
SYSTEM DROP VECTOR QUERY PLAN CACHE;
SELECT id, round(L2Distance(vector, [0., 3, 1]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [1., 4, 2]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [2., 5, 3]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [3., 6, 4]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [4., 7, 5]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [5., 8, 6]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [6., 9, 7]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [7., 10, 8]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [8., 11, 9]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [9., 12, 10]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [10., 13, 11]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [11., 14, 12]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [12., 15, 13]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [13., 16, 14]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [14., 17, 15]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [15., 18, 16]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [16., 19, 17]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [17., 20, 18]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [18., 21, 19]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [19., 22, 20]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [20., 23, 21]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [21., 24, 22]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [22., 25, 23]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [23., 26, 24]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [24., 27, 25]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [25., 28, 26]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [26., 29, 27]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [27., 30, 28]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [28., 31, 29]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [29., 32, 30]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [30., 33, 31]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [31., 34, 32]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [32., 35, 33]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [33., 36, 34]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [34., 37, 35]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [35., 38, 36]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [36., 39, 37]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [37., 40, 38]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [38., 41, 39]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [39., 42, 40]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [40., 43, 41]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [41., 44, 42]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [42., 45, 43]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [43., 46, 44]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [44., 47, 45]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [45., 48, 46]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [46., 49, 47]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [47., 50, 48]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [48., 51, 49]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [49., 52, 50]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [50., 53, 51]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [51., 54, 52]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [52., 55, 53]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [53., 56, 54]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [54., 57, 55]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [55., 58, 56]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [56., 59, 57]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [57., 60, 58]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [58., 61, 59]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [59., 62, 60]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [60., 63, 61]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [61., 64, 62]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [62., 65, 63]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [63., 66, 64]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [64., 67, 65]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [65., 68, 66]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [66., 69, 67]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [67., 70, 68]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [68., 71, 69]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [69., 72, 70]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [70., 73, 71]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [71., 74, 72]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [72., 75, 73]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [73., 76, 74]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [74., 77, 75]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [75., 78, 76]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [76., 79, 77]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [77., 80, 78]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [78., 81, 79]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [79., 82, 80]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [80., 83, 81]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [81., 84, 82]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [82., 85, 83]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [83., 86, 84]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [84., 87, 85]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [85., 88, 86]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [86., 89, 87]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [87., 90, 88]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [88., 91, 89]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [89., 92, 90]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [90., 93, 91]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [91., 94, 92]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [92., 95, 93]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [93., 96, 94]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [94., 97, 95]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [95., 98, 96]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [96., 99, 97]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [97., 100, 98]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [98., 101, 99]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [99., 102, 100]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SET log_comment = 'vpc_correctness_c01_assert';
SYSTEM FLUSH LOGS;
SELECT 'C01 hits, misses, result cache hits',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_correctness_c01'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vector_plan_cache_run_state);
SELECT 'C01 entries', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';

-- C02: one warm-up query and 20 cached instances -> 1 miss, 20 hits, 1 entry.
SET log_comment = 'vpc_correctness_c02';
SYSTEM DROP VECTOR QUERY PLAN CACHE;
SELECT id, round(L2Distance(vector, [40., 43, 41]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [41., 44, 42]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [42., 45, 43]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [43., 46, 44]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [44., 47, 45]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [45., 48, 46]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [46., 49, 47]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [47., 50, 48]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [48., 51, 49]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [49., 52, 50]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [50., 53, 51]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [51., 54, 52]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [52., 55, 53]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [53., 56, 54]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [54., 57, 55]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [55., 58, 56]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [56., 59, 57]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [57., 60, 58]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [58., 61, 59]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [59., 62, 60]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [60., 63, 61]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SET log_comment = 'vpc_correctness_c02_assert';
SYSTEM FLUSH LOGS;
SELECT 'C02 hits, misses, result cache hits',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_correctness_c02'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vector_plan_cache_run_state);
SELECT 'C02 entries', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';

-- C03: three syntactic forms of the same vector literal are three cache keys.
-- Each form is executed twice: the first execution is a miss and prints the
-- result, the second one is a hit and prints the identical result.
SET log_comment = 'vpc_correctness_c03';
SYSTEM DROP VECTOR QUERY PLAN CACHE;
SELECT id, round(L2Distance(vector, [7., 10, 8]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5;
SELECT id, round(L2Distance(vector, [7., 10, 8]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5;
SELECT id, round(L2Distance(vector, CAST([7., 10, 8] AS Array(Float32))), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5;
SELECT id, round(L2Distance(vector, CAST([7., 10, 8] AS Array(Float32))), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5;
SELECT id, round(L2Distance(vector, [7., 10, 8]::Array(Float32)), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5;
SELECT id, round(L2Distance(vector, [7., 10, 8]::Array(Float32)), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5;
SET log_comment = 'vpc_correctness_c03_assert';
SYSTEM FLUSH LOGS;
SELECT 'C03 hits, misses, result cache hits',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_correctness_c03'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vector_plan_cache_run_state);
SELECT 'C03 entries', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';

-- C04: replacing the constant after the template has been cached. The first
-- query is a miss, the following 10 are hits, and each prints its own Top-K.
SET log_comment = 'vpc_correctness_c04';
SYSTEM DROP VECTOR QUERY PLAN CACHE;
SELECT id, round(L2Distance(vector, [10., 13, 11]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 3;
SELECT id, round(L2Distance(vector, [11., 14, 12]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 3;
SELECT id, round(L2Distance(vector, [12., 15, 13]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 3;
SELECT id, round(L2Distance(vector, [13., 16, 14]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 3;
SELECT id, round(L2Distance(vector, [14., 17, 15]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 3;
SELECT id, round(L2Distance(vector, [15., 18, 16]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 3;
SELECT id, round(L2Distance(vector, [16., 19, 17]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 3;
SELECT id, round(L2Distance(vector, [17., 20, 18]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 3;
SELECT id, round(L2Distance(vector, [18., 21, 19]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 3;
SELECT id, round(L2Distance(vector, [19., 22, 20]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 3;
SELECT id, round(L2Distance(vector, [20., 23, 21]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 3;
SET log_comment = 'vpc_correctness_c04_assert';
SYSTEM FLUSH LOGS;
SELECT 'C04 hits, misses, result cache hits',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_correctness_c04'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vector_plan_cache_run_state);
SELECT 'C04 entries', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';

-- C05: the same query with the cache disabled, in plan-only mode and in the
-- default AST+plan mode produces identical results. The disabled run emits no
-- cache events. In plan-only mode the first execution is a miss and the second
-- one a hit. In AST+plan mode the first execution after the cache drop emits
-- no plan-cache event at all (the AST-cache miss skips the plan lookup) and
-- the second one is a hit, so the case has 1 miss and 2 hits in total.
SET log_comment = 'vpc_correctness_c05';
SYSTEM DROP VECTOR QUERY PLAN CACHE;
SET vector_query_plan_cache = 0;
SELECT id, round(L2Distance(vector, [50., 53, 51]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5;
SET vector_query_plan_cache = 1;
SELECT id, round(L2Distance(vector, [50., 53, 51]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5;
SELECT id, round(L2Distance(vector, [50., 53, 51]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5;
SET vector_only_cache_query_plan = 0;
SYSTEM DROP VECTOR QUERY PLAN CACHE;
SELECT id, round(L2Distance(vector, [50., 53, 51]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5;
SELECT id, round(L2Distance(vector, [50., 53, 51]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5;
SET vector_only_cache_query_plan = 1;
SET log_comment = 'vpc_correctness_c05_assert';
SYSTEM FLUSH LOGS;
SELECT 'C05 hits, misses, result cache hits',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_correctness_c05'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vector_plan_cache_run_state);
SELECT 'C05 entries', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';

-- C06: LIMIT is part of the cache key, so LIMIT 3 and LIMIT 5 are two
-- independent entries, each with one miss and one hit.
SET log_comment = 'vpc_correctness_c06';
SYSTEM DROP VECTOR QUERY PLAN CACHE;
SELECT id, round(L2Distance(vector, [60., 63, 61]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 3;
SELECT id, round(L2Distance(vector, [60., 63, 61]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 3;
SELECT id, round(L2Distance(vector, [60., 63, 61]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5;
SELECT id, round(L2Distance(vector, [60., 63, 61]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5;
SET log_comment = 'vpc_correctness_c06_assert';
SYSTEM FLUSH LOGS;
SELECT 'C06 hits, misses, result cache hits',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_correctness_c06'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vector_plan_cache_run_state);
SELECT 'C06 entries', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';

-- C08: a subquery is not cached: both executions miss and no entry is created.
SET log_comment = 'vpc_correctness_c08';
SYSTEM DROP VECTOR QUERY PLAN CACHE;
SELECT * FROM (SELECT id, round(L2Distance(vector, [80., 83, 81]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5) FORMAT Null;
SELECT * FROM (SELECT id, round(L2Distance(vector, [80., 83, 81]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5) FORMAT Null;
SET log_comment = 'vpc_correctness_c08_assert';
SYSTEM FLUSH LOGS;
SELECT 'C08 hits, misses, result cache hits',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_correctness_c08'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vector_plan_cache_run_state);
SELECT 'C08 entries', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';

-- C09: 20 distinct LIMIT values create 20 entries with the default cache
-- capacity (no silent eviction). The revisit below keeps the same
-- `log_comment`, because it is part of the cache key; its assertion reports
-- cumulative counts, so the second pair of executions must be 2 hits on top
-- of the 20 cold misses.
SET log_comment = 'vpc_correctness_c09';
SYSTEM DROP VECTOR QUERY PLAN CACHE;
SELECT id, round(L2Distance(vector, [70., 73, 71]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 1 FORMAT Null;
SELECT id, round(L2Distance(vector, [70., 73, 71]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 2 FORMAT Null;
SELECT id, round(L2Distance(vector, [70., 73, 71]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 3 FORMAT Null;
SELECT id, round(L2Distance(vector, [70., 73, 71]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 4 FORMAT Null;
SELECT id, round(L2Distance(vector, [70., 73, 71]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [70., 73, 71]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 6 FORMAT Null;
SELECT id, round(L2Distance(vector, [70., 73, 71]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 7 FORMAT Null;
SELECT id, round(L2Distance(vector, [70., 73, 71]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 8 FORMAT Null;
SELECT id, round(L2Distance(vector, [70., 73, 71]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 9 FORMAT Null;
SELECT id, round(L2Distance(vector, [70., 73, 71]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 10 FORMAT Null;
SELECT id, round(L2Distance(vector, [70., 73, 71]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 11 FORMAT Null;
SELECT id, round(L2Distance(vector, [70., 73, 71]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 12 FORMAT Null;
SELECT id, round(L2Distance(vector, [70., 73, 71]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 13 FORMAT Null;
SELECT id, round(L2Distance(vector, [70., 73, 71]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 14 FORMAT Null;
SELECT id, round(L2Distance(vector, [70., 73, 71]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 15 FORMAT Null;
SELECT id, round(L2Distance(vector, [70., 73, 71]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 16 FORMAT Null;
SELECT id, round(L2Distance(vector, [70., 73, 71]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 17 FORMAT Null;
SELECT id, round(L2Distance(vector, [70., 73, 71]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 18 FORMAT Null;
SELECT id, round(L2Distance(vector, [70., 73, 71]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 19 FORMAT Null;
SELECT id, round(L2Distance(vector, [70., 73, 71]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 20 FORMAT Null;
SET log_comment = 'vpc_correctness_c09_assert';
SYSTEM FLUSH LOGS;
SELECT 'C09 hits, misses, result cache hits',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_correctness_c09'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vector_plan_cache_run_state);
SELECT 'C09 entries', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_correctness_c09';
SELECT id, round(L2Distance(vector, [70., 73, 71]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 1 FORMAT Null;
SELECT id, round(L2Distance(vector, [70., 73, 71]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 20 FORMAT Null;
SET log_comment = 'vpc_correctness_c09_assert';
SYSTEM FLUSH LOGS;
SELECT 'C09 revisit, cumulative hits, misses, result cache hits',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_correctness_c09'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vector_plan_cache_run_state);

-- C10: the same single-template stability with `cosineDistance`: 100 distinct
-- query vectors normalize to one template -> 1 miss, 99 hits, 1 entry.
SET log_comment = 'vpc_correctness_c10';
SYSTEM DROP VECTOR QUERY PLAN CACHE;
SELECT id, round(cosineDistance(vector, [0., 3, 1]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [1., 4, 2]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [2., 5, 3]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [3., 6, 4]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [4., 7, 5]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [5., 8, 6]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [6., 9, 7]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [7., 10, 8]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [8., 11, 9]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [9., 12, 10]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [10., 13, 11]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [11., 14, 12]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [12., 15, 13]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [13., 16, 14]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [14., 17, 15]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [15., 18, 16]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [16., 19, 17]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [17., 20, 18]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [18., 21, 19]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [19., 22, 20]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [20., 23, 21]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [21., 24, 22]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [22., 25, 23]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [23., 26, 24]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [24., 27, 25]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [25., 28, 26]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [26., 29, 27]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [27., 30, 28]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [28., 31, 29]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [29., 32, 30]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [30., 33, 31]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [31., 34, 32]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [32., 35, 33]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [33., 36, 34]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [34., 37, 35]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [35., 38, 36]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [36., 39, 37]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [37., 40, 38]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [38., 41, 39]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [39., 42, 40]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [40., 43, 41]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [41., 44, 42]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [42., 45, 43]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [43., 46, 44]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [44., 47, 45]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [45., 48, 46]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [46., 49, 47]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [47., 50, 48]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [48., 51, 49]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [49., 52, 50]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [50., 53, 51]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [51., 54, 52]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [52., 55, 53]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [53., 56, 54]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [54., 57, 55]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [55., 58, 56]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [56., 59, 57]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [57., 60, 58]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [58., 61, 59]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [59., 62, 60]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [60., 63, 61]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [61., 64, 62]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [62., 65, 63]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [63., 66, 64]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [64., 67, 65]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [65., 68, 66]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [66., 69, 67]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [67., 70, 68]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [68., 71, 69]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [69., 72, 70]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [70., 73, 71]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [71., 74, 72]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [72., 75, 73]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [73., 76, 74]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [74., 77, 75]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [75., 78, 76]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [76., 79, 77]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [77., 80, 78]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [78., 81, 79]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [79., 82, 80]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [80., 83, 81]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [81., 84, 82]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [82., 85, 83]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [83., 86, 84]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [84., 87, 85]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [85., 88, 86]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [86., 89, 87]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [87., 90, 88]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [88., 91, 89]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [89., 92, 90]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [90., 93, 91]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [91., 94, 92]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [92., 95, 93]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [93., 96, 94]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [94., 97, 95]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [95., 98, 96]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [96., 99, 97]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [97., 100, 98]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [98., 101, 99]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [99., 102, 100]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SET log_comment = 'vpc_correctness_c10_assert';
SYSTEM FLUSH LOGS;
SELECT 'C10 hits, misses, result cache hits',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_correctness_c10'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vector_plan_cache_run_state);
SELECT 'C10 entries', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';

-- C11: the distance function is part of the normalized template, so the same
-- vector literal queried with `L2Distance` and with `cosineDistance` creates
-- two independent entries, each with one miss and one hit.
SET log_comment = 'vpc_correctness_c11';
SYSTEM DROP VECTOR QUERY PLAN CACHE;
SELECT id, round(L2Distance(vector, [60., 63, 61]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(L2Distance(vector, [60., 63, 61]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [60., 63, 61]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [60., 63, 61]), 4) AS d FROM vector_plan_cache_correctness ORDER BY d, id LIMIT 5 FORMAT Null;
SET log_comment = 'vpc_correctness_c11_assert';
SYSTEM FLUSH LOGS;
SELECT 'C11 hits, misses, result cache hits',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_correctness_c11'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vector_plan_cache_run_state);
SELECT 'C11 entries', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';

DROP TABLE vector_plan_cache_correctness;
DROP TABLE vector_plan_cache_run_state;

SYSTEM DROP VECTOR QUERY PLAN CACHE;
