-- count(x) of a LowCardinality(Nullable) argument reads only its null map. The plain Nullable copies are the oracle.

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_count_lc_nullable;
CREATE TABLE t_count_lc_nullable (k UInt32, s LowCardinality(Nullable(String)), p Nullable(String),
    f LowCardinality(Nullable(FixedString(4))), pf Nullable(FixedString(4)), u LowCardinality(Nullable(UInt32)), pu Nullable(UInt32))
ENGINE = MergeTree ORDER BY k;
INSERT INTO t_count_lc_nullable SELECT number, vs, vs, vf, vf, vu, vu FROM (SELECT number,
    if(number % 7 = 0, NULL, toString(number % 1000)) AS vs,
    if(number % 5 = 0, NULL, toFixedString(toString(number % 100), 4)) AS vf,
    if(number % 3 = 0, NULL, toUInt32(number % 200)) AS vu FROM numbers(100000));

SELECT 'without key';
SELECT count(s), count(p), count(f), count(pf), count(u), count(pu) FROM t_count_lc_nullable;
SELECT count(s) FROM t_count_lc_nullable WHERE k > 1000000;

SELECT 'keyed';
SELECT sum(cs), countIf(cs != cp), sum(cf), countIf(cf != cpf), count() FROM (SELECT k % 1000 AS g, count(s) AS cs,
    count(p) AS cp, count(f) AS cf, count(pf) AS cpf FROM t_count_lc_nullable GROUP BY g);

SELECT 'counted column is the key';
SELECT sum(cs), countIf(cs != cp), countIf(g IS NULL AND cs = 0), count() FROM (SELECT s AS g,
    count(s) AS cs, count(p) AS cp FROM t_count_lc_nullable GROUP BY g);

SELECT '-State';
SELECT finalizeAggregation(countState(s)) FROM t_count_lc_nullable;
SELECT countMerge(st), sum(finalizeAggregation(st)) FROM (SELECT k % 1000 AS g, countState(s) AS st FROM t_count_lc_nullable GROUP BY g);

SELECT 'shared with other aggregates';
SELECT sum(cs), countIf(cs != cp), countIf(ms != mp), countIf(ci != cip) FROM (SELECT k % 1000 AS g, count(s) AS cs,
    count(p) AS cp, max(s) AS ms, max(p) AS mp, countIf(s, k % 2 = 0) AS ci, countIf(p, k % 2 = 0) AS cip FROM t_count_lc_nullable GROUP BY g);

SELECT 'adaptive staging';
SELECT sum(cs), countIf(cs != cp), sum(cf), countIf(cf != cpf), count() FROM (SELECT k AS g, count(s) AS cs, count(p) AS cp,
    count(f) AS cf, count(pf) AS cpf FROM t_count_lc_nullable GROUP BY g)
SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 0, adaptive_aggregator_freeze_threshold_bytes = 0,
    adaptive_aggregator_disable_thaw = 1, collect_hash_table_stats_during_aggregation = 0, optimize_aggregation_in_order = 0,
    max_threads = 4, max_block_size = 8192;
SELECT sum(cs), countIf(cs != cp), countIf(ms != mp) FROM (SELECT k AS g, count(s) AS cs, count(p) AS cp, max(s) AS ms, max(p) AS mp
    FROM t_count_lc_nullable GROUP BY g)
SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 0, adaptive_aggregator_freeze_threshold_bytes = 0,
    adaptive_aggregator_disable_thaw = 1, collect_hash_table_stats_during_aggregation = 0, optimize_aggregation_in_order = 0,
    max_threads = 4, max_block_size = 8192;
SELECT countMerge(st) FROM (SELECT k AS g, countState(s) AS st FROM t_count_lc_nullable GROUP BY g)
SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 0, adaptive_aggregator_freeze_threshold_bytes = 0,
    adaptive_aggregator_disable_thaw = 1, collect_hash_table_stats_during_aggregation = 0, optimize_aggregation_in_order = 0,
    max_threads = 4, max_block_size = 8192;
SELECT sum(cs), countIf(cs != cp), sum(cf), countIf(cf != cpf), count() FROM (SELECT k AS g, count(s) AS cs, count(p) AS cp,
    count(f) AS cf, count(pf) AS cpf FROM t_count_lc_nullable GROUP BY g)
SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 0, adaptive_aggregator_freeze_threshold_bytes = 0,
    adaptive_aggregator_disable_thaw = 1, collect_hash_table_stats_during_aggregation = 0, optimize_aggregation_in_order = 0,
    max_threads = 4, max_block_size = 8192, max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0;

SELECT 'two-level and external';
SELECT sum(cs), countIf(cs != cp), count() FROM (SELECT k AS g, count(s) AS cs, count(p) AS cp FROM t_count_lc_nullable GROUP BY g)
SETTINGS enable_adaptive_aggregator = 0, optimize_aggregation_in_order = 0, group_by_two_level_threshold = 1,
    group_by_two_level_threshold_bytes = 1, max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0;

SELECT 'JIT';
SELECT sum(cs), countIf(cs != cp), sum(cu), countIf(cu != cpu), sum(sk) FROM (SELECT k % 1000 AS g, count(s) AS cs, count(p) AS cp,
    count(u) AS cu, count(pu) AS cpu, sum(k) AS sk FROM t_count_lc_nullable GROUP BY g)
SETTINGS compile_aggregate_expressions = 1, min_count_to_compile_aggregate_expression = 0;
SELECT sum(cs), countIf(cs != cp), sum(cu), countIf(cu != cpu), sum(sk) FROM (SELECT k AS g, count(s) AS cs, count(p) AS cp,
    count(u) AS cu, count(pu) AS cpu, sum(k) AS sk FROM t_count_lc_nullable GROUP BY g)
SETTINGS compile_aggregate_expressions = 1, min_count_to_compile_aggregate_expression = 0,
    enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 0, adaptive_aggregator_freeze_threshold_bytes = 0,
    adaptive_aggregator_disable_thaw = 1, collect_hash_table_stats_during_aggregation = 0, optimize_aggregation_in_order = 0,
    max_threads = 4, max_block_size = 8192;

SELECT 'in order';
SELECT sum(cs), countIf(cs != cp), count() FROM (SELECT k AS g, count(s) AS cs, count(p) AS cp FROM t_count_lc_nullable GROUP BY g)
SETTINGS optimize_aggregation_in_order = 1;

SELECT 'index widths';
SELECT 'UInt8', min(dumpColumnStructure(x) LIKE '%LowCardinality(size = 1000, UInt8(size = 1000)%'), count(x), count(n)
FROM (SELECT if(number % 7 = 0, NULL, toString(number % 100)) AS n, toLowCardinality(n) AS x FROM numbers(1000)) SETTINGS max_block_size = 1000;
SELECT 'UInt8 keyed', sum(c1), countIf(c1 != c2) FROM (SELECT number % 10 AS g, count(x) AS c1, count(n) AS c2
    FROM (SELECT number, if(number % 7 = 0, NULL, toString(number % 100)) AS n, toLowCardinality(n) AS x FROM numbers(1000)) GROUP BY g)
SETTINGS max_block_size = 1000;
SELECT 'UInt16', min(dumpColumnStructure(x) LIKE '%LowCardinality(size = 10000, UInt16(size = 10000)%'), count(x), count(n)
FROM (SELECT if(number % 7 = 0, NULL, toString(number % 1000)) AS n, toLowCardinality(n) AS x FROM numbers(10000)) SETTINGS max_block_size = 10000;
SELECT 'UInt16 keyed', sum(c1), countIf(c1 != c2) FROM (SELECT number % 10 AS g, count(x) AS c1, count(n) AS c2
    FROM (SELECT number, if(number % 7 = 0, NULL, toString(number % 1000)) AS n, toLowCardinality(n) AS x FROM numbers(10000)) GROUP BY g)
SETTINGS max_block_size = 10000;
SELECT 'UInt32', min(dumpColumnStructure(x) LIKE '%LowCardinality(size = 100000, UInt32(size = 100000)%'), count(x), count(n)
FROM (SELECT if(number % 7 = 0, NULL, toString(number)) AS n, toLowCardinality(n) AS x FROM numbers(100000)) SETTINGS max_block_size = 100000;
SELECT 'UInt32 keyed', sum(c1), countIf(c1 != c2) FROM (SELECT number % 10 AS g, count(x) AS c1, count(n) AS c2
    FROM (SELECT number, if(number % 7 = 0, NULL, toString(number)) AS n, toLowCardinality(n) AS x FROM numbers(100000)) GROUP BY g)
SETTINGS max_block_size = 100000;

SELECT 'constant argument';
SELECT count(toLowCardinality(toNullable('a'))), count(toLowCardinality(CAST(NULL, 'Nullable(String)')))
FROM numbers(10) SETTINGS optimize_normalize_count_variants = 0;
SELECT number % 2 AS g, count(toLowCardinality(toNullable('a'))), count(toLowCardinality(CAST(NULL, 'Nullable(String)')))
FROM numbers(10) GROUP BY g ORDER BY g SETTINGS optimize_normalize_count_variants = 0;

SELECT 'all NULL and never NULL';
SELECT count(x), count(y) FROM (SELECT toLowCardinality(if(number > 1000000, 'a', NULL)) AS x,
    toLowCardinality(toNullable(toString(number % 3))) AS y FROM numbers(1000));

SELECT 'grouping sets';
SELECT sum(cs), countIf(cs != cp), count() FROM (SELECT k % 3 AS a, k % 5 AS b, count(s) AS cs, count(p) AS cp
    FROM t_count_lc_nullable GROUP BY GROUPING SETS ((a), (b)));

DROP TABLE t_count_lc_nullable;
