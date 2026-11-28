-- Tags: no-fasttest
-- no-fasttest: uniqTheta is not in the fast test build

-- Disable force_primary_key_reverse_order: groupUniqArray(N) depends on the row order, which must match the insertion order of the `Memory` table
SET force_primary_key_reverse_order = 0;

-- Without GROUP BY, the functions that ignore repeated values add each value of a LowCardinality argument once:
-- the results must be the same as for the same rows in ordinary columns.

-- count_distinct_optimization would rewrite a lone uniqExact to a GROUP BY.
SET allow_suspicious_low_cardinality_types = 1, max_threads = 1, count_distinct_optimization = 0;

DROP VIEW IF EXISTS agg_lc;
DROP VIEW IF EXISTS agg_mem;
DROP VIEW IF EXISTS agg_plain;
DROP TABLE IF EXISTS t_lc;
DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_plain;
DROP TABLE IF EXISTS t_runs;

-- Only t_lc is a MergeTree table: it is the only one that needs a part-wide dictionary.
CREATE TABLE t_plain (id UInt64, s String, n Nullable(String), u UInt32, fs FixedString(16), iv IntervalSecond) ENGINE = Memory;
CREATE TABLE t_lc (id UInt64, s LowCardinality(String), n LowCardinality(Nullable(String)), u LowCardinality(UInt32),
                   fs LowCardinality(FixedString(16)), iv LowCardinality(IntervalSecond))
    ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1000, index_granularity_bytes = '10Mi';
CREATE TABLE t_mem AS t_lc ENGINE = Memory;

-- s: 3000 values, n: NULL in every third row, else 700 values, u: 1000, fs: 300, iv: 500; values in scrambled order.
INSERT INTO t_plain SELECT number, concat('s', toString(number * 7919 % 3000)), if(number % 3 = 0, NULL, concat('n', toString(number * 7919 % 700))),
    number * 7919 % 1000 * 3, toFixedString(concat('fs', toString(number * 7919 % 300)), 16), toIntervalSecond(number * 7919 % 500)
    FROM numbers(30000) SETTINGS max_insert_threads = 1;
INSERT INTO t_lc SELECT * FROM t_plain SETTINGS max_insert_threads = 1;
-- Three blocks with their own dictionaries and different values of `s`.
INSERT INTO t_mem SELECT * FROM t_plain WHERE id * 7919 % 3000 < 1000 SETTINGS max_insert_threads = 1;
INSERT INTO t_mem SELECT * FROM t_plain WHERE id * 7919 % 3000 >= 1000 AND id * 7919 % 3000 < 2000 SETTINGS max_insert_threads = 1;
INSERT INTO t_mem SELECT * FROM t_plain WHERE id * 7919 % 3000 >= 2000 SETTINGS max_insert_threads = 1;

CREATE VIEW agg_lc AS SELECT
    uniq(s), uniqExact(s), uniqCombined(s), uniqCombined64(s), uniqHLL12(s), uniqTheta(s), min(s), max(s), arraySort(groupUniqArray(s)), count(DISTINCT s),
    uniq(n), uniqExact(n), uniqCombined(n), uniqCombined64(n), uniqHLL12(n), uniqTheta(n), min(n), max(n), arraySort(groupUniqArray(n)), count(DISTINCT n),
    uniq(u), uniqExact(u), uniqCombined(u), uniqCombined64(u), uniqHLL12(u), uniqTheta(u), min(u), max(u), arraySort(groupUniqArray(u)), count(DISTINCT u),
    uniq(fs), uniqExact(fs), uniqCombined(fs), uniqCombined64(fs), uniqHLL12(fs), uniqTheta(fs), min(fs), max(fs), arraySort(groupUniqArray(fs)), count(DISTINCT fs),
    uniq(iv), uniqExact(iv), uniqCombined(iv), uniqCombined64(iv), uniqHLL12(iv), uniqTheta(iv), min(iv), max(iv), arraySort(groupUniqArray(iv)), count(DISTINCT iv)
    FROM t_lc WHERE id % {m:UInt64} = {r:UInt64};
CREATE VIEW agg_mem AS SELECT
    uniq(s), uniqExact(s), uniqCombined(s), uniqCombined64(s), uniqHLL12(s), uniqTheta(s), min(s), max(s), arraySort(groupUniqArray(s)), count(DISTINCT s),
    uniq(n), uniqExact(n), uniqCombined(n), uniqCombined64(n), uniqHLL12(n), uniqTheta(n), min(n), max(n), arraySort(groupUniqArray(n)), count(DISTINCT n),
    uniq(u), uniqExact(u), uniqCombined(u), uniqCombined64(u), uniqHLL12(u), uniqTheta(u), min(u), max(u), arraySort(groupUniqArray(u)), count(DISTINCT u),
    uniq(fs), uniqExact(fs), uniqCombined(fs), uniqCombined64(fs), uniqHLL12(fs), uniqTheta(fs), min(fs), max(fs), arraySort(groupUniqArray(fs)), count(DISTINCT fs),
    uniq(iv), uniqExact(iv), uniqCombined(iv), uniqCombined64(iv), uniqHLL12(iv), uniqTheta(iv), min(iv), max(iv), arraySort(groupUniqArray(iv)), count(DISTINCT iv)
    FROM t_mem WHERE id % {m:UInt64} = {r:UInt64};
CREATE VIEW agg_plain AS SELECT
    uniq(s), uniqExact(s), uniqCombined(s), uniqCombined64(s), uniqHLL12(s), uniqTheta(s), min(s), max(s), arraySort(groupUniqArray(s)), count(DISTINCT s),
    uniq(n), uniqExact(n), uniqCombined(n), uniqCombined64(n), uniqHLL12(n), uniqTheta(n), min(n), max(n), arraySort(groupUniqArray(n)), count(DISTINCT n),
    uniq(u), uniqExact(u), uniqCombined(u), uniqCombined64(u), uniqHLL12(u), uniqTheta(u), min(u), max(u), arraySort(groupUniqArray(u)), count(DISTINCT u),
    uniq(fs), uniqExact(fs), uniqCombined(fs), uniqCombined64(fs), uniqHLL12(fs), uniqTheta(fs), min(fs), max(fs), arraySort(groupUniqArray(fs)), count(DISTINCT fs),
    uniq(iv), uniqExact(iv), uniqCombined(iv), uniqCombined64(iv), uniqHLL12(iv), uniqTheta(iv), min(iv), max(iv), arraySort(groupUniqArray(iv)), count(DISTINCT iv)
    FROM t_plain WHERE id % {m:UInt64} = {r:UInt64};

-- t_lc is one part, read in blocks of 1000 rows with the part-wide dictionary or as one block; `% 3` uses a third of `s`.
-- toString: `n` has only NULL with `% 3`.
SELECT 'lc', 1000, 'all', toString((SELECT * FROM agg_lc(m = 1, r = 0))) = toString((SELECT * FROM agg_plain(m = 1, r = 0))) SETTINGS max_block_size = 1000;
SELECT 'lc', 65505, 'all', toString((SELECT * FROM agg_lc(m = 1, r = 0))) = toString((SELECT * FROM agg_plain(m = 1, r = 0))) SETTINGS max_block_size = 65505;
SELECT 'lc', 65505, '% 3', toString((SELECT * FROM agg_lc(m = 3, r = 0))) = toString((SELECT * FROM agg_plain(m = 3, r = 0))) SETTINGS max_block_size = 65505;
SELECT 'mem', 'all', toString((SELECT * FROM agg_mem(m = 1, r = 0))) = toString((SELECT * FROM agg_plain(m = 1, r = 0))) SETTINGS max_block_size = 65505;
SELECT 'mem', '% 3', toString((SELECT * FROM agg_mem(m = 3, r = 0))) = toString((SELECT * FROM agg_plain(m = 3, r = 0))) SETTINGS max_block_size = 65505;

-- The values themselves.
SELECT 's', uniqExact(s), min(s), max(s), length(groupUniqArray(s)) FROM t_mem;
SELECT 'n', uniqExact(n), min(n), max(n), length(groupUniqArray(n)) FROM t_mem;
SELECT 'n, only NULL', uniqExact(n), min(n), max(n), length(groupUniqArray(n)) FROM t_mem WHERE id % 3 = 0;
SELECT 'u', uniqExact(u), min(u), max(u), length(groupUniqArray(u)) FROM t_mem;
SELECT 'fs', uniqExact(fs), min(fs), max(fs), length(groupUniqArray(fs)) FROM t_mem;
SELECT 'iv', uniqExact(iv), min(iv), max(iv), length(groupUniqArray(iv)) FROM t_mem;

-- groupUniqArray(N) keeps the first N distinct values in row order: the used values must be added in the order of their first occurrence.
SELECT 'limit', 65505, toString((SELECT groupUniqArray(10)(s), groupUniqArray(10)(n), groupUniqArray(10)(fs) FROM t_lc))
    = toString((SELECT groupUniqArray(10)(s), groupUniqArray(10)(n), groupUniqArray(10)(fs) FROM t_plain))
    SETTINGS max_block_size = 65505, enable_parallel_replicas = 0;
SELECT 'limit', 1000, toString((SELECT groupUniqArray(10)(fs) FROM t_lc)) = toString((SELECT groupUniqArray(10)(fs) FROM t_plain))
    SETTINGS max_block_size = 1000, enable_parallel_replicas = 0;

-- A full exact set of uniqCombined turns into HLL on any insert, also of a repeated value: a LowCardinality column must give the same result.
SELECT 'uniqCombined 4096',
    (SELECT uniqCombined(s), uniqCombined64(s) FROM (SELECT toLowCardinality(toString(number % 4096)) AS s FROM numbers(12288)))
    = (SELECT uniqCombined(s), uniqCombined64(s) FROM (SELECT toString(number % 4096) AS s FROM numbers(12288)))
    SETTINGS max_block_size = 65505;

-- One block where the default value or NULL first appears after every other value, or never. Each line compares
-- several functions (the Aggregator), then shows a lone uniqExact (the radix partitioned uniqExact).
SELECT 'default last',
    (SELECT uniqExact(s), min(s), max(s), groupUniqArray(101)(s), groupUniqArray(100)(s) FROM (SELECT toLowCardinality(if(number = 99999, '', concat('k', toString(number % 100)))) AS s FROM numbers(100000)))
    = (SELECT uniqExact(s), min(s), max(s), groupUniqArray(101)(s), groupUniqArray(100)(s) FROM (SELECT if(number = 99999, '', concat('k', toString(number % 100))) AS s FROM numbers(100000))),
    (SELECT uniqExact(s) FROM (SELECT toLowCardinality(if(number = 99999, '', concat('k', toString(number % 100)))) AS s FROM numbers(100000)))
    SETTINGS max_block_size = 100000, optimize_uniq_exact_radix_partitioning = 1;
SELECT 'default unused',
    (SELECT uniqExact(s), min(s), max(s), groupUniqArray(101)(s) FROM (SELECT toLowCardinality(concat('k', toString(number % 100))) AS s FROM numbers(100000)))
    = (SELECT uniqExact(s), min(s), max(s), groupUniqArray(101)(s) FROM (SELECT concat('k', toString(number % 100)) AS s FROM numbers(100000))),
    (SELECT uniqExact(s) FROM (SELECT toLowCardinality(concat('k', toString(number % 100))) AS s FROM numbers(100000)))
    SETTINGS max_block_size = 100000, optimize_uniq_exact_radix_partitioning = 1;
SELECT 'NULL and default last',
    toString((SELECT uniqExact(s), min(s), max(s), groupUniqArray(101)(s) FROM (SELECT toLowCardinality(if(number = 99998, NULL, if(number = 99999, '', concat('k', toString(number % 100))))) AS s FROM numbers(100000))))
    = toString((SELECT uniqExact(s), min(s), max(s), groupUniqArray(101)(s) FROM (SELECT if(number = 99998, NULL, if(number = 99999, '', concat('k', toString(number % 100)))) AS s FROM numbers(100000)))),
    (SELECT uniqExact(s) FROM (SELECT toLowCardinality(if(number = 99998, NULL, if(number = 99999, '', concat('k', toString(number % 100))))) AS s FROM numbers(100000)))
    SETTINGS max_block_size = 100000, optimize_uniq_exact_radix_partitioning = 1;
SELECT 'NULL first, default last',
    toString((SELECT uniqExact(s), min(s), max(s), groupUniqArray(101)(s) FROM (SELECT toLowCardinality(if(number % 3 = 0, NULL, if(number = 99998, '', concat('k', toString(number % 100))))) AS s FROM numbers(100000))))
    = toString((SELECT uniqExact(s), min(s), max(s), groupUniqArray(101)(s) FROM (SELECT if(number % 3 = 0, NULL, if(number = 99998, '', concat('k', toString(number % 100)))) AS s FROM numbers(100000)))),
    (SELECT uniqExact(s) FROM (SELECT toLowCardinality(if(number % 3 = 0, NULL, if(number = 99998, '', concat('k', toString(number % 100))))) AS s FROM numbers(100000)))
    SETTINGS max_block_size = 100000, optimize_uniq_exact_radix_partitioning = 1;
SELECT 'only NULL and default',
    toString((SELECT uniqExact(s), min(s), max(s), groupUniqArray(s) FROM (SELECT toLowCardinality(if(number % 2 = 0, NULL, '')) AS s FROM numbers(100000))))
    = toString((SELECT uniqExact(s), min(s), max(s), groupUniqArray(s) FROM (SELECT if(number % 2 = 0, NULL, '') AS s FROM numbers(100000)))),
    (SELECT uniqExact(s) FROM (SELECT toLowCardinality(if(number % 2 = 0, NULL, '')) AS s FROM numbers(100000)))
    SETTINGS max_block_size = 100000, optimize_uniq_exact_radix_partitioning = 1;
SELECT 'only default',
    (SELECT uniqExact(s), min(s), max(s), groupUniqArray(s) FROM (SELECT toLowCardinality(repeat('x', number % 1)) AS s FROM numbers(100000)))
    = (SELECT uniqExact(s), min(s), max(s), groupUniqArray(s) FROM (SELECT repeat('x', number % 1) AS s FROM numbers(100000))),
    (SELECT uniqExact(s) FROM (SELECT toLowCardinality(repeat('x', number % 1)) AS s FROM numbers(100000)))
    SETTINGS max_block_size = 100000, optimize_uniq_exact_radix_partitioning = 1;

-- A lone uniqExact of blocks that use a third of their dictionaries.
SELECT 'radix % 3', (SELECT uniqExact(s) FROM t_mem WHERE id % 3 = 0) = (SELECT uniqExact(s) FROM t_plain WHERE id % 3 = 0),
    (SELECT uniqExact(fs) FROM t_mem WHERE id % 3 = 0) = (SELECT uniqExact(fs) FROM t_plain WHERE id % 3 = 0)
    SETTINGS optimize_uniq_exact_radix_partitioning = 1;

-- Sorted runs in one block, s of 64 rows, n of 50 rows with NULL and the default: `id < 9990` uses about half of the dictionaries,
-- and its last 6 rows start a run of s.
CREATE TABLE t_runs (id UInt64, s LowCardinality(String), n LowCardinality(Nullable(String)), sp String, np Nullable(String)) ENGINE = Memory;
INSERT INTO t_runs SELECT number, sp, np, sp, np FROM (SELECT number, concat('r', toString(intDiv(number, 64) % 300)) AS sp,
    multiIf(intDiv(number, 50) % 7 = 3, NULL, intDiv(number, 50) % 11 = 5, '', concat('r', toString(intDiv(number, 50) % 300))) AS np
    FROM numbers(30000)) SETTINGS max_insert_threads = 1, max_block_size = 30000;
SELECT 'runs', 9990,
    toString((SELECT uniqExact(s), min(s), max(s), groupUniqArray(10)(s), uniqExact(n), min(n), max(n), groupUniqArray(10)(n) FROM t_runs WHERE id < 9990))
    = toString((SELECT uniqExact(sp), min(sp), max(sp), groupUniqArray(10)(sp), uniqExact(np), min(np), max(np), groupUniqArray(10)(np) FROM t_runs WHERE id < 9990)),
    (SELECT uniqExact(s) FROM t_runs WHERE id < 9990) = (SELECT uniqExact(sp) FROM t_runs WHERE id < 9990)
    SETTINGS enable_parallel_replicas = 0, optimize_uniq_exact_radix_partitioning = 1;
SELECT 'runs', 30000,
    toString((SELECT uniqExact(s), min(s), max(s), groupUniqArray(10)(s), uniqExact(n), min(n), max(n), groupUniqArray(10)(n) FROM t_runs WHERE id < 30000))
    = toString((SELECT uniqExact(sp), min(sp), max(sp), groupUniqArray(10)(sp), uniqExact(np), min(np), max(np), groupUniqArray(10)(np) FROM t_runs WHERE id < 30000)),
    (SELECT uniqExact(s) FROM t_runs WHERE id < 30000) = (SELECT uniqExact(sp) FROM t_runs WHERE id < 30000)
    SETTINGS enable_parallel_replicas = 0, optimize_uniq_exact_radix_partitioning = 1;

-- Combinators and GROUP BY keep adding every row.
SELECT 'if', (SELECT uniqExactIf(s, id % 2 = 0) FROM t_mem) = (SELECT uniqExactIf(s, id % 2 = 0) FROM t_plain);
SELECT 'state', (SELECT finalizeAggregation(uniqExactState(s)) FROM t_mem) = (SELECT finalizeAggregation(uniqExactState(s)) FROM t_plain);
SELECT 'group by', (SELECT groupArray((k, c)) FROM (SELECT id % 3 AS k, uniqExact(s) AS c FROM t_mem GROUP BY k ORDER BY k))
    = (SELECT groupArray((k, c)) FROM (SELECT id % 3 AS k, uniqExact(s) AS c FROM t_plain GROUP BY k ORDER BY k));

DROP VIEW agg_lc;
DROP VIEW agg_mem;
DROP VIEW agg_plain;
DROP TABLE t_lc;
DROP TABLE t_mem;
DROP TABLE t_plain;
DROP TABLE t_runs;
