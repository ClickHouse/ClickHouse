-- Tags: no-fasttest
-- no-fasttest: uniqTheta is not in the fast test build

-- Without GROUP BY, the functions that ignore repeated values add each value of a LowCardinality argument once:
-- the results must be the same as for the same rows in ordinary columns.

SET allow_suspicious_low_cardinality_types = 1, max_threads = 1;

DROP VIEW IF EXISTS agg_lc;
DROP VIEW IF EXISTS agg_mem;
DROP VIEW IF EXISTS agg_plain;
DROP TABLE IF EXISTS t_lc;
DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_plain;

CREATE TABLE t_lc (id UInt64, s LowCardinality(String), n LowCardinality(Nullable(String)), u LowCardinality(UInt32),
                   fs LowCardinality(FixedString(16)), iv LowCardinality(IntervalSecond))
    ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 100;
CREATE TABLE t_mem AS t_lc ENGINE = Memory;
CREATE TABLE t_plain (id UInt64, s String, n Nullable(String), u UInt32, fs FixedString(16), iv IntervalSecond)
    ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 100;

-- s: 3000 values, n: NULL in every third row, else 700 values, u: 1000, fs: 300, iv: 500; values in scrambled order.
INSERT INTO t_lc SELECT number, concat('s', toString(number * 7919 % 3000)), if(number % 3 = 0, NULL, concat('n', toString(number * 7919 % 700))),
    number * 7919 % 1000 * 3, toFixedString(concat('fs', toString(number * 7919 % 300)), 16), toIntervalSecond(number * 7919 % 500)
    FROM numbers(30000);
INSERT INTO t_plain SELECT * FROM t_lc;
-- Three blocks with their own dictionaries and different values of `s`.
INSERT INTO t_mem SELECT * FROM t_plain WHERE id * 7919 % 3000 < 1000;
INSERT INTO t_mem SELECT * FROM t_plain WHERE id * 7919 % 3000 >= 1000 AND id * 7919 % 3000 < 2000;
INSERT INTO t_mem SELECT * FROM t_plain WHERE id * 7919 % 3000 >= 2000;
OPTIMIZE TABLE t_lc FINAL;
OPTIMIZE TABLE t_plain FINAL;

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

-- t_lc is read in blocks of one granule (100 rows), of 1000 rows with the part-wide dictionary, or as one block.
-- toString: `n` has only NULL with `% 3`.
SELECT 'lc', 7, 'all', toString((SELECT * FROM agg_lc(m = 1, r = 0))) = toString((SELECT * FROM agg_plain(m = 1, r = 0))) SETTINGS max_block_size = 7;
SELECT 'lc', 7, '% 97', toString((SELECT * FROM agg_lc(m = 97, r = 3))) = toString((SELECT * FROM agg_plain(m = 97, r = 3))) SETTINGS max_block_size = 7;
SELECT 'lc', 7, '% 3', toString((SELECT * FROM agg_lc(m = 3, r = 0))) = toString((SELECT * FROM agg_plain(m = 3, r = 0))) SETTINGS max_block_size = 7;
SELECT 'lc', 1000, 'all', toString((SELECT * FROM agg_lc(m = 1, r = 0))) = toString((SELECT * FROM agg_plain(m = 1, r = 0))) SETTINGS max_block_size = 1000;
SELECT 'lc', 1000, '% 97', toString((SELECT * FROM agg_lc(m = 97, r = 3))) = toString((SELECT * FROM agg_plain(m = 97, r = 3))) SETTINGS max_block_size = 1000;
SELECT 'lc', 1000, '% 3', toString((SELECT * FROM agg_lc(m = 3, r = 0))) = toString((SELECT * FROM agg_plain(m = 3, r = 0))) SETTINGS max_block_size = 1000;
SELECT 'lc', 65505, 'all', toString((SELECT * FROM agg_lc(m = 1, r = 0))) = toString((SELECT * FROM agg_plain(m = 1, r = 0))) SETTINGS max_block_size = 65505;
SELECT 'lc', 65505, '% 97', toString((SELECT * FROM agg_lc(m = 97, r = 3))) = toString((SELECT * FROM agg_plain(m = 97, r = 3))) SETTINGS max_block_size = 65505;
SELECT 'lc', 65505, '% 3', toString((SELECT * FROM agg_lc(m = 3, r = 0))) = toString((SELECT * FROM agg_plain(m = 3, r = 0))) SETTINGS max_block_size = 65505;
SELECT 'mem', 7, 'all', toString((SELECT * FROM agg_mem(m = 1, r = 0))) = toString((SELECT * FROM agg_plain(m = 1, r = 0))) SETTINGS max_block_size = 7;
SELECT 'mem', 7, '% 97', toString((SELECT * FROM agg_mem(m = 97, r = 3))) = toString((SELECT * FROM agg_plain(m = 97, r = 3))) SETTINGS max_block_size = 7;
SELECT 'mem', 7, '% 3', toString((SELECT * FROM agg_mem(m = 3, r = 0))) = toString((SELECT * FROM agg_plain(m = 3, r = 0))) SETTINGS max_block_size = 7;
SELECT 'mem', 1000, 'all', toString((SELECT * FROM agg_mem(m = 1, r = 0))) = toString((SELECT * FROM agg_plain(m = 1, r = 0))) SETTINGS max_block_size = 1000;
SELECT 'mem', 1000, '% 97', toString((SELECT * FROM agg_mem(m = 97, r = 3))) = toString((SELECT * FROM agg_plain(m = 97, r = 3))) SETTINGS max_block_size = 1000;
SELECT 'mem', 1000, '% 3', toString((SELECT * FROM agg_mem(m = 3, r = 0))) = toString((SELECT * FROM agg_plain(m = 3, r = 0))) SETTINGS max_block_size = 1000;
SELECT 'mem', 65505, 'all', toString((SELECT * FROM agg_mem(m = 1, r = 0))) = toString((SELECT * FROM agg_plain(m = 1, r = 0))) SETTINGS max_block_size = 65505;
SELECT 'mem', 65505, '% 97', toString((SELECT * FROM agg_mem(m = 97, r = 3))) = toString((SELECT * FROM agg_plain(m = 97, r = 3))) SETTINGS max_block_size = 65505;
SELECT 'mem', 65505, '% 3', toString((SELECT * FROM agg_mem(m = 3, r = 0))) = toString((SELECT * FROM agg_plain(m = 3, r = 0))) SETTINGS max_block_size = 65505;

-- The values themselves.
SELECT 's', uniqExact(s), min(s), max(s), length(groupUniqArray(s)) FROM t_lc;
SELECT 'n', uniqExact(n), min(n), max(n), length(groupUniqArray(n)) FROM t_lc;
SELECT 'n, only NULL', uniqExact(n), min(n), max(n), length(groupUniqArray(n)) FROM t_lc WHERE id % 3 = 0;
SELECT 'u', uniqExact(u), min(u), max(u), length(groupUniqArray(u)) FROM t_lc;
SELECT 'fs', uniqExact(fs), min(fs), max(fs), length(groupUniqArray(fs)) FROM t_lc;
SELECT 'iv', uniqExact(iv), min(iv), max(iv), length(groupUniqArray(iv)) FROM t_lc;

-- Combinators and GROUP BY keep adding every row.
SELECT 'if', (SELECT uniqExactIf(s, id % 2 = 0) FROM t_lc) = (SELECT uniqExactIf(s, id % 2 = 0) FROM t_plain);
SELECT 'state', (SELECT finalizeAggregation(uniqExactState(s)) FROM t_lc) = (SELECT finalizeAggregation(uniqExactState(s)) FROM t_plain);
SELECT 'group by', (SELECT groupArray((k, c)) FROM (SELECT id % 3 AS k, uniqExact(s) AS c FROM t_lc GROUP BY k ORDER BY k))
    = (SELECT groupArray((k, c)) FROM (SELECT id % 3 AS k, uniqExact(s) AS c FROM t_plain GROUP BY k ORDER BY k));

DROP VIEW agg_lc;
DROP VIEW agg_mem;
DROP VIEW agg_plain;
DROP TABLE t_lc;
DROP TABLE t_mem;
DROP TABLE t_plain;
