-- The hashed set method hashes the keys of up to 256 rows ahead of their lookups. Results must not depend on where
-- a block ends relative to those rows: runs of equal keys, the pattern a, b, a, NULL maps, masks, sub-range states.

SET max_threads = 1;

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t_sorted;
DROP TABLE IF EXISTS t_idx;
DROP TABLE IF EXISTS t_keys;

CREATE TABLE t (k UInt64, u UUID, u2 UUID, s String, ns Nullable(String), a Array(UInt8), z Nullable(UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t
SELECT k, reinterpretAsUUID(sipHash128(k)), reinterpretAsUUID(sipHash128(k + 1)), toString(k % 13),
    if(k % 5 = 0, NULL, toString(k % 13)), [toUInt8(k % 7), toUInt8(k % 3)], if(number % 6 = 1, NULL, 0)
FROM (SELECT number, multiIf(number % 6 < 3, intDiv(number, 6), number % 6 = 3, 1, number % 6 = 4, 2, 1) AS k FROM numbers(3000))
SETTINGS max_insert_threads = 1;

CREATE TABLE t_sorted (k UInt64, u UUID, u2 UUID, s String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_sorted SELECT k, u, u2, s FROM t;

CREATE TABLE t_idx (k UInt64, s String, INDEX i (k, s) TYPE set(0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 300, index_granularity_bytes = '10Mi';
INSERT INTO t_idx SELECT k, s FROM t SETTINGS max_insert_threads = 1;

-- Not a MergeTree table, so `force_data_skipping_indices` checks only the read of `t_idx`.
CREATE TABLE t_keys ENGINE = Memory AS SELECT k, s FROM t WHERE k % 5 = 1;

-- { echoOn }

SELECT count() FROM (SELECT DISTINCT k, u, u2 FROM t);
SELECT count() FROM t WHERE (k, u) IN (SELECT (k, u) FROM t WHERE k % 2 = 0);
SELECT count() FROM (SELECT DISTINCT k, s FROM t);
SELECT count() FROM t WHERE (k, s) IN (SELECT k, s FROM t WHERE k % 3 = 0);
SELECT count() FROM (SELECT k, s FROM t INTERSECT DISTINCT SELECT k, s FROM t WHERE k % 2 = 0);
SELECT count() FROM (SELECT k, s FROM t EXCEPT DISTINCT SELECT k, s FROM t WHERE k % 2 = 0);
SELECT count() FROM (SELECT k, s FROM t INTERSECT ALL SELECT k, s FROM t WHERE k % 2 = 0);
SELECT count() FROM (SELECT k, s FROM t EXCEPT ALL SELECT k, s FROM t WHERE k % 2 = 0);
SELECT count() FROM t WHERE (ns, k) IN (SELECT ns, k FROM t WHERE k % 4 = 0) SETTINGS transform_null_in = 0;
SELECT count() FROM t WHERE (ns, k) IN (SELECT ns, k FROM t WHERE k % 4 = 0) SETTINGS transform_null_in = 1;
SELECT count() FROM t WHERE (z, k, s) IN (SELECT z, k, s FROM t) SETTINGS transform_null_in = 0;
SELECT count() FROM (SELECT DISTINCT ns FROM t);
SELECT count() FROM (SELECT DISTINCT a FROM t);
SELECT count() FROM (SELECT DISTINCT toLowCardinality(s) FROM t);
SELECT count() FROM (SELECT DISTINCT k, s, u FROM t_sorted) SETTINGS optimize_distinct_in_order = 1;
SELECT count() FROM (SELECT DISTINCT k, u, u2, k * 7 FROM t_sorted) SETTINGS optimize_distinct_in_order = 1;
SELECT count() FROM t_idx WHERE (k, s) IN (SELECT k, s FROM t_keys) SETTINGS force_data_skipping_indices = 'i';

SET max_block_size = 7;

SELECT count() FROM (SELECT DISTINCT k, u, u2 FROM t);
SELECT count() FROM t WHERE (k, u) IN (SELECT (k, u) FROM t WHERE k % 2 = 0);
SELECT count() FROM (SELECT DISTINCT k, s FROM t);
SELECT count() FROM t WHERE (k, s) IN (SELECT k, s FROM t WHERE k % 3 = 0);
SELECT count() FROM (SELECT k, s FROM t INTERSECT DISTINCT SELECT k, s FROM t WHERE k % 2 = 0);
SELECT count() FROM (SELECT k, s FROM t EXCEPT DISTINCT SELECT k, s FROM t WHERE k % 2 = 0);
SELECT count() FROM (SELECT k, s FROM t INTERSECT ALL SELECT k, s FROM t WHERE k % 2 = 0);
SELECT count() FROM (SELECT k, s FROM t EXCEPT ALL SELECT k, s FROM t WHERE k % 2 = 0);
SELECT count() FROM t WHERE (ns, k) IN (SELECT ns, k FROM t WHERE k % 4 = 0) SETTINGS transform_null_in = 0;
SELECT count() FROM t WHERE (ns, k) IN (SELECT ns, k FROM t WHERE k % 4 = 0) SETTINGS transform_null_in = 1;
SELECT count() FROM t WHERE (z, k, s) IN (SELECT z, k, s FROM t) SETTINGS transform_null_in = 0;
SELECT count() FROM (SELECT DISTINCT ns FROM t);
SELECT count() FROM (SELECT DISTINCT a FROM t);
SELECT count() FROM (SELECT DISTINCT toLowCardinality(s) FROM t);
SELECT count() FROM (SELECT DISTINCT k, s, u FROM t_sorted) SETTINGS optimize_distinct_in_order = 1;
SELECT count() FROM (SELECT DISTINCT k, u, u2, k * 7 FROM t_sorted) SETTINGS optimize_distinct_in_order = 1;
SELECT count() FROM t_idx WHERE (k, s) IN (SELECT k, s FROM t_keys) SETTINGS force_data_skipping_indices = 'i';

SET max_block_size = 257;

SELECT count() FROM (SELECT DISTINCT k, u, u2 FROM t);
SELECT count() FROM t WHERE (k, u) IN (SELECT (k, u) FROM t WHERE k % 2 = 0);
SELECT count() FROM (SELECT DISTINCT k, s FROM t);
SELECT count() FROM t WHERE (k, s) IN (SELECT k, s FROM t WHERE k % 3 = 0);
SELECT count() FROM (SELECT k, s FROM t INTERSECT DISTINCT SELECT k, s FROM t WHERE k % 2 = 0);
SELECT count() FROM (SELECT k, s FROM t EXCEPT DISTINCT SELECT k, s FROM t WHERE k % 2 = 0);
SELECT count() FROM (SELECT k, s FROM t INTERSECT ALL SELECT k, s FROM t WHERE k % 2 = 0);
SELECT count() FROM (SELECT k, s FROM t EXCEPT ALL SELECT k, s FROM t WHERE k % 2 = 0);
SELECT count() FROM t WHERE (ns, k) IN (SELECT ns, k FROM t WHERE k % 4 = 0) SETTINGS transform_null_in = 0;
SELECT count() FROM t WHERE (ns, k) IN (SELECT ns, k FROM t WHERE k % 4 = 0) SETTINGS transform_null_in = 1;
SELECT count() FROM t WHERE (z, k, s) IN (SELECT z, k, s FROM t) SETTINGS transform_null_in = 0;
SELECT count() FROM (SELECT DISTINCT ns FROM t);
SELECT count() FROM (SELECT DISTINCT a FROM t);
SELECT count() FROM (SELECT DISTINCT toLowCardinality(s) FROM t);
SELECT count() FROM (SELECT DISTINCT k, s, u FROM t_sorted) SETTINGS optimize_distinct_in_order = 1;
SELECT count() FROM (SELECT DISTINCT k, u, u2, k * 7 FROM t_sorted) SETTINGS optimize_distinct_in_order = 1;
SELECT count() FROM t_idx WHERE (k, s) IN (SELECT k, s FROM t_keys) SETTINGS force_data_skipping_indices = 'i';

-- { echoOff }

DROP TABLE t;
DROP TABLE t_sorted;
DROP TABLE t_idx;
DROP TABLE t_keys;
