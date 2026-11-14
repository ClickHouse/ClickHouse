-- Rows of a SummingMergeTree that are groups of one row are copied in bulk on INSERT, FINAL and merges.
-- Each result is compared with a GROUP BY over the same rows.

SET optimize_on_insert = 1;

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t_merged_rows;
DROP TABLE IF EXISTS t_merged_bytes;
DROP TABLE IF EXISTS merged_expected;

CREATE TABLE src (k UInt64, a Int64, f Float64, f32 Float32, dec Decimal(18, 4), s String, lc LowCardinality(String)) ENGINE = Memory;

-- 100000 unsorted unique keys: the lowest ~100 have only zero sums, every 7th row has a -0.0 sum (`1 / f < 0` finds -0.0).
INSERT INTO src SELECT k, if(z, 0, toInt64(number % 13) - 6), if(z, 0., if(number % 7 = 0, -0., (number % 1000) / 4)),
    if(z, 0, number % 100), if(z, 0, toDecimal64(number % 1000, 4)), toString(k % 1000), toString(k % 10)
FROM (SELECT number, intHash64(number) AS k, k < 18446744073709551 AS z FROM numbers(100000));

-- The 1000 highest keys get two more rows each.
INSERT INTO src SELECT k, 1, 0.25, 1, 1, s, lc FROM (SELECT k, s, lc FROM src ORDER BY k DESC LIMIT 1000) ARRAY JOIN [1, 2] AS rep;

CREATE TABLE t (k UInt64, a Int64, f Float64, f32 Float32, dec Decimal(18, 4), s String, lc LowCardinality(String))
ENGINE = SummingMergeTree ORDER BY k;

-- INSERT: one block of 102000 rows.
INSERT INTO t SELECT * FROM src SETTINGS max_block_size = 200000, min_insert_block_size_rows = 200000, min_insert_block_size_bytes = 0,
    max_insert_block_size = 200000, max_threads = 1, max_insert_threads = 1;

SELECT 'insert', count() FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active;
SELECT 'insert', count(), countIf(f = 0 AND 1 / f < 0) FROM t;
SELECT 'insert', count() FROM (
    SELECT * FROM t
    EXCEPT
    SELECT k, sumWithOverflow(a), sumWithOverflow(f), sumWithOverflow(f32), sumWithOverflow(dec), any(s), any(lc) FROM src GROUP BY k
    HAVING NOT (sumWithOverflow(a) = 0 AND sumWithOverflow(f) = 0 AND sumWithOverflow(f32) = 0 AND sumWithOverflow(dec) = 0));
SELECT 'insert', count() FROM (
    SELECT k, sumWithOverflow(a), sumWithOverflow(f), sumWithOverflow(f32), sumWithOverflow(dec), any(s), any(lc) FROM src GROUP BY k
    HAVING NOT (sumWithOverflow(a) = 0 AND sumWithOverflow(f) = 0 AND sumWithOverflow(f32) = 0 AND sumWithOverflow(dec) = 0)
    EXCEPT
    SELECT * FROM t);

-- FINAL: the merged part gets rows with only zero sums and -0.0 sums, then three small parts of new rows,
-- rows cancelling existing ones and new keys.
ALTER TABLE t UPDATE a = if(k % 11 = 0, 0, a), f = multiIf(k % 11 = 0, 0., k % 7 = 0, -0., f), f32 = if(k % 11 = 0, 0, f32),
    dec = if(k % 11 = 0, 0, dec) WHERE 1 SETTINGS mutations_sync = 2;
ALTER TABLE t MODIFY SETTING max_bytes_to_merge_at_max_space_in_pool = 1, max_bytes_to_merge_at_min_space_in_pool = 1;

INSERT INTO t SELECT k, -a, -f, -f32, -dec, s, lc FROM t WHERE k % 101 = 0;
INSERT INTO t SELECT k, 1, 0.5, 1, 1, s, lc FROM t WHERE k % 103 = 1;
INSERT INTO t SELECT intHash64(number + 1000000) AS k, 1, 0.25, 2, 3, toString(k % 1000), toString(k % 10) FROM numbers(1000);

SELECT 'final', count(), countIf(f = 0 AND 1 / f < 0) FROM t FINAL;
SELECT 'final', count() FROM (
    SELECT * FROM t FINAL
    EXCEPT
    SELECT k, sumWithOverflow(a), sumWithOverflow(f), sumWithOverflow(f32), sumWithOverflow(dec), any(s), any(lc) FROM t GROUP BY k);
SELECT 'final', count() FROM (
    SELECT k, sumWithOverflow(a), sumWithOverflow(f), sumWithOverflow(f32), sumWithOverflow(dec), any(s), any(lc) FROM t GROUP BY k
    EXCEPT
    SELECT * FROM t FINAL);
SELECT 'final', count() FROM (
    SELECT * FROM t FINAL
    EXCEPT
    SELECT k, sumWithOverflow(a), sumWithOverflow(f), sumWithOverflow(f32), sumWithOverflow(dec), any(s), any(lc) FROM t GROUP BY k)
SETTINGS max_block_size = 1000;
SELECT 'final', count() FROM (
    SELECT k, sumWithOverflow(a), sumWithOverflow(f), sumWithOverflow(f32), sumWithOverflow(dec), any(s), any(lc) FROM t GROUP BY k
    EXCEPT
    SELECT * FROM t FINAL)
SETTINGS max_block_size = 1000;

-- Merges remove the rows with only zero sums.
CREATE TABLE merged_expected (k UInt64, a Int64, f Float64, f32 Float32, dec Decimal(18, 4), s String, lc LowCardinality(String)) ENGINE = Memory;
INSERT INTO merged_expected
SELECT k, sumWithOverflow(a), sumWithOverflow(f), sumWithOverflow(f32), sumWithOverflow(dec), any(s), any(lc) FROM t GROUP BY k
HAVING NOT (sumWithOverflow(a) = 0 AND sumWithOverflow(f) = 0 AND sumWithOverflow(f32) = 0 AND sumWithOverflow(dec) = 0);

CREATE TABLE t_merged_rows AS t;
ALTER TABLE t_merged_rows MODIFY SETTING merge_max_block_size = 1000;
ALTER TABLE t_merged_rows ATTACH PARTITION tuple() FROM t;
OPTIMIZE TABLE t_merged_rows FINAL;

CREATE TABLE t_merged_bytes AS t;
ALTER TABLE t_merged_bytes MODIFY SETTING merge_max_block_size_bytes = 65536;
ALTER TABLE t_merged_bytes ATTACH PARTITION tuple() FROM t;
OPTIMIZE TABLE t_merged_bytes FINAL;

SELECT 'merge rows', count(), countIf(f = 0 AND 1 / f < 0) FROM t_merged_rows;
SELECT 'merge rows', count() FROM (SELECT * FROM t_merged_rows EXCEPT SELECT * FROM merged_expected);
SELECT 'merge rows', count() FROM (SELECT * FROM merged_expected EXCEPT SELECT * FROM t_merged_rows);
SELECT 'merge bytes', count(), countIf(f = 0 AND 1 / f < 0) FROM t_merged_bytes;
SELECT 'merge bytes', count() FROM (SELECT * FROM t_merged_bytes EXCEPT SELECT * FROM merged_expected);
SELECT 'merge bytes', count() FROM (SELECT * FROM merged_expected EXCEPT SELECT * FROM t_merged_bytes);

DROP TABLE src;
DROP TABLE t;
DROP TABLE t_merged_rows;
DROP TABLE t_merged_bytes;
DROP TABLE merged_expected;
