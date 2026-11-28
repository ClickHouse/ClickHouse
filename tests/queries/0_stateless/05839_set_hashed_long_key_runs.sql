-- Random settings limits: min_bytes_for_wide_part=(10485760, None)
-- 10485760 is this setting's default: a drawn 0 makes `t_runs` a `Wide` part, which reads 7 rows at a time about 30 times slower.

-- Hashed sets: runs of equal keys longer than the prepared rows, where one key column changes at a time and only in its
-- upper bytes.

SET max_threads = 1;

DROP TABLE IF EXISTS t_runs;
CREATE TABLE t_runs (a UInt16, b UInt32, c UInt64, d UInt128, e UInt256, f UInt8, g FixedString(3))
ENGINE = MergeTree ORDER BY a;
INSERT INTO t_runs SELECT
    toUInt16(intDiv(number, 4999) % 200 * 256), toUInt32(intDiv(number, 5003) % 200 * 65536),
    bitShiftLeft(toUInt64(intDiv(number, 5009)), 32), bitShiftLeft(toUInt128(intDiv(number, 5011)), 64),
    bitShiftLeft(toUInt256(intDiv(number, 5021)), 128), toUInt8(intDiv(number, 5023)),
    toFixedString(leftPad(toString(intDiv(number, 5001)), 3, '0'), 3)
FROM numbers(100000) SETTINGS max_insert_threads = 1;
OPTIMIZE TABLE t_runs FINAL;

-- { echoOn }

SET max_block_size = 65409;

SELECT count() FROM (SELECT DISTINCT a, b, c, d, e, f, g FROM t_runs) SETTINGS optimize_distinct_in_order = 0;
SELECT count() FROM (SELECT DISTINCT a, b, c, d, e, f, g FROM t_runs) SETTINGS optimize_distinct_in_order = 1;
SELECT count() FROM t_runs WHERE (a, b, c, d, e, f, g) IN (SELECT a, b, c, d, e, f, g FROM t_runs WHERE bitShiftRight(c, 32) % 2 = 0);
SELECT count() FROM (SELECT a, b, c, d, e, f, g FROM t_runs INTERSECT DISTINCT SELECT a, b, c, d, e, f, g FROM t_runs WHERE bitShiftRight(c, 32) % 2 = 0);
SELECT count() FROM (SELECT a, b, c, d, e, f, g FROM t_runs EXCEPT DISTINCT SELECT a, b, c, d, e, f, g FROM t_runs WHERE bitShiftRight(c, 32) % 2 = 0);

SET max_block_size = 7;

SELECT count() FROM (SELECT DISTINCT a, b, c, d, e, f, g FROM t_runs) SETTINGS optimize_distinct_in_order = 0;
SELECT count() FROM (SELECT DISTINCT a, b, c, d, e, f, g FROM t_runs) SETTINGS optimize_distinct_in_order = 1;
SELECT count() FROM t_runs WHERE (a, b, c, d, e, f, g) IN (SELECT a, b, c, d, e, f, g FROM t_runs WHERE bitShiftRight(c, 32) % 2 = 0);
SELECT count() FROM (SELECT a, b, c, d, e, f, g FROM t_runs INTERSECT DISTINCT SELECT a, b, c, d, e, f, g FROM t_runs WHERE bitShiftRight(c, 32) % 2 = 0);
SELECT count() FROM (SELECT a, b, c, d, e, f, g FROM t_runs EXCEPT DISTINCT SELECT a, b, c, d, e, f, g FROM t_runs WHERE bitShiftRight(c, 32) % 2 = 0);

SET max_block_size = 1000;

SELECT count() FROM (SELECT DISTINCT a, b, c, d, e, f, g FROM t_runs) SETTINGS optimize_distinct_in_order = 0;
SELECT count() FROM (SELECT DISTINCT a, b, c, d, e, f, g FROM t_runs) SETTINGS optimize_distinct_in_order = 1;
SELECT count() FROM t_runs WHERE (a, b, c, d, e, f, g) IN (SELECT a, b, c, d, e, f, g FROM t_runs WHERE bitShiftRight(c, 32) % 2 = 0);
SELECT count() FROM (SELECT a, b, c, d, e, f, g FROM t_runs INTERSECT DISTINCT SELECT a, b, c, d, e, f, g FROM t_runs WHERE bitShiftRight(c, 32) % 2 = 0);
SELECT count() FROM (SELECT a, b, c, d, e, f, g FROM t_runs EXCEPT DISTINCT SELECT a, b, c, d, e, f, g FROM t_runs WHERE bitShiftRight(c, 32) % 2 = 0);

-- { echoOff }

DROP TABLE t_runs;
