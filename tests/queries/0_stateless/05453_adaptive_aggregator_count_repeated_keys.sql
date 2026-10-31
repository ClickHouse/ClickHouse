-- Exact results of a lone `count()` GROUP BY over keys that repeat on consecutive rows, compared with
-- `EXCEPT ALL` in both directions against the aggregation without the adaptive aggregator. The tables
-- freeze at 128 keys, so most rows of every cell are counted by the frozen tables or staged as new keys.
-- Every cell prints 0.

-- The free-memory cap of the thread count is off, so the queries keep their four streams on a loaded server.
SET max_threads = 4;
SET max_threads_min_free_memory_per_thread = 0;
SET max_block_size = 8192;
SET adaptive_aggregator_freeze_threshold = 128;
SET adaptive_aggregator_disable_thaw = 1;
SET group_by_two_level_threshold = 10000000;
SET group_by_two_level_threshold_bytes = 500000000;
SET collect_hash_table_stats_during_aggregation = 0;
SET enable_packed_string_keys_in_aggregation = 1;

-- Runs of 3 rows of a number key, every key new after the freeze; neighbouring keys differ only in their high bytes.
SELECT 'UInt64 runs';
SELECT count() FROM
(
    (SELECT * FROM (SELECT bitShiftLeft(intDiv(number, 3), 32) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0)
     EXCEPT ALL
     SELECT * FROM (SELECT bitShiftLeft(intDiv(number, 3), 32) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1))
    UNION ALL
    (SELECT * FROM (SELECT bitShiftLeft(intDiv(number, 3), 32) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1)
     EXCEPT ALL
     SELECT * FROM (SELECT bitShiftLeft(intDiv(number, 3), 32) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
);

-- Runs of 4 rows: three of four runs repeat one of 48 keys counted before the freeze.
SELECT 'UInt64 runs of frequent and new keys';
SELECT count() FROM
(
    (SELECT * FROM (WITH intDiv(number, 4) AS r SELECT bitShiftLeft(if(r % 4 = 3, 1000000 + r, r % 64), 32) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0)
     EXCEPT ALL
     SELECT * FROM (WITH intDiv(number, 4) AS r SELECT bitShiftLeft(if(r % 4 = 3, 1000000 + r, r % 64), 32) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1))
    UNION ALL
    (SELECT * FROM (WITH intDiv(number, 4) AS r SELECT bitShiftLeft(if(r % 4 = 3, 1000000 + r, r % 64), 32) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1)
     EXCEPT ALL
     SELECT * FROM (WITH intDiv(number, 4) AS r SELECT bitShiftLeft(if(r % 4 = 3, 1000000 + r, r % 64), 32) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
);

-- Runs of 3 rows of empty, short, medium and long strings; neighbouring runs of the same length.
SELECT 'String runs of varied lengths';
SELECT count() FROM
(
    (SELECT * FROM (WITH intDiv(number, 3) AS r SELECT multiIf(r % 5 = 0, '', intDiv(r, 1000) % 3 = 0, toString(r), intDiv(r, 1000) % 3 = 1, concat(repeat('a', 30), toString(r)), concat(repeat('b', 100), toString(r))) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0)
     EXCEPT ALL
     SELECT * FROM (WITH intDiv(number, 3) AS r SELECT multiIf(r % 5 = 0, '', intDiv(r, 1000) % 3 = 0, toString(r), intDiv(r, 1000) % 3 = 1, concat(repeat('a', 30), toString(r)), concat(repeat('b', 100), toString(r))) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1))
    UNION ALL
    (SELECT * FROM (WITH intDiv(number, 3) AS r SELECT multiIf(r % 5 = 0, '', intDiv(r, 1000) % 3 = 0, toString(r), intDiv(r, 1000) % 3 = 1, concat(repeat('a', 30), toString(r)), concat(repeat('b', 100), toString(r))) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1)
     EXCEPT ALL
     SELECT * FROM (WITH intDiv(number, 3) AS r SELECT multiIf(r % 5 = 0, '', intDiv(r, 1000) % 3 = 0, toString(r), intDiv(r, 1000) % 3 = 1, concat(repeat('a', 30), toString(r)), concat(repeat('b', 100), toString(r))) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
);

-- As the second cell, with string keys.
SELECT 'String runs of frequent and new keys';
SELECT count() FROM
(
    (SELECT * FROM (WITH intDiv(number, 4) AS r SELECT if(r % 4 = 3, concat('new', toString(r)), concat('frequent', toString(r % 64))) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0)
     EXCEPT ALL
     SELECT * FROM (WITH intDiv(number, 4) AS r SELECT if(r % 4 = 3, concat('new', toString(r)), concat('frequent', toString(r % 64))) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1))
    UNION ALL
    (SELECT * FROM (WITH intDiv(number, 4) AS r SELECT if(r % 4 = 3, concat('new', toString(r)), concat('frequent', toString(r % 64))) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1)
     EXCEPT ALL
     SELECT * FROM (WITH intDiv(number, 4) AS r SELECT if(r % 4 = 3, concat('new', toString(r)), concat('frequent', toString(r % 64))) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
);

SELECT 'String runs of varied lengths, unpacked string keys';
SELECT count() FROM
(
    (SELECT * FROM (WITH intDiv(number, 3) AS r SELECT multiIf(r % 5 = 0, '', intDiv(r, 1000) % 3 = 0, toString(r), intDiv(r, 1000) % 3 = 1, concat(repeat('a', 30), toString(r)), concat(repeat('b', 100), toString(r))) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0, enable_packed_string_keys_in_aggregation = 0)
     EXCEPT ALL
     SELECT * FROM (WITH intDiv(number, 3) AS r SELECT multiIf(r % 5 = 0, '', intDiv(r, 1000) % 3 = 0, toString(r), intDiv(r, 1000) % 3 = 1, concat(repeat('a', 30), toString(r)), concat(repeat('b', 100), toString(r))) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1, enable_packed_string_keys_in_aggregation = 0))
    UNION ALL
    (SELECT * FROM (WITH intDiv(number, 3) AS r SELECT multiIf(r % 5 = 0, '', intDiv(r, 1000) % 3 = 0, toString(r), intDiv(r, 1000) % 3 = 1, concat(repeat('a', 30), toString(r)), concat(repeat('b', 100), toString(r))) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1, enable_packed_string_keys_in_aggregation = 0)
     EXCEPT ALL
     SELECT * FROM (WITH intDiv(number, 3) AS r SELECT multiIf(r % 5 = 0, '', intDiv(r, 1000) % 3 = 0, toString(r), intDiv(r, 1000) % 3 = 1, concat(repeat('a', 30), toString(r)), concat(repeat('b', 100), toString(r))) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0, enable_packed_string_keys_in_aggregation = 0))
);

SELECT 'FixedString runs';
SELECT count() FROM
(
    (SELECT * FROM (SELECT toFixedString(toString(intDiv(number, 3)), 16) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0)
     EXCEPT ALL
     SELECT * FROM (SELECT toFixedString(toString(intDiv(number, 3)), 16) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1))
    UNION ALL
    (SELECT * FROM (SELECT toFixedString(toString(intDiv(number, 3)), 16) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1)
     EXCEPT ALL
     SELECT * FROM (SELECT toFixedString(toString(intDiv(number, 3)), 16) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
);

-- Runs of (NULL, b) next to runs of (0, b), and runs of (NULL, b) or (0, b) next to runs of the same first key with another b.
SELECT 'Nullable key pairs';
SELECT count() FROM
(
    (SELECT * FROM (WITH intDiv(number, 3) AS r SELECT if(r % 4 IN (0, 3), NULL, toUInt32(0)) AS a, toNullable(toUInt32(intDiv(r, 4) * 2 + (r % 4 >= 2))) AS b, count() AS c FROM numbers_mt(300000) GROUP BY a, b SETTINGS enable_adaptive_aggregator = 0)
     EXCEPT ALL
     SELECT * FROM (WITH intDiv(number, 3) AS r SELECT if(r % 4 IN (0, 3), NULL, toUInt32(0)) AS a, toNullable(toUInt32(intDiv(r, 4) * 2 + (r % 4 >= 2))) AS b, count() AS c FROM numbers_mt(300000) GROUP BY a, b SETTINGS enable_adaptive_aggregator = 1))
    UNION ALL
    (SELECT * FROM (WITH intDiv(number, 3) AS r SELECT if(r % 4 IN (0, 3), NULL, toUInt32(0)) AS a, toNullable(toUInt32(intDiv(r, 4) * 2 + (r % 4 >= 2))) AS b, count() AS c FROM numbers_mt(300000) GROUP BY a, b SETTINGS enable_adaptive_aggregator = 1)
     EXCEPT ALL
     SELECT * FROM (WITH intDiv(number, 3) AS r SELECT if(r % 4 IN (0, 3), NULL, toUInt32(0)) AS a, toNullable(toUInt32(intDiv(r, 4) * 2 + (r % 4 >= 2))) AS b, count() AS c FROM numbers_mt(300000) GROUP BY a, b SETTINGS enable_adaptive_aggregator = 0))
);

-- Neighbouring runs share the first key.
SELECT 'Three number keys';
SELECT count() FROM
(
    (SELECT * FROM (SELECT intDiv(number, 3000) AS a, intDiv(number, 3) AS b, intDiv(number, 3) % 7 AS d, count() AS c FROM numbers_mt(300000) GROUP BY a, b, d SETTINGS enable_adaptive_aggregator = 0)
     EXCEPT ALL
     SELECT * FROM (SELECT intDiv(number, 3000) AS a, intDiv(number, 3) AS b, intDiv(number, 3) % 7 AS d, count() AS c FROM numbers_mt(300000) GROUP BY a, b, d SETTINGS enable_adaptive_aggregator = 1))
    UNION ALL
    (SELECT * FROM (SELECT intDiv(number, 3000) AS a, intDiv(number, 3) AS b, intDiv(number, 3) % 7 AS d, count() AS c FROM numbers_mt(300000) GROUP BY a, b, d SETTINGS enable_adaptive_aggregator = 1)
     EXCEPT ALL
     SELECT * FROM (SELECT intDiv(number, 3000) AS a, intDiv(number, 3) AS b, intDiv(number, 3) % 7 AS d, count() AS c FROM numbers_mt(300000) GROUP BY a, b, d SETTINGS enable_adaptive_aggregator = 0))
);

-- Runs of 0, -0 and NaN next to each other and to new keys; compared by their bits.
SELECT 'Float64 runs with zeros of both signs and NaN';
SELECT count() FROM
(
    (SELECT reinterpretAsUInt64(k), c FROM (WITH intDiv(number, 3) AS r SELECT multiIf(r % 4 = 0, 0., r % 4 = 1, -0., r % 4 = 2, nan, r / 7) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0)
     EXCEPT ALL
     SELECT reinterpretAsUInt64(k), c FROM (WITH intDiv(number, 3) AS r SELECT multiIf(r % 4 = 0, 0., r % 4 = 1, -0., r % 4 = 2, nan, r / 7) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1))
    UNION ALL
    (SELECT reinterpretAsUInt64(k), c FROM (WITH intDiv(number, 3) AS r SELECT multiIf(r % 4 = 0, 0., r % 4 = 1, -0., r % 4 = 2, nan, r / 7) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1)
     EXCEPT ALL
     SELECT reinterpretAsUInt64(k), c FROM (WITH intDiv(number, 3) AS r SELECT multiIf(r % 4 = 0, 0., r % 4 = 1, -0., r % 4 = 2, nan, r / 7) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
);

-- Runs of 7 rows in blocks of 1000 rows; after row 100000 every other run repeats one of 16 new keys.
SELECT 'String runs across blocks with keys first seen after the freeze';
SELECT count() FROM
(
    (SELECT * FROM (WITH intDiv(number, 7) AS r SELECT if(number >= 100000 AND r % 2 = 0, concat('late', toString(intDiv(r, 2) % 16)), concat('unique', toString(r))) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0, max_block_size = 1000)
     EXCEPT ALL
     SELECT * FROM (WITH intDiv(number, 7) AS r SELECT if(number >= 100000 AND r % 2 = 0, concat('late', toString(intDiv(r, 2) % 16)), concat('unique', toString(r))) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1, max_block_size = 1000))
    UNION ALL
    (SELECT * FROM (WITH intDiv(number, 7) AS r SELECT if(number >= 100000 AND r % 2 = 0, concat('late', toString(intDiv(r, 2) % 16)), concat('unique', toString(r))) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1, max_block_size = 1000)
     EXCEPT ALL
     SELECT * FROM (WITH intDiv(number, 7) AS r SELECT if(number >= 100000 AND r % 2 = 0, concat('late', toString(intDiv(r, 2) % 16)), concat('unique', toString(r))) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0, max_block_size = 1000))
);

-- Every key repeats, never on consecutive rows.
SELECT 'Alternating keys';
SELECT count() FROM
(
    (SELECT * FROM (SELECT (number % 2) * 1000000 + intDiv(number, 4) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0)
     EXCEPT ALL
     SELECT * FROM (SELECT (number % 2) * 1000000 + intDiv(number, 4) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1))
    UNION ALL
    (SELECT * FROM (SELECT (number % 2) * 1000000 + intDiv(number, 4) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1)
     EXCEPT ALL
     SELECT * FROM (SELECT (number % 2) * 1000000 + intDiv(number, 4) AS k, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
);
