-- The radix partitioned `uniqExact` drops the rows equal to their previous row before it routes the values. Each value
-- here is one run of rows, so a wrongly dropped first row of a run changes the count. The values that differ only in
-- their high bytes check that the rows are compared in their whole width.

SET optimize_uniq_exact_radix_partitioning = 1, optimize_injective_functions_inside_uniq = 0, collect_hash_table_stats_during_aggregation = 0;

-- Runs of 1 to 4 rows.
SELECT 'UInt8', uniqExact(toUInt8(n)) FROM (SELECT arrayJoin(arrayWithConstant(1 + intHash32(number) % 4, number)) AS n FROM numbers(256));
SELECT 'UInt16', uniqExact(toUInt16(n)) FROM (SELECT arrayJoin(arrayWithConstant(1 + intHash32(number) % 4, number)) AS n FROM numbers(20000));
SELECT 'UInt16 high byte', uniqExact(bitRotateLeft(toUInt16(n), 8)) FROM (SELECT arrayJoin(arrayWithConstant(1 + intHash32(number) % 4, number)) AS n FROM numbers(20000));
SELECT 'UInt32', uniqExact(toUInt32(n)) FROM (SELECT arrayJoin(arrayWithConstant(1 + intHash32(number) % 4, number)) AS n FROM numbers(20000));
SELECT 'UInt32 high half', uniqExact(bitRotateLeft(toUInt32(n), 16)) FROM (SELECT arrayJoin(arrayWithConstant(1 + intHash32(number) % 4, number)) AS n FROM numbers(20000));
SELECT 'UInt64', uniqExact(n) FROM (SELECT arrayJoin(arrayWithConstant(1 + intHash32(number) % 4, number)) AS n FROM numbers(20000));
SELECT 'UInt64 high half', uniqExact(bitRotateLeft(n, 32)) FROM (SELECT arrayJoin(arrayWithConstant(1 + intHash32(number) % 4, number)) AS n FROM numbers(20000));
SELECT 'Tuple of 4 bytes', uniqExact((toUInt16(n % 256), toUInt16(intDiv(n, 256)))) FROM (SELECT arrayJoin(arrayWithConstant(1 + intHash32(number) % 4, number)) AS n FROM numbers(20000));
SELECT 'Tuple of 5 bytes', uniqExact((toUInt32(n), toUInt8(n % 3))) FROM (SELECT arrayJoin(arrayWithConstant(1 + intHash32(number) % 4, number)) AS n FROM numbers(20000));

-- Runs of 1 to 70 rows: groups of rows that all repeat the previous row are skipped.
SELECT 'UInt8 long', uniqExact(toUInt8(n)) FROM (SELECT arrayJoin(arrayWithConstant(1 + intHash32(number) % 70, number)) AS n FROM numbers(256));
SELECT 'UInt16 long', uniqExact(toUInt16(n)) FROM (SELECT arrayJoin(arrayWithConstant(1 + intHash32(number) % 70, number)) AS n FROM numbers(20000));
SELECT 'UInt16 high byte long', uniqExact(bitRotateLeft(toUInt16(n), 8)) FROM (SELECT arrayJoin(arrayWithConstant(1 + intHash32(number) % 70, number)) AS n FROM numbers(20000));
SELECT 'UInt32 long', uniqExact(toUInt32(n)) FROM (SELECT arrayJoin(arrayWithConstant(1 + intHash32(number) % 70, number)) AS n FROM numbers(20000));
SELECT 'UInt32 high half long', uniqExact(bitRotateLeft(toUInt32(n), 16)) FROM (SELECT arrayJoin(arrayWithConstant(1 + intHash32(number) % 70, number)) AS n FROM numbers(20000));
SELECT 'UInt64 long', uniqExact(n) FROM (SELECT arrayJoin(arrayWithConstant(1 + intHash32(number) % 70, number)) AS n FROM numbers(20000));
SELECT 'UInt64 high half long', uniqExact(bitRotateLeft(n, 32)) FROM (SELECT arrayJoin(arrayWithConstant(1 + intHash32(number) % 70, number)) AS n FROM numbers(20000));
SELECT 'Tuple of 4 bytes long', uniqExact((toUInt16(n % 256), toUInt16(intDiv(n, 256)))) FROM (SELECT arrayJoin(arrayWithConstant(1 + intHash32(number) % 70, number)) AS n FROM numbers(20000));
SELECT 'Tuple of 5 bytes long', uniqExact((toUInt32(n), toUInt8(n % 3))) FROM (SELECT arrayJoin(arrayWithConstant(1 + intHash32(number) % 70, number)) AS n FROM numbers(20000));

-- Blocks shorter than a group of rows, and batches that end at other rows.
SELECT 'UInt32 small blocks', uniqExact(toUInt32(n)) FROM (SELECT arrayJoin(arrayWithConstant(1 + intHash32(number) % 4, number)) AS n FROM numbers(20000)) SETTINGS max_block_size = 7;
SELECT 'UInt64 small blocks', uniqExact(n) FROM (SELECT arrayJoin(arrayWithConstant(1 + intHash32(number) % 4, number)) AS n FROM numbers(20000)) SETTINGS max_block_size = 7;
SELECT 'UInt64 threads', uniqExact(n) FROM (SELECT arrayJoin(arrayWithConstant(1 + intHash32(number) % 4, number)) AS n FROM numbers_mt(200000)) SETTINGS max_threads = 4;

SELECT 'radix', count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(n) FROM (SELECT arrayJoin(arrayWithConstant(1 + intHash32(number) % 4, number)) AS n FROM numbers(20000))) WHERE explain LIKE '%RadixUniqExactRouteTransform%';
