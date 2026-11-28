-- An INSERT block is split by the hash of its partition key, and the partitions are numbered in the order of their
-- first rows. One block of 510 rows with 17 partition keys, the rows of each key not adjacent.
-- The hashes of the split reach no output (partition IDs come from `MergeTreePartition::getID`, a different hash), so
-- they are pinned in `gtest_column_hash128.cpp`, and this test pins the split.

DROP TABLE IF EXISTS t_partition_split_every_key_type;

CREATE TABLE t_partition_split_every_key_type
(
    id UInt64,
    s String,
    ns Nullable(String),
    lc LowCardinality(String),
    fs FixedString(3),
    u UUID,
    d Date,
    dec Decimal64(2),
    i128 Int128,
    t Tuple(UInt8, String),
    a Array(UInt8)
)
ENGINE = MergeTree
PARTITION BY (s, ns, lc, fs, u, d, dec, i128, t, a)
ORDER BY id
SETTINGS allow_nullable_key = 1;

-- `k` is the partition key of the row: 0 to 6 in the rows 0 to 229, 7 to 12 in the rows 230 to 379, 13 to 16 in the
-- others. The first rows of the keys are not in the order of `k`, and the next row always has another key.
INSERT INTO t_partition_split_every_key_type
SELECT
    id,
    concat('s', toString(k % 4)),
    if(k % 5 = 0, NULL, concat('n', toString(k % 3))),
    concat('l', toString(k % 2)),
    concat('f', toString(k % 7), 'x'),
    reinterpret(toUInt128(k % 3) * 1000003, 'UUID'),
    toDate('2020-01-01') + k % 5,
    toDecimal64(toInt64(k % 6) - 3, 2) / 4,
    toInt128(k % 8) * toInt128('-100000000000000000000000'),
    (k % 3, concat('t', toString(k % 2))),
    arraySlice([1, 2, 3], 1, k % 4)
FROM (SELECT number AS id, multiIf(id < 230, 5 * id % 7, id < 380, 7 + 5 * id % 6, 13 + 3 * id % 4) AS k FROM numbers(510))
SETTINGS max_block_size = 1000, max_insert_block_size = 1000, min_insert_block_size_rows = 1000, min_insert_block_size_bytes = 0,
    max_insert_threads = 1, max_partitions_per_insert_block = 0;

-- One part per partition. The block numbers of the parts follow the first rows of their partitions.
SELECT p.partition, p.min_block_number, p.max_block_number, p.rows, r.first_id
FROM system.parts AS p
INNER JOIN
(
    SELECT _part AS name, min(id) AS first_id FROM t_partition_split_every_key_type GROUP BY name
) AS r USING (name)
WHERE p.database = currentDatabase() AND p.table = 't_partition_split_every_key_type' AND p.active
ORDER BY p.min_block_number;

SELECT count(), uniqExact(partition) FROM system.parts
WHERE database = currentDatabase() AND table = 't_partition_split_every_key_type' AND active;

DROP TABLE t_partition_split_every_key_type;

-- A key of one `FixedString` or `Decimal` column is hashed many rows at a time. 600 rows, more than two chunks of 256
-- rows, with 17 keys: 0 to 6 in the rows 0 to 299, 7 to 12 in the rows 300 to 449, 13 to 16 in the others.
DROP TABLE IF EXISTS t_partition_split_fixed_string;
DROP TABLE IF EXISTS t_partition_split_decimal;

CREATE TABLE t_partition_split_fixed_string (id UInt64, p FixedString(3)) ENGINE = MergeTree PARTITION BY p ORDER BY id;
CREATE TABLE t_partition_split_decimal (id UInt64, p Decimal64(2)) ENGINE = MergeTree PARTITION BY p ORDER BY id;

INSERT INTO t_partition_split_fixed_string
SELECT id, concat('f', toString(k), if(k < 10, 'x', ''))
FROM (SELECT number AS id, multiIf(id < 300, 5 * id % 7, id < 450, 7 + 5 * id % 6, 13 + 3 * id % 4) AS k FROM numbers(600))
SETTINGS max_block_size = 1000, max_insert_block_size = 1000, min_insert_block_size_rows = 1000, min_insert_block_size_bytes = 0,
    max_insert_threads = 1, max_partitions_per_insert_block = 0;

INSERT INTO t_partition_split_decimal
SELECT id, toDecimal64(toInt64(k) - 8, 2) / 4
FROM (SELECT number AS id, multiIf(id < 300, 5 * id % 7, id < 450, 7 + 5 * id % 6, 13 + 3 * id % 4) AS k FROM numbers(600))
SETTINGS max_block_size = 1000, max_insert_block_size = 1000, min_insert_block_size_rows = 1000, min_insert_block_size_bytes = 0,
    max_insert_threads = 1, max_partitions_per_insert_block = 0;

SELECT p.table, p.partition, p.min_block_number, p.max_block_number, p.rows, r.first_id
FROM system.parts AS p
INNER JOIN
(
    SELECT 't_partition_split_fixed_string' AS table, _part AS name, min(id) AS first_id FROM t_partition_split_fixed_string GROUP BY name
    UNION ALL
    SELECT 't_partition_split_decimal' AS table, _part AS name, min(id) AS first_id FROM t_partition_split_decimal GROUP BY name
) AS r USING (table, name)
WHERE p.database = currentDatabase() AND p.table IN ('t_partition_split_fixed_string', 't_partition_split_decimal') AND p.active
ORDER BY p.table DESC, p.min_block_number;

DROP TABLE t_partition_split_fixed_string;
DROP TABLE t_partition_split_decimal;
