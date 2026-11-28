-- Tags: no-fasttest, use-rocksdb
-- Tag no-fasttest, use-rocksdb: `EmbeddedRocksDB`

-- A `Set` matches a floating-point zero with the zero of the other sign, and with the values of other types
-- that convert to it, like the `String` '-0'. The optimizations that convert the elements of a set to another type
-- or evaluate an expression on them must not lose the rows holding the other representation.

-- The `bloom_filter` skip index of a `String` column, with a `Float64` set.
DROP TABLE IF EXISTS nz_bloom;
CREATE TABLE nz_bloom
(
    id UInt32,
    s String,
    a Array(String),
    INDEX idx_s s TYPE bloom_filter(0.00001) GRANULARITY 1,
    INDEX idx_a a TYPE bloom_filter(0.00001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO nz_bloom VALUES (1, '-0', ['-0']), (2, '2', ['2']), (3, '3', ['3']);
SELECT count() FROM nz_bloom WHERE s IN (SELECT arrayJoin([0., 1.])) SETTINGS use_skip_indexes = 1;
SELECT count() FROM nz_bloom WHERE s IN (SELECT arrayJoin([0., 1.])) SETTINGS use_skip_indexes = 0;
SELECT count() FROM nz_bloom WHERE a IN (SELECT [0.]) SETTINGS use_skip_indexes = 1;
SELECT count() FROM nz_bloom WHERE a IN (SELECT [0.]) SETTINGS use_skip_indexes = 0;
DROP TABLE nz_bloom;

-- The key lookup of `EmbeddedRocksDB` with a `String` key.
DROP TABLE IF EXISTS nz_kv;
CREATE TABLE nz_kv (k String, id UInt64) ENGINE = EmbeddedRocksDB PRIMARY KEY k;
INSERT INTO nz_kv VALUES ('-0', 1), ('2', 2);
SELECT count() FROM nz_kv WHERE k IN (SELECT 0.);
SELECT count() FROM nz_kv WHERE materialize(k) IN (SELECT 0.);
DROP TABLE nz_kv;

-- `optimize_skip_unused_shards` with a sharding key that tells the zeros apart: `-0.` is on the second shard.
SELECT count()
FROM remote('127.0.0.{1,2}',
    view(SELECT arrayJoin([0., -0., 1.]) AS f WHERE bitShiftRight(reinterpretAsUInt64(f), 63) = shardNum() - 1),
    bitShiftRight(reinterpretAsUInt64(f), 63))
WHERE f IN (0., 1.)
SETTINGS optimize_skip_unused_shards = 1, optimize_skip_unused_shards_rewrite_in = 0, prefer_localhost_replica = 0;
SELECT count()
FROM remote('127.0.0.{1,2}',
    view(SELECT arrayJoin([0., -0., 1.]) AS f WHERE bitShiftRight(reinterpretAsUInt64(f), 63) = shardNum() - 1),
    bitShiftRight(reinterpretAsUInt64(f), 63))
WHERE f IN (0., 1.)
SETTINGS optimize_skip_unused_shards = 0, prefer_localhost_replica = 0;
