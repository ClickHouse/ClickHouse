-- Tags: no-fasttest, use-rocksdb
-- Tag no-fasttest, use-rocksdb: `EmbeddedRocksDB`

-- `-0.` and `0.` are the same key of a `Set`, `GROUP BY`, `DISTINCT` and `LIMIT BY`.

-- The key lookup of `EmbeddedRocksDB` with a composite key: the `String` '-0' matches the `Float64` zero of the set.
DROP TABLE IF EXISTS nz_kv_composite;
CREATE TABLE nz_kv_composite (k String, id UInt64, v UInt64) ENGINE = EmbeddedRocksDB PRIMARY KEY (k, id);
INSERT INTO nz_kv_composite VALUES ('-0', 7, 42), ('2', 7, 43);
SELECT count() FROM nz_kv_composite WHERE (k, id) IN (SELECT 0., toUInt64(7));
SELECT count() FROM nz_kv_composite WHERE (materialize(k), id) IN (SELECT 0., toUInt64(7));
DROP TABLE nz_kv_composite;

-- `optimize_distributed_group_by_sharding_key` with a sharding key that tells the zeros apart: `-0.` is on the second shard,
-- so a group spans both shards and the final reduction on the initiator must stay.
SELECT 'distinct';
SELECT count() FROM (SELECT DISTINCT f
FROM remote('127.0.0.{1,2}',
    view(SELECT arrayJoin([0., -0.]) AS f WHERE bitShiftRight(reinterpretAsUInt64(f), 63) = shardNum() - 1),
    bitShiftRight(reinterpretAsUInt64(f), 63))
) SETTINGS optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 1, prefer_localhost_replica = 0;
SELECT count() FROM (SELECT DISTINCT f
FROM remote('127.0.0.{1,2}',
    view(SELECT arrayJoin([0., -0.]) AS f WHERE bitShiftRight(reinterpretAsUInt64(f), 63) = shardNum() - 1),
    bitShiftRight(reinterpretAsUInt64(f), 63))
) SETTINGS optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 0, prefer_localhost_replica = 0;

SELECT 'group by';
SELECT abs(f), count()
FROM remote('127.0.0.{1,2}',
    view(SELECT arrayJoin([0., -0.]) AS f WHERE bitShiftRight(reinterpretAsUInt64(f), 63) = shardNum() - 1),
    bitShiftRight(reinterpretAsUInt64(f), 63))
GROUP BY f
SETTINGS optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 1, prefer_localhost_replica = 0;
SELECT abs(f), count()
FROM remote('127.0.0.{1,2}',
    view(SELECT arrayJoin([0., -0.]) AS f WHERE bitShiftRight(reinterpretAsUInt64(f), 63) = shardNum() - 1),
    bitShiftRight(reinterpretAsUInt64(f), 63))
GROUP BY f
SETTINGS optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 0, prefer_localhost_replica = 0;

SELECT 'limit by';
SELECT count() FROM (SELECT f
FROM remote('127.0.0.{1,2}',
    view(SELECT arrayJoin([0., -0.]) AS f WHERE bitShiftRight(reinterpretAsUInt64(f), 63) = shardNum() - 1),
    bitShiftRight(reinterpretAsUInt64(f), 63))
LIMIT 1 BY f
) SETTINGS optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 1, prefer_localhost_replica = 0;
SELECT count() FROM (SELECT f
FROM remote('127.0.0.{1,2}',
    view(SELECT arrayJoin([0., -0.]) AS f WHERE bitShiftRight(reinterpretAsUInt64(f), 63) = shardNum() - 1),
    bitShiftRight(reinterpretAsUInt64(f), 63))
LIMIT 1 BY f
) SETTINGS optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 0, prefer_localhost_replica = 0;
