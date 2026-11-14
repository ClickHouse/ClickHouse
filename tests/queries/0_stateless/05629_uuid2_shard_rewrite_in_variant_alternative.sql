-- Tags: shard

-- When `optimize_skip_unused_shards_rewrite_in` prunes one element of an `IN` list, the remaining
-- elements must keep the active alternative of a `Variant`. A value stored under `UUID2` in
-- `Variant(UUID, UUID2)` used to be rebuilt from its `Field` as historical `UUID`, so the shard was
-- sent a different identifier and the row was silently lost.

DROP TABLE IF EXISTS data_05629;
CREATE TABLE data_05629 (id UUID2) ENGINE = MergeTree ORDER BY id;
INSERT INTO data_05629 VALUES ('00000000-0000-0000-0000-000000000001'), ('00000000-0000-0001-0000-000000000002');

SET allow_suspicious_variant_types = 1;
SET enable_analyzer = 1;

SELECT 'rewrite_in = 0';
SELECT id, _shard_num FROM remote('127.{1,2}', currentDatabase(), data_05629, reinterpretAsUInt64(id))
WHERE id IN (
    CAST(toUUID2('00000000-0000-0000-0000-000000000001') AS Variant(UUID, UUID2)),
    toUUID2('00000000-0000-0001-0000-000000000002'))
ORDER BY id, _shard_num
SETTINGS optimize_skip_unused_shards = 1, optimize_skip_unused_shards_rewrite_in = 0, prefer_localhost_replica = 0;

-- Both shards hold every row. The `Variant` element is ambiguous and is kept on both shards, while the
-- plain `UUID2` element is pruned on one of them, which rebuilds the constant there.
SELECT 'rewrite_in = 1';
SELECT id, _shard_num FROM remote('127.{1,2}', currentDatabase(), data_05629, reinterpretAsUInt64(id))
WHERE id IN (
    CAST(toUUID2('00000000-0000-0000-0000-000000000001') AS Variant(UUID, UUID2)),
    toUUID2('00000000-0000-0001-0000-000000000002'))
ORDER BY id, _shard_num
SETTINGS optimize_skip_unused_shards = 1, optimize_skip_unused_shards_rewrite_in = 1, prefer_localhost_replica = 0;

DROP TABLE data_05629;
