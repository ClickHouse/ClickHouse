-- Partition-targeting ALTER must use the active type of a `Variant` or `Dynamic` constant: the values below
-- differ only by swapped 64-bit halves, which is exactly the layout difference between `UUID` and `UUID2`,
-- so treating a `UUID2` value as a historical `UUID` would drop the other partition.

SET allow_suspicious_variant_types = 1;

DROP TABLE IF EXISTS t_uuid_partition_variant;
CREATE TABLE t_uuid_partition_variant (u UUID) ENGINE = MergeTree PARTITION BY u ORDER BY u;

INSERT INTO t_uuid_partition_variant VALUES ('00000000-0000-0000-0000-000000000001'), ('00000000-0000-0001-0000-000000000000');
ALTER TABLE t_uuid_partition_variant DROP PARTITION tuple(CAST(toUUID2('00000000-0000-0000-0000-000000000001') AS Variant(UUID2, String)));
SELECT u FROM t_uuid_partition_variant ORDER BY u;

TRUNCATE TABLE t_uuid_partition_variant;
INSERT INTO t_uuid_partition_variant VALUES ('00000000-0000-0000-0000-000000000001'), ('00000000-0000-0001-0000-000000000000');
ALTER TABLE t_uuid_partition_variant DROP PARTITION tuple(toUUID2('00000000-0000-0000-0000-000000000001')::Dynamic);
SELECT u FROM t_uuid_partition_variant ORDER BY u;

-- A `Variant` holding a `String` is parsed as the key type.
ALTER TABLE t_uuid_partition_variant DROP PARTITION tuple(CAST('00000000-0000-0001-0000-000000000000' AS Variant(UUID2, String)));
SELECT count() FROM t_uuid_partition_variant;

DROP TABLE t_uuid_partition_variant;

-- The same for a complex (tuple) partition key.
DROP TABLE IF EXISTS t_uuid_partition_variant_tuple;
CREATE TABLE t_uuid_partition_variant_tuple (u UUID, n UInt8) ENGINE = MergeTree PARTITION BY (u, n) ORDER BY u;

INSERT INTO t_uuid_partition_variant_tuple VALUES ('00000000-0000-0000-0000-000000000001', 1), ('00000000-0000-0001-0000-000000000000', 1);
ALTER TABLE t_uuid_partition_variant_tuple DROP PARTITION (CAST(toUUID2('00000000-0000-0000-0000-000000000001') AS Variant(UUID2, String)), 1);
SELECT u, n FROM t_uuid_partition_variant_tuple ORDER BY u;

ALTER TABLE t_uuid_partition_variant_tuple DROP PARTITION (toUUID2('00000000-0000-0001-0000-000000000000')::Dynamic, 1);
SELECT count() FROM t_uuid_partition_variant_tuple;

DROP TABLE t_uuid_partition_variant_tuple;

-- A `UUID2` partition key with a `Variant` constant holding a historical `UUID`.
DROP TABLE IF EXISTS t_uuid2_partition_variant;
CREATE TABLE t_uuid2_partition_variant (u UUID2) ENGINE = MergeTree PARTITION BY u ORDER BY u;

INSERT INTO t_uuid2_partition_variant VALUES ('00000000-0000-0000-0000-000000000001'), ('00000000-0000-0001-0000-000000000000');
ALTER TABLE t_uuid2_partition_variant DROP PARTITION tuple(CAST(toUUID('00000000-0000-0000-0000-000000000001') AS Variant(UUID, String)));
SELECT u FROM t_uuid2_partition_variant ORDER BY u;

DROP TABLE t_uuid2_partition_variant;
