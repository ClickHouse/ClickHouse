-- A `Variant` nested in a collection loses its discriminator when the value becomes a `Field`;
-- the layout of a UUID value is then taken from the single `UUID` or `UUID2` alternative of the `Variant`.

SET allow_suspicious_variant_types = 1;

SELECT u FROM values('u Array(UUID2)', [CAST(toUUID('00000000-0000-0000-0000-000000000001') AS Variant(UUID, String))]);
SELECT u FROM values('u Array(UUID1)', [CAST(toUUID2('00000000-0000-0000-0000-000000000001') AS Variant(UUID2, String))]);
SELECT u FROM values('u Tuple(UUID1, UInt8)', (CAST(toUUID2('00000000-0000-0000-0000-000000000001') AS Variant(UUID2, String)), 1));

-- Both alternatives: the layout of the value is unknown.
SELECT u FROM values('u Array(UUID2)', [CAST(toUUID('00000000-0000-0000-0000-000000000001') AS Variant(UUID, UUID2, String))]); -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS t_uuid2_array_partition;
CREATE TABLE t_uuid2_array_partition (u Array(UUID1)) ENGINE = MergeTree PARTITION BY u ORDER BY tuple();
INSERT INTO t_uuid2_array_partition VALUES (['00000000-0000-0000-0000-000000000001']), (['00000000-0000-0001-0000-000000000000']);
ALTER TABLE t_uuid2_array_partition DROP PARTITION tuple([CAST(toUUID2('00000000-0000-0000-0000-000000000001') AS Variant(UUID2, String))]);
SELECT u FROM t_uuid2_array_partition;
DROP TABLE t_uuid2_array_partition;
