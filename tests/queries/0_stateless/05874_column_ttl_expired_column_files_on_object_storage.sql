-- A column expired by its TTL in a mutation must not leave its files in the new part on an object storage disk.
-- https://github.com/ClickHouse/ClickHouse/issues/134206

DROP TABLE IF EXISTS t_ttl_expired_obj;

-- Wide parts in full storage: otherwise MATERIALIZE TTL rewrites every column.
CREATE TABLE t_ttl_expired_obj
(
    id UInt64,
    d DateTime,
    c String TTL d + INTERVAL 1 SECOND,
    t Tuple(a Int32) TTL d + INTERVAL 1 SECOND,
    payload String
)
ENGINE = MergeTree ORDER BY id
SETTINGS disk = 'local_disk', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

-- Keep the expiration in the mutation.
SYSTEM STOP TTL MERGES t_ttl_expired_obj;

INSERT INTO t_ttl_expired_obj VALUES (1, '2000-01-01 00:00:00', 'abc', tuple(5), 'payload');
ALTER TABLE t_ttl_expired_obj MATERIALIZE TTL SETTINGS mutations_sync = 2;

SELECT countIf(column IN ('c', 't')) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_ttl_expired_obj' AND active;

CHECK TABLE t_ttl_expired_obj SETTINGS check_query_single_value_result = 1;

ALTER TABLE t_ttl_expired_obj UPDATE payload = 'x' WHERE 1 SETTINGS mutations_sync = 2;

CHECK TABLE t_ttl_expired_obj SETTINGS check_query_single_value_result = 1;

SELECT id, c, t, payload FROM t_ttl_expired_obj;

DROP TABLE t_ttl_expired_obj;
