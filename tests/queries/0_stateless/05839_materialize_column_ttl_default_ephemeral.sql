-- `MATERIALIZE COLUMN` of a column feeding a column TTL runs the full TTL pass, which resets an expired
-- TTL target to its `DEFAULT` expression. If that expression reads an `EPHEMERAL` column, it cannot be
-- evaluated by a mutation (the column is not stored), so the command is refused up front instead of
-- queuing a mutation that never finishes.

DROP TABLE IF EXISTS t_mat_ttl_default_ephemeral;
CREATE TABLE t_mat_ttl_default_ephemeral
(
    id UInt64,
    e UInt64 EPHEMERAL 7,
    c DateTime MATERIALIZED toDateTime(2000000000),
    y UInt64 DEFAULT e + 1 TTL c + INTERVAL 1 SECOND
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SYSTEM STOP TTL MERGES t_mat_ttl_default_ephemeral;
INSERT INTO t_mat_ttl_default_ephemeral (id, y) VALUES (1, 100);
ALTER TABLE t_mat_ttl_default_ephemeral MODIFY COLUMN c DateTime MATERIALIZED toDateTime(1000000000);
ALTER TABLE t_mat_ttl_default_ephemeral MATERIALIZE COLUMN c SETTINGS mutations_sync = 2; -- { serverError CANNOT_UPDATE_COLUMN }
SELECT 'refused', id, toUnixTimestamp(c), y FROM t_mat_ttl_default_ephemeral;
SELECT 'mutations', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_mat_ttl_default_ephemeral' AND NOT is_done;

-- Materializing a column that feeds no TTL expression does not run the TTL pass, so it is not refused.
ALTER TABLE t_mat_ttl_default_ephemeral MATERIALIZE COLUMN y SETTINGS mutations_sync = 2;
SELECT 'other column', id, y FROM t_mat_ttl_default_ephemeral;
DROP TABLE t_mat_ttl_default_ephemeral;

-- The same when the `EPHEMERAL` column is reached through an `ALIAS`.
DROP TABLE IF EXISTS t_mat_ttl_default_ephemeral_alias;
CREATE TABLE t_mat_ttl_default_ephemeral_alias
(
    id UInt64,
    e UInt64 EPHEMERAL 7,
    a UInt64 ALIAS e + 10,
    c DateTime MATERIALIZED toDateTime(2000000000),
    y UInt64 DEFAULT a + 1 TTL c + INTERVAL 1 SECOND
)
ENGINE = MergeTree ORDER BY id;

SYSTEM STOP TTL MERGES t_mat_ttl_default_ephemeral_alias;
INSERT INTO t_mat_ttl_default_ephemeral_alias (id, y) VALUES (1, 100);
ALTER TABLE t_mat_ttl_default_ephemeral_alias MODIFY COLUMN c DateTime MATERIALIZED toDateTime(1000000000);
ALTER TABLE t_mat_ttl_default_ephemeral_alias MATERIALIZE COLUMN c SETTINGS mutations_sync = 2; -- { serverError CANNOT_UPDATE_COLUMN }
SELECT 'refused alias', id, toUnixTimestamp(c), y FROM t_mat_ttl_default_ephemeral_alias;
DROP TABLE t_mat_ttl_default_ephemeral_alias;
