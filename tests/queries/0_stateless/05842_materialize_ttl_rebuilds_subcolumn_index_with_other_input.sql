-- A column TTL on `t` resets it, so MATERIALIZE TTL rebuilds a skip index over its subcolumn and another column
-- (`t.a + y`). The mutation has to read `y` as well, otherwise it fails, and the rebuilt index has to describe
-- the reset value, otherwise it skips the granule that now matches.

DROP TABLE IF EXISTS t_ttl_index;

CREATE TABLE t_ttl_index
(
    id UInt64,
    d DateTime,
    t Tuple(a Int32) TTL d + INTERVAL 1 SECOND,
    y Int32,
    payload String,
    INDEX idx (t.a + y) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, materialize_ttl_recalculate_only = 0;

SYSTEM STOP TTL MERGES t_ttl_index;
INSERT INTO t_ttl_index VALUES (1, '2000-01-01 00:00:00', tuple(100), 7, 'payload');

ALTER TABLE t_ttl_index MATERIALIZE TTL SETTINGS mutations_sync = 2;

SELECT t, y FROM t_ttl_index;
SELECT count() FROM t_ttl_index WHERE t.a + y = 7 SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_ttl_index WHERE t.a + y = 7 SETTINGS use_skip_indexes = 0;

-- The same when an ordinary UPDATE of the TTL source makes the TTL reset `t`.
TRUNCATE TABLE t_ttl_index;
INSERT INTO t_ttl_index VALUES (1, '2100-01-01 00:00:00', tuple(100), 7, 'payload');

ALTER TABLE t_ttl_index UPDATE d = toDateTime('2000-01-01 00:00:00') WHERE 1 SETTINGS mutations_sync = 2;

SELECT t, y FROM t_ttl_index;
SELECT count() FROM t_ttl_index WHERE t.a + y = 7 SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_ttl_index WHERE t.a + y = 7 SETTINGS use_skip_indexes = 0;

DROP TABLE t_ttl_index;
