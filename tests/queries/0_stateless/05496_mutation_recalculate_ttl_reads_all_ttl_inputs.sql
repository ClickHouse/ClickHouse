-- Tags: no-object-storage
-- An object-storage policy has no disk `default`, which the move TTL of `t_move` needs.

-- A mutation that updates the input of one TTL rule and no TTL target recalculates the TTL infos of
-- every rule, so it has to read the inputs of all of them, not only `d1`. Each table has a Wide part
-- (a Compact or packed part is rewritten whole) and a column `v` that nothing reads or writes, so the
-- part is mutated partially; the `part_log` query at the end checks that.

SET mutations_sync = 2;

DROP TABLE IF EXISTS t_recompress;
DROP TABLE IF EXISTS t_delete;
DROP TABLE IF EXISTS t_delete_where;
DROP TABLE IF EXISTS t_group_by;
DROP TABLE IF EXISTS t_column;
DROP TABLE IF EXISTS t_move;
DROP TABLE IF EXISTS t_clear;

CREATE TABLE t_recompress (k UInt64, d1 DateTime('UTC'), d2 DateTime('UTC'), v String)
ENGINE = MergeTree ORDER BY k
TTL d1 + INTERVAL 1 DAY RECOMPRESS CODEC(ZSTD(3)), d2 + INTERVAL 1 DAY RECOMPRESS CODEC(ZSTD(5))
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

CREATE TABLE t_delete (k UInt64, d1 DateTime('UTC'), d2 DateTime('UTC'), v String)
ENGINE = MergeTree ORDER BY k
TTL d1 + INTERVAL 1 DAY RECOMPRESS CODEC(ZSTD(3)), d2 + INTERVAL 10 YEAR
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

CREATE TABLE t_delete_where (k UInt64, d1 DateTime('UTC'), d2 DateTime('UTC'), v String)
ENGINE = MergeTree ORDER BY k
TTL d1 + INTERVAL 1 DAY RECOMPRESS CODEC(ZSTD(3)), d2 + INTERVAL 10 YEAR DELETE WHERE k < 1000000
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

CREATE TABLE t_group_by (k UInt64, d1 DateTime('UTC'), d2 DateTime('UTC'), v String)
ENGINE = MergeTree ORDER BY k
TTL d1 + INTERVAL 1 DAY RECOMPRESS CODEC(ZSTD(3)), d2 + INTERVAL 10 YEAR GROUP BY k SET v = any(v)
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

CREATE TABLE t_column (k UInt64, d1 DateTime('UTC'), d2 DateTime('UTC'), w String TTL d2 + INTERVAL 10 YEAR, v String)
ENGINE = MergeTree ORDER BY k
TTL d1 + INTERVAL 1 DAY RECOMPRESS CODEC(ZSTD(3))
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

CREATE TABLE t_move (k UInt64, d1 DateTime('UTC'), d2 DateTime('UTC'), v String)
ENGINE = MergeTree ORDER BY k
TTL d1 + INTERVAL 1 DAY RECOMPRESS CODEC(ZSTD(3)), d2 + INTERVAL 10 YEAR TO DISK 'default'
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

-- The `d2` rule is in the future both before (2100) and after (2070) the CLEAR, and tells the two apart.
CREATE TABLE t_clear (k UInt64, d1 DateTime('UTC'), d2 DateTime('UTC'), v String)
ENGINE = MergeTree ORDER BY k
TTL d1 + INTERVAL 1 DAY RECOMPRESS CODEC(ZSTD(3)), d2 + INTERVAL 100 YEAR RECOMPRESS CODEC(ZSTD(5))
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_recompress SELECT number, toDateTime('2090-01-01 00:00:00', 'UTC'), toDateTime('2090-01-01 00:00:00', 'UTC'), toString(number) FROM numbers(100);
INSERT INTO t_delete SELECT * FROM t_recompress;
INSERT INTO t_delete_where SELECT * FROM t_recompress;
INSERT INTO t_group_by SELECT * FROM t_recompress;
INSERT INTO t_column SELECT k, d1, d2, v, v FROM t_recompress;
INSERT INTO t_move SELECT * FROM t_recompress;
INSERT INTO t_clear SELECT k, d1, toDateTime('2000-01-01 00:00:00', 'UTC'), v FROM t_recompress;

ALTER TABLE t_recompress UPDATE d1 = toDateTime('2095-01-01 00:00:00', 'UTC') WHERE 1;
ALTER TABLE t_delete UPDATE d1 = toDateTime('2095-01-01 00:00:00', 'UTC') WHERE 1;
ALTER TABLE t_delete_where UPDATE d1 = toDateTime('2095-01-01 00:00:00', 'UTC') WHERE 1;
ALTER TABLE t_group_by UPDATE d1 = toDateTime('2095-01-01 00:00:00', 'UTC') WHERE 1;
ALTER TABLE t_column UPDATE d1 = toDateTime('2095-01-01 00:00:00', 'UTC') WHERE 1;
ALTER TABLE t_move UPDATE d1 = toDateTime('2095-01-01 00:00:00', 'UTC') WHERE 1;
ALTER TABLE t_clear CLEAR COLUMN d2;

SELECT 'recompress', count(), min(d1), max(d1), min(d2), max(d2), sum(cityHash64(k, v)) FROM t_recompress;
SELECT 'delete', count(), min(d1), max(d1), min(d2), max(d2), sum(cityHash64(k, v)) FROM t_delete;
SELECT 'delete_where', count(), min(d1), max(d1), min(d2), max(d2), sum(cityHash64(k, v)) FROM t_delete_where;
SELECT 'group_by', count(), min(d1), max(d1), min(d2), max(d2), sum(cityHash64(k, v)) FROM t_group_by;
SELECT 'column', count(), min(d1), max(d1), min(d2), max(d2), sum(cityHash64(k, v)), sum(cityHash64(k, w)) FROM t_column;
SELECT 'move', count(), min(d1), max(d1), min(d2), max(d2), sum(cityHash64(k, v)) FROM t_move;
SELECT 'clear', count(), min(d1), max(d1), min(d2), max(d2), sum(cityHash64(k, v)) FROM t_clear;

-- The recalculated TTL infos: the `d1` rule moved to 2095, every other rule kept its bound.
SELECT DISTINCT table, 'recompression', e, toDateTime(mn, 'UTC'), toDateTime(mx, 'UTC')
FROM system.parts
ARRAY JOIN recompression_ttl_info.expression AS e, recompression_ttl_info.min AS mn, recompression_ttl_info.max AS mx
WHERE database = currentDatabase() AND active
ORDER BY table, e;

SELECT DISTINCT table, 'delete', toDateTime(delete_ttl_info_min, 'UTC'), toDateTime(delete_ttl_info_max, 'UTC')
FROM system.parts WHERE database = currentDatabase() AND table = 't_delete' AND active;

SELECT DISTINCT table, 'rows_where', e, toDateTime(mn, 'UTC'), toDateTime(mx, 'UTC')
FROM system.parts
ARRAY JOIN rows_where_ttl_info.expression AS e, rows_where_ttl_info.min AS mn, rows_where_ttl_info.max AS mx
WHERE database = currentDatabase() AND table = 't_delete_where' AND active;

SELECT DISTINCT table, 'group_by', e, toDateTime(mn, 'UTC'), toDateTime(mx, 'UTC')
FROM system.parts
ARRAY JOIN group_by_ttl_info.expression AS e, group_by_ttl_info.min AS mn, group_by_ttl_info.max AS mx
WHERE database = currentDatabase() AND table = 't_group_by' AND active;

SELECT DISTINCT table, 'column', column, toDateTime(column_ttl_min, 'UTC'), toDateTime(column_ttl_max, 'UTC')
FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_column' AND column = 'w' AND active;

SELECT DISTINCT table, 'move', e, toDateTime(mn, 'UTC'), toDateTime(mx, 'UTC')
FROM system.parts
ARRAY JOIN move_ttl_info.expression AS e, move_ttl_info.min AS mn, move_ttl_info.max AS mx
WHERE database = currentDatabase() AND table = 't_move' AND active;

-- Every mutation was a partial one, the path that did not read the other inputs.
SYSTEM FLUSH LOGS part_log;
SELECT table, min(ProfileEvents['MutationSomePartColumns'])
FROM system.part_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND database = currentDatabase() AND event_type = 'MutatePart'
GROUP BY table ORDER BY table;

DROP TABLE t_recompress;
DROP TABLE t_delete;
DROP TABLE t_delete_where;
DROP TABLE t_group_by;
DROP TABLE t_column;
DROP TABLE t_move;
DROP TABLE t_clear;
