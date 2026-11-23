-- A type change of `x` and a later UPDATE that reads a MATERIALIZED column computed from `x` (`y = m`) can be
-- executed by one mutation of a part. The UPDATE must see `m` recomputed from the converted `x`, as when the two
-- run one after the other.

DROP TABLE IF EXISTS t_batched;
DROP TABLE IF EXISTS t_serial;

CREATE TABLE t_batched (x Int64, m Int64 MATERIALIZED x % 1000, y Int64, s String)
ENGINE = MergeTree ORDER BY s
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_batched (x, y, s) VALUES (5000000001, 0, 'payload');

SYSTEM STOP MERGES t_batched;
ALTER TABLE t_batched MODIFY COLUMN x Int32 SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_batched UPDATE y = m WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0;
SYSTEM START MERGES t_batched;
-- Mutations are applied in order, so waiting for this one waits for both above.
ALTER TABLE t_batched UPDATE y = y WHERE 0 SETTINGS mutations_sync = 2;

SELECT x, m, y FROM t_batched;

CREATE TABLE t_serial (x Int64, m Int64 MATERIALIZED x % 1000, y Int64, s String)
ENGINE = MergeTree ORDER BY s
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_serial (x, y, s) VALUES (5000000001, 0, 'payload');
ALTER TABLE t_serial MODIFY COLUMN x Int32 SETTINGS mutations_sync = 2;
ALTER TABLE t_serial UPDATE y = m WHERE 1 SETTINGS mutations_sync = 2;

SELECT x, m, y FROM t_serial;

DROP TABLE t_batched;
DROP TABLE t_serial;

-- With two conversions around the UPDATE, only the one before it affects what the UPDATE reads: `m` depends on
-- `z`, which is converted after the UPDATE, so the UPDATE still reads the old `m`.
CREATE TABLE t_batched (x Int64, z Int64, m Int64 MATERIALIZED z % 1000, y Int64, s String)
ENGINE = MergeTree ORDER BY s
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_batched (x, z, y, s) VALUES (1, 5000000001, 0, 'payload');

SYSTEM STOP MERGES t_batched;
ALTER TABLE t_batched MODIFY COLUMN x Int32 SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_batched UPDATE y = m WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_batched MODIFY COLUMN z Int32 SETTINGS mutations_sync = 0, alter_sync = 0;
SYSTEM START MERGES t_batched;
ALTER TABLE t_batched UPDATE y = y WHERE 0 SETTINGS mutations_sync = 2;

SELECT z, m, y FROM t_batched;

CREATE TABLE t_serial (x Int64, z Int64, m Int64 MATERIALIZED z % 1000, y Int64, s String)
ENGINE = MergeTree ORDER BY s
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_serial (x, z, y, s) VALUES (1, 5000000001, 0, 'payload');
ALTER TABLE t_serial MODIFY COLUMN x Int32 SETTINGS mutations_sync = 2;
ALTER TABLE t_serial UPDATE y = m WHERE 1 SETTINGS mutations_sync = 2;
ALTER TABLE t_serial MODIFY COLUMN z Int32 SETTINGS mutations_sync = 2;

SELECT z, m, y FROM t_serial;

DROP TABLE t_batched;
DROP TABLE t_serial;
