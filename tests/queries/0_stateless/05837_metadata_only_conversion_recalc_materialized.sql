-- A conversion that changes only the metadata (Enum8 -> Int8 keeps the stored bytes) can still change the
-- value of a MATERIALIZED column computed from the column, so such an ALTER runs a mutation that
-- recalculates it. Without MATERIALIZED dependents it stays a metadata-only change.

DROP TABLE IF EXISTS t_enum_materialized;
DROP TABLE IF EXISTS t_enum_plain;
DROP TABLE IF EXISTS t_enum_materialized_key;

CREATE TABLE t_enum_materialized (x Enum8('a' = 1, 'b' = 2), m String MATERIALIZED toString(x))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_enum_materialized (x) VALUES ('a'), ('b');
ALTER TABLE t_enum_materialized MODIFY COLUMN x Int8 SETTINGS mutations_sync = 2;
SELECT x, m, m = toString(x) FROM t_enum_materialized ORDER BY x;

CREATE TABLE t_enum_plain (x Enum8('a' = 1, 'b' = 2)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_enum_plain VALUES ('a');
ALTER TABLE t_enum_plain MODIFY COLUMN x Int8 SETTINGS mutations_sync = 2;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_enum_plain';

-- Restating the same type (here with a new codec) does not change any value, so no mutation either.
ALTER TABLE t_enum_materialized MODIFY COLUMN x Int8 CODEC(ZSTD(1)) SETTINGS mutations_sync = 2;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_enum_materialized';

-- Adding values to an Enum keeps every existing value, so it stays a metadata-only change.
DROP TABLE IF EXISTS t_enum_extend;
CREATE TABLE t_enum_extend (x Enum8('a' = 1), m String MATERIALIZED toString(x)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_enum_extend (x) VALUES ('a');
ALTER TABLE t_enum_extend MODIFY COLUMN x Enum8('a' = 1, 'b' = 2) SETTINGS mutations_sync = 2;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_enum_extend';
SELECT x, m FROM t_enum_extend;
DROP TABLE t_enum_extend;

-- The MATERIALIZED column is a key column, which cannot be recalculated in an existing part.
CREATE TABLE t_enum_materialized_key (x Enum8('a' = 1, 'b' = 2), m String MATERIALIZED toString(x))
ENGINE = MergeTree ORDER BY m;
ALTER TABLE t_enum_materialized_key MODIFY COLUMN x Int8; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t_enum_materialized;
DROP TABLE t_enum_plain;
DROP TABLE t_enum_materialized_key;
