-- `CLEAR COLUMN` of a whole `Nested` group recalculates the `MATERIALIZED` columns that read its members,
-- and `CLEAR COLUMN` must not rewrite the sign, version or `is_deleted` column of a merging engine.
-- The `Nested` tables use compact parts: on wide parts, `CLEAR COLUMN` of a whole `Nested` group leaves the
-- stored values in place (also without matchers), so the recalculated `MATERIALIZED` value is unchanged there.

DROP TABLE IF EXISTS t_nested;
CREATE TABLE t_nested (k UInt64, t Nested(a UInt64, b UInt64), m UInt64 MATERIALIZED arraySum(t.a) + 1)
ENGINE = MergeTree ORDER BY k SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000;
INSERT INTO t_nested (k, `t.a`, `t.b`) VALUES (1, [5], [6]);
ALTER TABLE t_nested CLEAR COLUMN t SETTINGS mutations_sync = 2;
SELECT k, t.a, t.b, m FROM t_nested ORDER BY k;
DROP TABLE t_nested;

DROP TABLE IF EXISTS t_nested_key;
CREATE TABLE t_nested_key (k UInt64, t Nested(a UInt64, b UInt64), m UInt64 MATERIALIZED arraySum(t.a) + 1)
ENGINE = MergeTree ORDER BY k PARTITION BY m SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000;
INSERT INTO t_nested_key (k, `t.a`, `t.b`) VALUES (1, [5], [6]);
ALTER TABLE t_nested_key CLEAR COLUMN t SETTINGS mutations_sync = 2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT k, t.a, m FROM t_nested_key ORDER BY k;
DROP TABLE t_nested_key;

DROP TABLE IF EXISTS t_collapsing;
CREATE TABLE t_collapsing (k UInt64, s Int8 MATERIALIZED if(x > 0, 1, -1), x Int64) ENGINE = CollapsingMergeTree(s) ORDER BY k;
INSERT INTO t_collapsing (k, x) VALUES (1, 5);
ALTER TABLE t_collapsing CLEAR COLUMN x SETTINGS mutations_sync = 2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT k, s, x FROM t_collapsing;
DROP TABLE t_collapsing;

DROP TABLE IF EXISTS t_replacing;
CREATE TABLE t_replacing (k UInt64, v UInt64 MATERIALIZED x * 2, d UInt8 MATERIALIZED toUInt8(y > 0), x UInt64, y UInt64)
ENGINE = ReplacingMergeTree(v, d) ORDER BY k;
INSERT INTO t_replacing (k, x, y) VALUES (1, 5, 1);
ALTER TABLE t_replacing CLEAR COLUMN x SETTINGS mutations_sync = 2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_replacing CLEAR COLUMN y SETTINGS mutations_sync = 2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT k, v, d, x, y FROM t_replacing;
DROP TABLE t_replacing;
