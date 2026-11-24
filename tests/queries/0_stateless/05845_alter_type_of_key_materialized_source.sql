-- A MATERIALIZED column that is part of a key, or is the engine's version column, cannot be recalculated in an
-- existing part: the sort order, the partition id and the merge semantics are fixed when the part is written. So
-- a type change of a column it is computed from is rejected, even one that keeps the values of the source.

-- Case 1: the key is a subcolumn of the MATERIALIZED column (`ORDER BY m.a`).
DROP TABLE IF EXISTS t_materialized_subcolumn_key;
CREATE TABLE t_materialized_subcolumn_key (x Int64, m Tuple(a Int64) MATERIALIZED tuple(x), payload Int64)
ENGINE = MergeTree ORDER BY m.a
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_materialized_subcolumn_key (x, payload) SELECT toInt64(3000000000) + number, number FROM numbers(16);

ALTER TABLE t_materialized_subcolumn_key MODIFY COLUMN x Int32 SETTINGS mutations_sync = 2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- The ALTER did not apply, so the data and its order are intact.
SELECT count() FROM t_materialized_subcolumn_key WHERE m.a >= toInt64(3000000000);

DROP TABLE t_materialized_subcolumn_key;

-- Case 2: the MATERIALIZED column is the version column, which is not part of the key.
DROP TABLE IF EXISTS t_materialized_version;
CREATE TABLE t_materialized_version (d Int64, x Int64, v UInt64 MATERIALIZED toUInt64(x))
ENGINE = ReplacingMergeTree(v) ORDER BY d
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_materialized_version (d, x) SELECT number, number FROM numbers(4);

ALTER TABLE t_materialized_version MODIFY COLUMN x Int32 SETTINGS mutations_sync = 2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

SELECT count() FROM t_materialized_version;

DROP TABLE t_materialized_version;

-- Case 3: the version column is a subcolumn of the MATERIALIZED column (`ReplacingMergeTree(m.v)`).
DROP TABLE IF EXISTS t_materialized_version_subcolumn;
CREATE TABLE t_materialized_version_subcolumn (id UInt64, x UInt64, m Tuple(v UInt64) MATERIALIZED tuple(x))
ENGINE = ReplacingMergeTree(m.v) ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_materialized_version_subcolumn (id, x) VALUES (1, 4294967297);

ALTER TABLE t_materialized_version_subcolumn MODIFY COLUMN x UInt32 SETTINGS mutations_sync = 2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

SELECT x, m.v FROM t_materialized_version_subcolumn;

DROP TABLE t_materialized_version_subcolumn;

-- Case 4: the altered column is the version column itself, whose type may otherwise be widened.
DROP TABLE IF EXISTS t_version_source_of_key;
DROP TABLE IF EXISTS t_version_plain;
CREATE TABLE t_version_source_of_key (v UInt32, m String MATERIALIZED toTypeName(v), s String)
ENGINE = ReplacingMergeTree(v) ORDER BY m
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_version_source_of_key (v, s) VALUES (1, 'payload');

ALTER TABLE t_version_source_of_key MODIFY COLUMN v UInt64 SETTINGS mutations_sync = 2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

SELECT m = toTypeName(v) FROM t_version_source_of_key;

CREATE TABLE t_version_plain (v UInt32, s String) ENGINE = ReplacingMergeTree(v) ORDER BY s;
INSERT INTO t_version_plain VALUES (1, 'payload');
ALTER TABLE t_version_plain MODIFY COLUMN v UInt64 SETTINGS mutations_sync = 2;
SELECT toTypeName(v) FROM t_version_plain;

DROP TABLE t_version_source_of_key;
DROP TABLE t_version_plain;

-- Case 5: a key column that also reads an EPHEMERAL column, including through a metadata-only conversion.
DROP TABLE IF EXISTS t_ephemeral_key_type_name;
DROP TABLE IF EXISTS t_ephemeral_key_value;
DROP TABLE IF EXISTS t_ephemeral_key_metadata_only;
DROP TABLE IF EXISTS t_ephemeral_not_key;

CREATE TABLE t_ephemeral_key_type_name (x Int64, e UInt8 EPHEMERAL 1, m String MATERIALIZED if(e, toTypeName(x), ''))
ENGINE = MergeTree ORDER BY m;
ALTER TABLE t_ephemeral_key_type_name MODIFY COLUMN x Int32; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

CREATE TABLE t_ephemeral_key_value (x Int64, e UInt8 EPHEMERAL 1, m Int64 MATERIALIZED if(e, x % 1000, 0))
ENGINE = MergeTree ORDER BY m;
ALTER TABLE t_ephemeral_key_value MODIFY COLUMN x Int32; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

CREATE TABLE t_ephemeral_key_metadata_only (x Enum8('a' = 1), e UInt8 EPHEMERAL 1, m String MATERIALIZED if(e, toString(x), ''))
ENGINE = MergeTree ORDER BY m;
ALTER TABLE t_ephemeral_key_metadata_only MODIFY COLUMN x Int8; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- Not a key column: allowed, the column keeps its value, as it cannot be recalculated without the EPHEMERAL input.
CREATE TABLE t_ephemeral_not_key (x Int64, e UInt8 EPHEMERAL 1, m Int64 MATERIALIZED if(e, x % 1000, 0), s String)
ENGINE = MergeTree ORDER BY s;
ALTER TABLE t_ephemeral_not_key MODIFY COLUMN x Int32;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_ephemeral_not_key' AND name = 'x';

DROP TABLE t_ephemeral_key_type_name;
DROP TABLE t_ephemeral_key_value;
DROP TABLE t_ephemeral_key_metadata_only;
DROP TABLE t_ephemeral_not_key;

-- Case 6: the altered column is an ALIAS, which casts its value to its type.
DROP TABLE IF EXISTS t_alias_key;
DROP TABLE IF EXISTS t_alias_key_through_subcolumn;
DROP TABLE IF EXISTS t_alias_not_key;
DROP TABLE IF EXISTS t_alias_lambda_parameter;
DROP TABLE IF EXISTS t_alias_dotted_stored_column;

CREATE TABLE t_alias_key (x Int64, a Int64 ALIAS x, m Int64 MATERIALIZED a % 1000) ENGINE = MergeTree ORDER BY m;
INSERT INTO t_alias_key (x) VALUES (5000000001);
ALTER TABLE t_alias_key MODIFY COLUMN a Int32 SETTINGS mutations_sync = 2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT x, a, m, a % 1000 FROM t_alias_key;

-- The alias reached through a subcolumn of another MATERIALIZED column (`m.v` of `m MATERIALIZED tuple(a)`).
CREATE TABLE t_alias_key_through_subcolumn
    (x Int64, a Int64 ALIAS x, m Tuple(v Int64) MATERIALIZED tuple(a), k Int64 MATERIALIZED m.v % 1000)
ENGINE = MergeTree ORDER BY k;
ALTER TABLE t_alias_key_through_subcolumn MODIFY COLUMN a Int32; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- An alias that the key column does not read can still change its type.
CREATE TABLE t_alias_not_key (x Int64, a Int64 ALIAS x, m Int64 MATERIALIZED x % 1000) ENGINE = MergeTree ORDER BY m;
ALTER TABLE t_alias_not_key MODIFY COLUMN a Int32;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_alias_not_key' AND name = 'a';

-- A lambda parameter named like the alias is not a reference to it.
CREATE TABLE t_alias_lambda_parameter (x Int64, a Int64 ALIAS x, m Int64 MATERIALIZED arraySum(arrayMap(a -> a + 1, [1, 2])))
ENGINE = MergeTree ORDER BY m;
ALTER TABLE t_alias_lambda_parameter MODIFY COLUMN a Int32;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_alias_lambda_parameter' AND name = 'a';

-- A stored column named `a.v` next to the alias `a` is not a subcolumn of the alias.
CREATE TABLE t_alias_dotted_stored_column (x Int64, a Int64 ALIAS x, `a.v` Int64, m Int64 MATERIALIZED `a.v`)
ENGINE = MergeTree ORDER BY m;
INSERT INTO t_alias_dotted_stored_column (x, `a.v`) VALUES (5000000001, 7);
ALTER TABLE t_alias_dotted_stored_column MODIFY COLUMN a Int32;
SELECT a, `a.v`, m FROM t_alias_dotted_stored_column;

DROP TABLE t_alias_key;
DROP TABLE t_alias_key_through_subcolumn;
DROP TABLE t_alias_not_key;
DROP TABLE t_alias_lambda_parameter;
DROP TABLE t_alias_dotted_stored_column;

-- Case 7: a lambda argument that shadows a table column is not a dependency on it, so the ALTER is allowed.
DROP TABLE IF EXISTS t_lambda_shadow;
CREATE TABLE t_lambda_shadow (x Int64, m UInt64 MATERIALIZED arraySum(arrayMap(x -> x + 1, [1, 2])))
ENGINE = MergeTree ORDER BY m
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_lambda_shadow (x) VALUES (10);

ALTER TABLE t_lambda_shadow MODIFY COLUMN x Int32 SETTINGS mutations_sync = 2;

SELECT x, m FROM t_lambda_shadow;

DROP TABLE t_lambda_shadow;
