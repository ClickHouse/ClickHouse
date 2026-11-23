-- A MATERIALIZED key column that also reads an EPHEMERAL column depends on its other sources all the same, so a
-- type change of such a source is rejected, including a conversion that changes only the metadata.

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

-- Not a key column: the ALTER is allowed (the column keeps its value, as it cannot be recalculated without the EPHEMERAL input).
CREATE TABLE t_ephemeral_not_key (x Int64, e UInt8 EPHEMERAL 1, m Int64 MATERIALIZED if(e, x % 1000, 0), s String)
ENGINE = MergeTree ORDER BY s;
ALTER TABLE t_ephemeral_not_key MODIFY COLUMN x Int32;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_ephemeral_not_key' AND name = 'x';

DROP TABLE t_ephemeral_key_type_name;
DROP TABLE t_ephemeral_key_value;
DROP TABLE t_ephemeral_key_metadata_only;
DROP TABLE t_ephemeral_not_key;
