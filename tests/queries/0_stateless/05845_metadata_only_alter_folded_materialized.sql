-- a materialized expression folded to a constant does not depend on the column, so the metadata-only alter is safe

DROP TABLE IF EXISTS t_folded;

CREATE TABLE t_folded (t Tuple(a Int64), m UInt8 MATERIALIZED (1 OR length(toString(t)) > 0))
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_folded (t) VALUES ((1));

ALTER TABLE t_folded MODIFY COLUMN t Tuple(a Int64, b String) SETTINGS allow_metadata_only_named_tuple_alter = 1;

SELECT t, m FROM t_folded;

DROP TABLE t_folded;
