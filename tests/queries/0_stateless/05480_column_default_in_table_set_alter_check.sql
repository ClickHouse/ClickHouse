-- `ALTER ... MODIFY COLUMN` from `Nullable(T)` to `T` checks that the conversion of the existing data is
-- possible, which uses the new `DEFAULT` for `NULL`. When that default reads a table (`x IN rhs`), the check
-- only validates the metadata and must not read `rhs`. The same holds for the conversion check of a projection.
-- `rhs` is a view that throws when it is read.

DROP TABLE IF EXISTS t_alter;
DROP TABLE IF EXISTS t_alter_projection;
DROP VIEW IF EXISTS rhs;

CREATE VIEW rhs AS SELECT toUInt8(throwIf(number = 0, 'rhs is read')) AS p FROM numbers(1);

CREATE TABLE t_alter (p UInt8, f Nullable(UInt8)) ENGINE = MergeTree ORDER BY p;
INSERT INTO t_alter VALUES (1, NULL), (2, 7);
-- The mutation converting the data would read `rhs`, so it is not executed.
SYSTEM STOP MERGES t_alter;

CREATE TABLE t_alter_projection (p UInt8, f Nullable(UInt8), PROJECTION by_f (SELECT f, p ORDER BY f))
    ENGINE = MergeTree ORDER BY p;

ALTER TABLE t_alter MODIFY COLUMN f UInt8 DEFAULT toUInt8(1) IN rhs SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_alter_projection MODIFY COLUMN f UInt8 DEFAULT toUInt8(1) IN rhs SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT table, type, default_expression FROM system.columns
WHERE database = currentDatabase() AND table IN ('t_alter', 't_alter_projection') AND name = 'f'
ORDER BY table;

KILL MUTATION WHERE database = currentDatabase() AND table = 't_alter' FORMAT Null;
DROP TABLE t_alter;
DROP TABLE t_alter_projection;
DROP VIEW rhs;
