-- A reference to a subcolumn of the column itself in a `DEFAULT` written inside a tuple element is a
-- self-reference even if the subcolumn appears only in the finalized type of the column: `Nullable` added
-- by the `NULL` modifier or `data_type_default_nullable` adds the `null` subcolumn, and a later
-- `MODIFY COLUMN` of the same query may change the type while keeping the default.

SET enable_nullable_tuple_type = 1;

DROP TABLE IF EXISTS t_tuple_default_finalized;

CREATE TABLE t_tuple_default_finalized (id UInt8, c Tuple(a UInt8, b UInt8 DEFAULT c.null) NULL) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }
SET data_type_default_nullable = 1;
CREATE TABLE t_tuple_default_finalized (id UInt8, c Tuple(a UInt8, b UInt8 DEFAULT c.null)) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }
SET data_type_default_nullable = 0;
CREATE TABLE t_tuple_default_finalized (id UInt8, c Nullable(Tuple(a UInt8, b UInt8 DEFAULT c.null))) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_tuple_default_finalized (id UInt8) ENGINE = Memory;
ALTER TABLE t_tuple_default_finalized ADD COLUMN c Tuple(a UInt8, b UInt8 DEFAULT c.null) NULL; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_tuple_default_finalized ADD COLUMN c Tuple(a UInt8, b UInt8 DEFAULT c.x), MODIFY COLUMN c Tuple(a UInt8, b UInt8, x UInt8); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_tuple_default_finalized ADD COLUMN c Tuple(a UInt8, b UInt8);
ALTER TABLE t_tuple_default_finalized MODIFY COLUMN c Tuple(a UInt8, b UInt8 DEFAULT c.null) NULL; -- { serverError BAD_ARGUMENTS }

-- An independent column with exactly this name takes precedence over the subcolumn.
ALTER TABLE t_tuple_default_finalized ADD COLUMN `c.null` UInt8 DEFAULT 7;
ALTER TABLE t_tuple_default_finalized MODIFY COLUMN c Tuple(a UInt8, b UInt8 DEFAULT `c.null`) NULL;
INSERT INTO t_tuple_default_finalized (id) VALUES (1);
SELECT id, c, `c.null` FROM t_tuple_default_finalized;

DROP TABLE t_tuple_default_finalized;
