-- Tags: no-replicated-database
-- Tag no-replicated-database: the test needs a mutation between other `ALTER` commands of one query, which `Replicated` databases prohibit.

-- An `ALTER` query is executed as segments of consecutive commands of the same kind, and `AlterCommands::prepare`
-- decides whether a conditional command (`IF EXISTS`) runs by the columns committed before its segment. A mutation
-- between the commands starts a new segment, so a column added before the mutation counts as existing. The columns
-- that tell an independent dotted column from a subcolumn of the column itself, for a reference in a `DEFAULT`
-- written inside a tuple element, must follow the same decisions.

SET flatten_nested = 1;

DROP TABLE IF EXISTS t_tuple_default_after_mutation;

-- The rename runs, so there is a column `c.a` and the reference is to it.
CREATE TABLE t_tuple_default_after_mutation (id UInt8, c Tuple(a UInt8, b UInt8)) ENGINE = Memory;
ALTER TABLE t_tuple_default_after_mutation
    ADD COLUMN `c.z` UInt8,
    UPDATE id = id WHERE 0,
    RENAME COLUMN IF EXISTS `c.z` TO `c.a`,
    MODIFY COLUMN c Tuple(a UInt8, b UInt8 DEFAULT `c.a`);
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_tuple_default_after_mutation' ORDER BY position;
INSERT INTO t_tuple_default_after_mutation (id, `c.a`) VALUES (1, 7);
SELECT c FROM t_tuple_default_after_mutation;
DROP TABLE t_tuple_default_after_mutation;

-- The drop runs, so there is no column `c.a` and the reference is to the element `a` of `c` itself.
CREATE TABLE t_tuple_default_after_mutation (id UInt8, c Tuple(a UInt8, b UInt8)) ENGINE = Memory;
ALTER TABLE t_tuple_default_after_mutation
    ADD COLUMN `c.a` UInt8,
    UPDATE id = id WHERE 0,
    DROP COLUMN IF EXISTS `c.a`,
    MODIFY COLUMN c Tuple(a UInt8, b UInt8 DEFAULT `c.a`); -- { serverError BAD_ARGUMENTS }
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_tuple_default_after_mutation' ORDER BY position;
DROP TABLE t_tuple_default_after_mutation;
