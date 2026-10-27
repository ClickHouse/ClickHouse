-- A function that keeps a weak reference to its context must still be executable when the actions of a
-- standalone expression (here a `CHECK` constraint) run after the expression was analyzed.

DROP TABLE IF EXISTS t_constraint_function_context;

CREATE TABLE t_constraint_function_context
(
    x UInt64,
    CONSTRAINT c CHECK timeSeriesStoreTags(x, []) >= 0
)
ENGINE = Memory;

INSERT INTO t_constraint_function_context VALUES (1);
SELECT x FROM t_constraint_function_context;

DROP TABLE t_constraint_function_context;
