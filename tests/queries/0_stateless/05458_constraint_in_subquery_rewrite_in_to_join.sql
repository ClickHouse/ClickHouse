-- A `CHECK` constraint is compiled as a standalone expression. `rewrite_in_to_join` must not turn its
-- `IN (subquery)` into a correlated subquery, which a standalone expression cannot execute.

DROP TABLE IF EXISTS constraint_in_rewrite;
DROP TABLE IF EXISTS constraint_in_rewrite_src;

CREATE TABLE constraint_in_rewrite_src (y UInt64) ENGINE = Memory;
INSERT INTO constraint_in_rewrite_src VALUES (1), (2);

CREATE TABLE constraint_in_rewrite
(
    x UInt64,
    CONSTRAINT c CHECK x IN (SELECT y FROM constraint_in_rewrite_src)
)
ENGINE = Memory;

SET allow_correlated_subqueries = 1, rewrite_in_to_join = 1;

INSERT INTO constraint_in_rewrite VALUES (1);
INSERT INTO constraint_in_rewrite VALUES (2);
INSERT INTO constraint_in_rewrite VALUES (3); -- { error VIOLATED_CONSTRAINT }

SELECT * FROM constraint_in_rewrite ORDER BY x;

DROP TABLE constraint_in_rewrite;
DROP TABLE constraint_in_rewrite_src;
