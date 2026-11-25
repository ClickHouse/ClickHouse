-- The legacy expression analysis (`TreeRewriter`, used for constraints) rejects the `UNIQUE`
-- predicate, but must not reserve the name of its internal function: an ordinary call `__unique(...)`
-- goes through the usual function resolution and fails as an unknown function.
DROP TABLE IF EXISTS t_unique_ordinary_call;
CREATE TABLE t_unique_ordinary_call (a UInt32, CONSTRAINT c CHECK __unique(a) > 1) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_unique_ordinary_call VALUES (1); -- { serverError UNKNOWN_FUNCTION }
DROP TABLE t_unique_ordinary_call;

-- The predicate itself is rejected in a constraint.
CREATE TABLE t_unique_predicate_constraint (a UInt32, CONSTRAINT c CHECK UNIQUE(SELECT 1) = 1) ENGINE = MergeTree ORDER BY a; -- { serverError NOT_IMPLEMENTED }

SELECT 'ok';
