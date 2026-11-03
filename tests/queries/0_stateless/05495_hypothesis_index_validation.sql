-- exactly one expression
CREATE TABLE t_hypothesis_invalid (c0 Int, INDEX i0 (c0, c0) TYPE hypothesis) ENGINE = MergeTree ORDER BY tuple() SETTINGS allow_suspicious_indices = 1; -- { serverError INCORRECT_QUERY }
-- the expression must be of an integer type
CREATE TABLE t_hypothesis_invalid (c0 Float64, INDEX i0 c0 TYPE hypothesis) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_QUERY }
CREATE TABLE t_hypothesis_invalid (c0 String, INDEX i0 c0 TYPE hypothesis) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_QUERY }
CREATE TABLE t_hypothesis_invalid (c0 LowCardinality(UInt8), INDEX i0 c0 TYPE hypothesis) ENGINE = MergeTree ORDER BY tuple() SETTINGS allow_suspicious_low_cardinality_types = 1; -- { serverError INCORRECT_QUERY }
-- the expression must not contradict itself
CREATE TABLE t_hypothesis_invalid (a UInt8, b UInt8, INDEX i0 (a < b AND b < a) TYPE hypothesis) ENGINE = MergeTree ORDER BY tuple(); -- { serverError VIOLATED_CONSTRAINT }
CREATE TABLE t_hypothesis_valid (c0 Float64, c1 Bool, c2 Nullable(UInt64), INDEX i0 c0 < 1.5 TYPE hypothesis, INDEX i1 c1 TYPE hypothesis, INDEX i2 c2 < 10 TYPE hypothesis) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_hypothesis_valid VALUES (1.0, true, NULL), (2.0, false, 5);
SELECT count() FROM t_hypothesis_valid WHERE c2 >= 10;
DROP TABLE t_hypothesis_valid;
