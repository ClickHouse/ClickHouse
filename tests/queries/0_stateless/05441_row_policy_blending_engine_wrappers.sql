-- a materialized view and a Buffer hand the policy filter to the read of their target, so a merging target needs the opt-in

CREATE TABLE t_target (k UInt64, v UInt64) ENGINE = SummingMergeTree ORDER BY k;
CREATE TABLE t_plain (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_src (k UInt64, v UInt64) ENGINE = Null;

CREATE MATERIALIZED VIEW mv_to TO t_target AS SELECT * FROM t_src;
CREATE MATERIALIZED VIEW mv_inner (k UInt64, v UInt64) ENGINE = SummingMergeTree ORDER BY k AS SELECT * FROM t_src;
CREATE TABLE t_buffer AS t_target ENGINE = Buffer(currentDatabase(), t_target, 1, 0, 0, 1000000, 1000000, 100000000, 100000000);
CREATE MATERIALIZED VIEW mv_plain TO t_plain AS SELECT * FROM t_src;

CREATE ROW POLICY p ON mv_to USING v % 2 = 1 TO ALL; -- { serverError BAD_ARGUMENTS }
CREATE ROW POLICY p ON mv_inner USING v % 2 = 1 TO ALL; -- { serverError BAD_ARGUMENTS }
CREATE ROW POLICY p ON t_buffer USING v % 2 = 1 TO ALL; -- { serverError BAD_ARGUMENTS }

-- a target which does not merge rows needs nothing
CREATE ROW POLICY p ON mv_plain USING v % 2 = 1 TO ALL;

SET allow_suspicious_row_policies_with_blending_engines = 1;
CREATE ROW POLICY p ON mv_to USING v % 2 = 1 TO ALL;

SELECT table FROM system.row_policies WHERE database = currentDatabase() ORDER BY table;

DROP ROW POLICY p ON mv_to;
DROP ROW POLICY p ON mv_plain;
