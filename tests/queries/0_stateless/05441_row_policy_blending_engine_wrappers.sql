-- a materialized view and a Buffer hand the policy filter to the read of their target, so a merging target needs the opt-in

CREATE TABLE t_target (k UInt64, v UInt64) ENGINE = SummingMergeTree ORDER BY k;
CREATE TABLE t_plain (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_src (k UInt64, v UInt64) ENGINE = Null;

CREATE MATERIALIZED VIEW mv_to TO t_target AS SELECT * FROM t_src;
CREATE MATERIALIZED VIEW mv_inner (k UInt64, v UInt64) ENGINE = SummingMergeTree ORDER BY k AS SELECT * FROM t_src;
CREATE TABLE t_buffer AS t_target ENGINE = Buffer(currentDatabase(), t_target, 1, 0, 0, 1000000, 1000000, 100000000, 100000000);
CREATE MATERIALIZED VIEW mv_plain TO t_plain AS SELECT * FROM t_src;
CREATE TABLE t_merge AS t_target ENGINE = Merge(currentDatabase(), '^t_target$');
CREATE TABLE t_merge_outer AS t_target ENGINE = Merge(currentDatabase(), '^t_merge$');
CREATE TABLE t_merge_plain AS t_plain ENGINE = Merge(currentDatabase(), '^t_plain$');

CREATE ROW POLICY p ON mv_to USING v % 2 = 1 TO ALL; -- { serverError BAD_ARGUMENTS }
CREATE ROW POLICY p ON mv_inner USING v % 2 = 1 TO ALL; -- { serverError BAD_ARGUMENTS }
CREATE ROW POLICY p ON t_buffer USING v % 2 = 1 TO ALL; -- { serverError BAD_ARGUMENTS }

-- a merge table hands the policy filter to the read of its sources too
CREATE ROW POLICY p ON t_merge USING v % 2 = 1 TO ALL; -- { serverError BAD_ARGUMENTS }
CREATE ROW POLICY p ON t_merge_outer USING v % 2 = 1 TO ALL; -- { serverError BAD_ARGUMENTS }

-- a target which does not merge rows needs nothing
CREATE ROW POLICY p ON mv_plain USING v % 2 = 1 TO ALL;
CREATE ROW POLICY p ON t_merge_plain USING v % 2 = 1 TO ALL;

-- a table function creates its storage on first use, and the check follows that storage
CREATE TABLE t_function AS merge(currentDatabase(), '^t_target$');
CREATE ROW POLICY p ON t_function USING v % 2 = 1 TO ALL; -- { serverError BAD_ARGUMENTS }
CREATE ROW POLICY p_later ON t_function_later USING v % 2 = 1 TO ALL;
CREATE TABLE t_function_later AS merge(currentDatabase(), '^t_target$'); -- { serverError BAD_ARGUMENTS }
RENAME TABLE t_function TO t_function_later; -- { serverError BAD_ARGUMENTS }
DROP ROW POLICY p_later ON t_function_later;

-- a policy on a view filters the rows of its query, which can read a merging table directly or in a subquery or another view
CREATE VIEW v_final AS SELECT * FROM t_target FINAL;
CREATE VIEW v_subquery AS SELECT * FROM t_plain WHERE k IN (SELECT k FROM t_plain) UNION ALL SELECT * FROM (SELECT * FROM t_target);
CREATE VIEW v_outer AS SELECT * FROM v_final;
CREATE VIEW v_plain AS SELECT * FROM t_plain;
CREATE ROW POLICY p ON v_final USING v % 2 = 1 TO ALL; -- { serverError BAD_ARGUMENTS }
CREATE ROW POLICY p ON v_subquery USING v % 2 = 1 TO ALL; -- { serverError BAD_ARGUMENTS }
CREATE ROW POLICY p ON v_outer USING v % 2 = 1 TO ALL; -- { serverError BAD_ARGUMENTS }
CREATE ROW POLICY p ON v_plain USING v % 2 = 1 TO ALL;
CREATE ROW POLICY p_later ON v_later USING v % 2 = 1 TO ALL;
CREATE VIEW v_later AS SELECT * FROM t_target; -- { serverError BAD_ARGUMENTS }
CREATE TABLE v_later AS view(SELECT * FROM {CLICKHOUSE_DATABASE:Identifier}.t_target); -- { serverError BAD_ARGUMENTS }
RENAME TABLE v_final TO v_later; -- { serverError BAD_ARGUMENTS }
DROP ROW POLICY p_later ON v_later;

SET allow_suspicious_row_policies_with_blending_engines = 1;
CREATE ROW POLICY p ON mv_to USING v % 2 = 1 TO ALL;

SELECT table FROM system.row_policies WHERE database = currentDatabase() ORDER BY table;

DROP ROW POLICY p ON mv_to;
DROP ROW POLICY p ON mv_plain;
DROP ROW POLICY p ON t_merge_plain;
DROP ROW POLICY p ON v_plain;
