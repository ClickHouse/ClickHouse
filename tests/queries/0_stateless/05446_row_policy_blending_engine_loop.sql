-- a loop table hands the policy filter to the read of its input, so a merging input needs the opt-in

CREATE TABLE t_target (k UInt64, v UInt64) ENGINE = SummingMergeTree ORDER BY k;
CREATE TABLE t_plain (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;

CREATE TABLE t_loop AS loop(currentDatabase(), 't_target');
CREATE TABLE t_loop_function AS loop(merge(currentDatabase(), '^t_target$'));
CREATE TABLE t_loop_plain AS loop(currentDatabase(), 't_plain');

CREATE ROW POLICY p ON t_loop USING v % 2 = 1 TO ALL; -- { serverError BAD_ARGUMENTS }
CREATE ROW POLICY p ON t_loop_function USING v % 2 = 1 TO ALL; -- { serverError BAD_ARGUMENTS }

-- an input which does not merge rows needs nothing
CREATE ROW POLICY p ON t_loop_plain USING v % 2 = 1 TO ALL;

-- the same check runs when a loop table gets the name of a table with a policy
CREATE ROW POLICY p_later ON t_loop_later USING v % 2 = 1 TO ALL;
CREATE TABLE t_loop_later AS loop(currentDatabase(), 't_target'); -- { serverError BAD_ARGUMENTS }
RENAME TABLE t_loop TO t_loop_later; -- { serverError BAD_ARGUMENTS }
DROP ROW POLICY p_later ON t_loop_later;

SET allow_suspicious_row_policies_with_blending_engines = 1;
CREATE ROW POLICY p ON t_loop USING v % 2 = 1 TO ALL;

SELECT table FROM system.row_policies WHERE database = currentDatabase() ORDER BY table;

DROP ROW POLICY p ON t_loop;
DROP ROW POLICY p ON t_loop_plain;
