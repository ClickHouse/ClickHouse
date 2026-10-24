SELECT sum(sleepEachRow(0.001)) FROM numbers(200) SETTINGS max_block_size=1, max_estimated_execution_time=0.1, timeout_before_checking_execution_speed=0;
-- The addition of `min_execution_speed=1` should not change whether or not `max_estimated_execution_time` gets checked.
SELECT sum(sleepEachRow(0.001)) FROM numbers(200) SETTINGS max_block_size=1, max_estimated_execution_time=0.1, timeout_before_checking_execution_speed=0, min_execution_speed=1;
