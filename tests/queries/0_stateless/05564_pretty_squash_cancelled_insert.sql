-- Case 1: the squash thread must not render a queued block into the sink's buffer after the failed INSERT has cancelled it.
-- A huge interval keeps the thread from writing on its own; a short max wait makes it write on the next wake-up.
SET output_format_pretty_squash_consecutive_ms = 600000, output_format_pretty_squash_max_wait_ms = 100;

-- One row per block: the first block is queued in the format before the second one throws.
INSERT INTO FUNCTION file(currentDatabase() || '_pretty_squash_cancelled_insert.txt', 'PrettySpaceNoEscapes')
SELECT number, throwIf(number = 1 AND sleepEachRow(0.5) = 0, 'boom') FROM numbers(2)
SETTINGS max_block_size = 1; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- Case 2: the squash thread is still writing the first block when the second one throws, so cancelling the format must wait for that write.
SET output_format_pretty_squash_consecutive_ms = 1, output_format_pretty_squash_max_wait_ms = 1000000, output_format_pretty_max_rows = 1000000;

INSERT INTO FUNCTION file(currentDatabase() || '_pretty_squash_cancelled_insert_2.txt', 'PrettySpaceNoEscapes')
SELECT throwIf(number = 100000 AND sleep(0.8) = 0, 'boom') FROM numbers(200000)
SETTINGS max_block_size = 100000, max_threads = 1, engine_file_truncate_on_insert = 1; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

SELECT 'ok';
