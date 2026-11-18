-- The value is read once per block during execution and grows as the query progresses.
SELECT number, getQueryProfileEvent('SelectedRows') FROM numbers(10) SETTINGS max_block_size = 3, max_threads = 1;

SELECT 'old analyzer';
SELECT number, getQueryProfileEvent('SelectedRows') FROM numbers(5) SETTINGS max_block_size = 2, max_threads = 1, enable_analyzer = 0;

-- Annotate inserted data with the progress of reading.
SELECT 'insert';
DROP TABLE IF EXISTS dst;
CREATE TABLE dst (number UInt64, progress UInt64) ENGINE = MergeTree ORDER BY number;
INSERT INTO dst SELECT number, getQueryProfileEvent('SelectedRows') FROM numbers(100000) SETTINGS max_block_size = 1000, max_threads = 1, max_insert_threads = 1;
SELECT count(), uniqExact(progress), min(progress), max(progress), countIf(progress <= number OR progress > number + 1000) FROM dst;
-- All rows of a block have the same value.
SELECT count() FROM (SELECT intDiv(number, 1000) AS b, uniqExact(progress) AS u FROM dst GROUP BY b) WHERE u != 1;
DROP TABLE dst;

SELECT getQueryProfileEvent('SelectedRowz'); -- { serverError BAD_ARGUMENTS }
SELECT getQueryProfileEvent(toString(number)) FROM numbers(1); -- { serverError ILLEGAL_COLUMN }
SELECT getQueryProfileEvent(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT getQueryProfileEvent(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
