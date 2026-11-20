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

-- Inside a materialized view the value is for the INSERT query, not only for the view.
SELECT 'materialized view';
DROP TABLE IF EXISTS mv;
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS t;
CREATE TABLE t (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO t VALUES (1);
CREATE TABLE src (x UInt8) ENGINE = Null;
CREATE TABLE dst (parallel UInt8, parts UInt64, insert_queries UInt64) ENGINE = Memory;
CREATE MATERIALIZED VIEW mv TO dst AS SELECT getSetting('parallel_view_processing') AS parallel, getQueryProfileEvent('SelectedParts') AS parts, getQueryProfileEvent('InsertQuery') AS insert_queries FROM src;
INSERT INTO src SELECT x FROM t SETTINGS parallel_view_processing = 0;
INSERT INTO src SELECT x FROM t SETTINGS parallel_view_processing = 1;
SELECT parallel, parts > 0, insert_queries FROM dst ORDER BY parallel;
DROP TABLE mv;
DROP TABLE src;
DROP TABLE dst;
DROP TABLE t;

SELECT getQueryProfileEvent('SelectedRowz'); -- { serverError BAD_ARGUMENTS }
SELECT getQueryProfileEvent(toString(number)) FROM numbers(1); -- { serverError ILLEGAL_COLUMN }
SELECT getQueryProfileEvent(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT getQueryProfileEvent(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
