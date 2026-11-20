-- The query SETTINGS after `CREATE TABLE ... ENGINE = ... AS table` stay query settings when the query is formatted.

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;

CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;

SELECT formatQuerySingleLine('CREATE TABLE dst ENGINE = MergeTree ORDER BY x AS src SETTINGS max_partitions_to_read = 1');
SELECT formatQuerySingleLine('CREATE TABLE dst ENGINE = MergeTree ORDER BY x EMPTY AS src SETTINGS max_threads = 1');
SELECT formatQuerySingleLine('CREATE TABLE dst ENGINE = MergeTree ORDER BY x CLONE AS src SETTINGS max_threads = 1');
SELECT formatQuerySingleLine('CREATE TABLE dst ENGINE = TimeSeries DATA ENGINE = MergeTree ORDER BY (id, timestamp) AS src SETTINGS max_threads = 1');

-- Unchanged: no query settings, the storage has its own SETTINGS, or COMMENT / FORMAT is in between.
SELECT formatQuerySingleLine('CREATE TABLE dst ENGINE = MergeTree ORDER BY x AS src');
SELECT formatQuerySingleLine('CREATE TABLE dst ENGINE = MergeTree ORDER BY x COMMENT ''c'' AS src SETTINGS max_threads = 1');
SELECT formatQuerySingleLine('CREATE TABLE dst ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 128 AS src SETTINGS max_threads = 1');
SELECT formatQuerySingleLine('CREATE TABLE dst ENGINE = MergeTree ORDER BY x AS src FORMAT Null SETTINGS max_threads = 1');

CREATE TABLE dst ENGINE = MergeTree ORDER BY x AS src SETTINGS max_partitions_to_read = 1;

DROP TABLE dst;
DROP TABLE src;
