-- The clauses of the targets of a TimeSeries table may be written in any order: the AST must not depend on it, otherwise the
-- formatted query parses back to a different AST.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts;

-- `INNER ENGINE` before `INNER COLUMNS`.
CREATE TABLE ts ENGINE = TimeSeries
    RECENT SAMPLES INNER ENGINE = AggregatingMergeTree SETTINGS allow_dimensions_outside_sorting_key = 1
    RECENT SAMPLES INNER COLUMNS (extra UInt8 DEFAULT 7);
SELECT name, type, default_expression FROM system.columns
WHERE database = currentDatabase() AND table LIKE '.inner\_id.recentsamples.%' AND name = 'extra';
DROP TABLE ts;

-- Both orders give the same AST.
EXPLAIN AST CREATE TABLE ts ENGINE = TimeSeries RECENT SAMPLES INNER ENGINE = MergeTree RECENT SAMPLES INNER COLUMNS (extra UInt8);
EXPLAIN AST CREATE TABLE ts ENGINE = TimeSeries RECENT SAMPLES INNER COLUMNS (extra UInt8) RECENT SAMPLES INNER ENGINE = MergeTree;

-- The clauses of `TAGS` separated by a clause of `METRIC FAMILIES`, and the same definition in the formatted order.
EXPLAIN AST CREATE TABLE ts ENGINE = TimeSeries TAGS INNER COLUMNS (extra UInt8) METRIC FAMILIES INNER ENGINE = MergeTree TAGS INNER ENGINE = ReplacingMergeTree;
EXPLAIN AST CREATE TABLE ts ENGINE = TimeSeries TAGS INNER COLUMNS (extra UInt8) TAGS INNER ENGINE = ReplacingMergeTree METRIC FAMILIES INNER ENGINE = MergeTree;
