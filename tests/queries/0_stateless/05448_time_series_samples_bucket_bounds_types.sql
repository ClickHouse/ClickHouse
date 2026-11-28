SET allow_experimental_time_series_table = 1;

-- The columns `min_time` and `max_time` of the samples tables must be `SimpleAggregateFunction(min, ...)` and
-- `SimpleAggregateFunction(max, ...)` of the type of the timestamps: the aggregating engine merges the rows of one series
-- and bucket with these functions, so the bounds of a merged row cover all its samples. A plain or Nullable column
-- would keep the bounds of one insert after a merge, and a wrapper with another function would merge them wrongly.
CREATE TABLE ts ENGINE = TimeSeries SAMPLES INNER COLUMNS (min_time DateTime64(3)); -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE ts ENGINE = TimeSeries SAMPLES INNER COLUMNS (max_time Nullable(DateTime64(3))); -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE ts ENGINE = TimeSeries SAMPLES INNER COLUMNS (min_time SimpleAggregateFunction(min, Nullable(DateTime64(3)))); -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE ts ENGINE = TimeSeries SAMPLES INNER COLUMNS (min_time SimpleAggregateFunction(max, DateTime64(3))); -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE ts ENGINE = TimeSeries SAMPLES INNER COLUMNS (max_time SimpleAggregateFunction(min, DateTime64(3))); -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE ts ENGINE = TimeSeries SAMPLES INNER COLUMNS (min_time SimpleAggregateFunction(min, DateTime64(6))); -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE ts ENGINE = TimeSeries RECENT SAMPLES INNER COLUMNS (min_time DateTime64(3)); -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE ts ENGINE = TimeSeries RECENT SAMPLES INNER COLUMNS (max_time SimpleAggregateFunction(min, DateTime64(3))); -- { serverError BAD_TYPE_OF_FIELD }

-- The generated types can be declared explicitly.
CREATE TABLE ts ENGINE = TimeSeries
    SAMPLES INNER COLUMNS (min_time SimpleAggregateFunction(min, DateTime64(3)), max_time SimpleAggregateFunction(max, DateTime64(3)))
    RECENT SAMPLES INNER COLUMNS (min_time SimpleAggregateFunction(min, DateTime64(3)), max_time SimpleAggregateFunction(max, DateTime64(3)));
SELECT name, type, count() FROM system.columns
WHERE database = currentDatabase() AND (table LIKE '.inner_id.samples.%' OR table LIKE '.inner_id.recentsamples.%') AND name IN ('min_time', 'max_time')
GROUP BY name, type ORDER BY name;
DROP TABLE ts;
