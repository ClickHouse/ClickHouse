-- Tags: distributed

-- In-order input and shuffled input, including one duplicate timestamp, must agree for every
-- timeSeries*ToGrid function. A dense step puts about one sample in each bucket. A sparse step
-- leaves most buckets empty. A merge of one sorted partial state and one unsorted partial state
-- must equal the aggregation of the whole series.

SET allow_experimental_time_series_aggregate_functions = 1;

WITH
    arrayMap(i -> toDateTime64(1000 + i * 15, 3, 'UTC'), range(40)) AS ts0,
    arrayMap(i -> toFloat64((i * 3) % 11), range(40)) AS vals0,
    arrayConcat(ts0, [ts0[10]]) AS ts,
    arrayConcat(vals0, [vals0[10] - 1]) AS vals,
    arrayReverse(ts) AS shuffled_ts,
    arrayReverse(vals) AS shuffled_vals
SELECT 'dense step, in-order input equals shuffled input:';

WITH
    arrayMap(i -> toDateTime64(1000 + i * 15, 3, 'UTC'), range(40)) AS ts0,
    arrayMap(i -> toFloat64((i * 3) % 11), range(40)) AS vals0,
    arrayConcat(ts0, [ts0[10]]) AS ts,
    arrayConcat(vals0, [vals0[10] - 1]) AS vals,
    arrayReverse(ts) AS shuffled_ts,
    arrayReverse(vals) AS shuffled_vals
SELECT
    [
        timeSeriesRateToGrid(1000, 1600, 15, 60)(shuffled_ts, shuffled_vals) = timeSeriesRateToGrid(1000, 1600, 15, 60)(ts, vals),
        timeSeriesIncreaseToGrid(1000, 1600, 15, 60)(shuffled_ts, shuffled_vals) = timeSeriesIncreaseToGrid(1000, 1600, 15, 60)(ts, vals),
        timeSeriesDeltaToGrid(1000, 1600, 15, 60)(shuffled_ts, shuffled_vals) = timeSeriesDeltaToGrid(1000, 1600, 15, 60)(ts, vals),
        timeSeriesChangesToGrid(1000, 1600, 15, 60)(shuffled_ts, shuffled_vals) = timeSeriesChangesToGrid(1000, 1600, 15, 60)(ts, vals),
        timeSeriesResetsToGrid(1000, 1600, 15, 60)(shuffled_ts, shuffled_vals) = timeSeriesResetsToGrid(1000, 1600, 15, 60)(ts, vals),
        timeSeriesDerivToGrid(1000, 1600, 15, 60)(shuffled_ts, shuffled_vals) = timeSeriesDerivToGrid(1000, 1600, 15, 60)(ts, vals),
        timeSeriesPredictLinearToGrid(1000, 1600, 15, 60, 30)(shuffled_ts, shuffled_vals) = timeSeriesPredictLinearToGrid(1000, 1600, 15, 60, 30)(ts, vals),
        timeSeriesLinearRegressionToGrid(1000, 1600, 15, 60)(shuffled_ts, shuffled_vals) = timeSeriesLinearRegressionToGrid(1000, 1600, 15, 60)(ts, vals),
        timeSeriesLastToGrid(1000, 1600, 15, 60)(shuffled_ts, shuffled_vals) = timeSeriesLastToGrid(1000, 1600, 15, 60)(ts, vals),
        timeSeriesTimestampOfLastToGrid(1000, 1600, 15, 60)(shuffled_ts, shuffled_vals) = timeSeriesTimestampOfLastToGrid(1000, 1600, 15, 60)(ts, vals),
        timeSeriesFirstToGrid(1000, 1600, 15, 60)(shuffled_ts, shuffled_vals) = timeSeriesFirstToGrid(1000, 1600, 15, 60)(ts, vals),
        timeSeriesTimestampOfFirstToGrid(1000, 1600, 15, 60)(shuffled_ts, shuffled_vals) = timeSeriesTimestampOfFirstToGrid(1000, 1600, 15, 60)(ts, vals),
        timeSeriesMaxToGrid(1000, 1600, 15, 60)(shuffled_ts, shuffled_vals) = timeSeriesMaxToGrid(1000, 1600, 15, 60)(ts, vals),
        timeSeriesTimestampOfMaxToGrid(1000, 1600, 15, 60)(shuffled_ts, shuffled_vals) = timeSeriesTimestampOfMaxToGrid(1000, 1600, 15, 60)(ts, vals),
        timeSeriesMinToGrid(1000, 1600, 15, 60)(shuffled_ts, shuffled_vals) = timeSeriesMinToGrid(1000, 1600, 15, 60)(ts, vals),
        timeSeriesTimestampOfMinToGrid(1000, 1600, 15, 60)(shuffled_ts, shuffled_vals) = timeSeriesTimestampOfMinToGrid(1000, 1600, 15, 60)(ts, vals),
        timeSeriesSumToGrid(1000, 1600, 15, 60)(shuffled_ts, shuffled_vals) = timeSeriesSumToGrid(1000, 1600, 15, 60)(ts, vals),
        timeSeriesAvgToGrid(1000, 1600, 15, 60)(shuffled_ts, shuffled_vals) = timeSeriesAvgToGrid(1000, 1600, 15, 60)(ts, vals),
        timeSeriesCountToGrid(1000, 1600, 15, 60)(shuffled_ts, shuffled_vals) = timeSeriesCountToGrid(1000, 1600, 15, 60)(ts, vals),
        timeSeriesStddevToGrid(1000, 1600, 15, 60)(shuffled_ts, shuffled_vals) = timeSeriesStddevToGrid(1000, 1600, 15, 60)(ts, vals),
        timeSeriesStdvarToGrid(1000, 1600, 15, 60)(shuffled_ts, shuffled_vals) = timeSeriesStdvarToGrid(1000, 1600, 15, 60)(ts, vals),
        timeSeriesInstantRateToGrid(1000, 1600, 15, 60)(shuffled_ts, shuffled_vals) = timeSeriesInstantRateToGrid(1000, 1600, 15, 60)(ts, vals),
        timeSeriesInstantDeltaToGrid(1000, 1600, 15, 60)(shuffled_ts, shuffled_vals) = timeSeriesInstantDeltaToGrid(1000, 1600, 15, 60)(ts, vals),
        timeSeriesPresentToGrid(1000, 1600, 15, 60)(shuffled_ts, shuffled_vals) = timeSeriesPresentToGrid(1000, 1600, 15, 60)(ts, vals),
        timeSeriesQuantileToGrid(1000, 1600, 15, 60)(shuffled_ts, shuffled_vals, 0.5) = timeSeriesQuantileToGrid(1000, 1600, 15, 60)(ts, vals, 0.5),
        timeSeriesMadToGrid(1000, 1600, 15, 60)(shuffled_ts, shuffled_vals) = timeSeriesMadToGrid(1000, 1600, 15, 60)(ts, vals)
    ];

WITH
    arrayMap(i -> toDateTime64(1000 + i * 15, 3, 'UTC'), range(40)) AS ts0,
    arrayMap(i -> toFloat64((i * 3) % 11), range(40)) AS vals0,
    arrayConcat(ts0, [ts0[10]]) AS ts,
    arrayConcat(vals0, [vals0[10] - 1]) AS vals,
    arrayReverse(ts) AS shuffled_ts,
    arrayReverse(vals) AS shuffled_vals
SELECT 'sparse step, in-order input equals shuffled input:';

WITH
    arrayMap(i -> toDateTime64(1000 + i * 15, 3, 'UTC'), range(40)) AS ts0,
    arrayMap(i -> toFloat64((i * 3) % 11), range(40)) AS vals0,
    arrayConcat(ts0, [ts0[10]]) AS ts,
    arrayConcat(vals0, [vals0[10] - 1]) AS vals,
    arrayReverse(ts) AS shuffled_ts,
    arrayReverse(vals) AS shuffled_vals
SELECT
    [
        timeSeriesRateToGrid(1000, 1600, 300, 60)(shuffled_ts, shuffled_vals) = timeSeriesRateToGrid(1000, 1600, 300, 60)(ts, vals),
        timeSeriesIncreaseToGrid(1000, 1600, 300, 60)(shuffled_ts, shuffled_vals) = timeSeriesIncreaseToGrid(1000, 1600, 300, 60)(ts, vals),
        timeSeriesDeltaToGrid(1000, 1600, 300, 60)(shuffled_ts, shuffled_vals) = timeSeriesDeltaToGrid(1000, 1600, 300, 60)(ts, vals),
        timeSeriesChangesToGrid(1000, 1600, 300, 60)(shuffled_ts, shuffled_vals) = timeSeriesChangesToGrid(1000, 1600, 300, 60)(ts, vals),
        timeSeriesResetsToGrid(1000, 1600, 300, 60)(shuffled_ts, shuffled_vals) = timeSeriesResetsToGrid(1000, 1600, 300, 60)(ts, vals),
        timeSeriesDerivToGrid(1000, 1600, 300, 60)(shuffled_ts, shuffled_vals) = timeSeriesDerivToGrid(1000, 1600, 300, 60)(ts, vals),
        timeSeriesPredictLinearToGrid(1000, 1600, 300, 60, 30)(shuffled_ts, shuffled_vals) = timeSeriesPredictLinearToGrid(1000, 1600, 300, 60, 30)(ts, vals),
        timeSeriesLinearRegressionToGrid(1000, 1600, 300, 60)(shuffled_ts, shuffled_vals) = timeSeriesLinearRegressionToGrid(1000, 1600, 300, 60)(ts, vals),
        timeSeriesLastToGrid(1000, 1600, 300, 60)(shuffled_ts, shuffled_vals) = timeSeriesLastToGrid(1000, 1600, 300, 60)(ts, vals),
        timeSeriesTimestampOfLastToGrid(1000, 1600, 300, 60)(shuffled_ts, shuffled_vals) = timeSeriesTimestampOfLastToGrid(1000, 1600, 300, 60)(ts, vals),
        timeSeriesFirstToGrid(1000, 1600, 300, 60)(shuffled_ts, shuffled_vals) = timeSeriesFirstToGrid(1000, 1600, 300, 60)(ts, vals),
        timeSeriesTimestampOfFirstToGrid(1000, 1600, 300, 60)(shuffled_ts, shuffled_vals) = timeSeriesTimestampOfFirstToGrid(1000, 1600, 300, 60)(ts, vals),
        timeSeriesMaxToGrid(1000, 1600, 300, 60)(shuffled_ts, shuffled_vals) = timeSeriesMaxToGrid(1000, 1600, 300, 60)(ts, vals),
        timeSeriesTimestampOfMaxToGrid(1000, 1600, 300, 60)(shuffled_ts, shuffled_vals) = timeSeriesTimestampOfMaxToGrid(1000, 1600, 300, 60)(ts, vals),
        timeSeriesMinToGrid(1000, 1600, 300, 60)(shuffled_ts, shuffled_vals) = timeSeriesMinToGrid(1000, 1600, 300, 60)(ts, vals),
        timeSeriesTimestampOfMinToGrid(1000, 1600, 300, 60)(shuffled_ts, shuffled_vals) = timeSeriesTimestampOfMinToGrid(1000, 1600, 300, 60)(ts, vals),
        timeSeriesSumToGrid(1000, 1600, 300, 60)(shuffled_ts, shuffled_vals) = timeSeriesSumToGrid(1000, 1600, 300, 60)(ts, vals),
        timeSeriesAvgToGrid(1000, 1600, 300, 60)(shuffled_ts, shuffled_vals) = timeSeriesAvgToGrid(1000, 1600, 300, 60)(ts, vals),
        timeSeriesCountToGrid(1000, 1600, 300, 60)(shuffled_ts, shuffled_vals) = timeSeriesCountToGrid(1000, 1600, 300, 60)(ts, vals),
        timeSeriesStddevToGrid(1000, 1600, 300, 60)(shuffled_ts, shuffled_vals) = timeSeriesStddevToGrid(1000, 1600, 300, 60)(ts, vals),
        timeSeriesStdvarToGrid(1000, 1600, 300, 60)(shuffled_ts, shuffled_vals) = timeSeriesStdvarToGrid(1000, 1600, 300, 60)(ts, vals),
        timeSeriesInstantRateToGrid(1000, 1600, 300, 60)(shuffled_ts, shuffled_vals) = timeSeriesInstantRateToGrid(1000, 1600, 300, 60)(ts, vals),
        timeSeriesInstantDeltaToGrid(1000, 1600, 300, 60)(shuffled_ts, shuffled_vals) = timeSeriesInstantDeltaToGrid(1000, 1600, 300, 60)(ts, vals),
        timeSeriesPresentToGrid(1000, 1600, 300, 60)(shuffled_ts, shuffled_vals) = timeSeriesPresentToGrid(1000, 1600, 300, 60)(ts, vals),
        timeSeriesQuantileToGrid(1000, 1600, 300, 60)(shuffled_ts, shuffled_vals, 0.5) = timeSeriesQuantileToGrid(1000, 1600, 300, 60)(ts, vals, 0.5),
        timeSeriesMadToGrid(1000, 1600, 300, 60)(shuffled_ts, shuffled_vals) = timeSeriesMadToGrid(1000, 1600, 300, 60)(ts, vals)
    ];

SELECT 'merge of a sorted partial state and an unsorted partial state:';

WITH
    arrayMap(i -> toDateTime64(1000 + i * 15, 3, 'UTC'), range(40)) AS ts0,
    arrayMap(i -> toFloat64((i * 3) % 11), range(40)) AS vals0,
    arrayConcat(ts0, [ts0[10]]) AS ts,
    arrayConcat(vals0, [vals0[10] - 1]) AS vals
SELECT
    [
        timeSeriesRateToGridMerge(1000, 1600, 15, 60)(st) = timeSeriesRateToGrid(1000, 1600, 15, 60)(ts, vals),
        timeSeriesQuantileToGridMerge(1000, 1600, 15, 60)(st_q) = timeSeriesQuantileToGrid(1000, 1600, 15, 60)(ts, vals, 0.5)
    ]
FROM
(
    SELECT
        initializeAggregation('timeSeriesRateToGridState(1000, 1600, 15, 60)', part_ts, part_vals) AS st,
        initializeAggregation('timeSeriesQuantileToGridState(1000, 1600, 15, 60)', part_ts, part_vals, 0.5) AS st_q
    FROM
    (
        SELECT part_ts, part_vals
        FROM
        (
            SELECT 1 AS ord, arraySlice(ts, 1, 20) AS part_ts, arraySlice(vals, 1, 20) AS part_vals
            UNION ALL
            SELECT 2, arrayReverse(arraySlice(ts, 21)), arrayReverse(arraySlice(vals, 21))
        )
        ORDER BY ord
    )
)
SETTINGS max_threads = 1;
