-- The rate-family native-histogram aggregates reject a sample with a NaN zero threshold: reconciling the zero buckets
-- of two samples compares the thresholds until they are equal, and a NaN never is.

SET allow_experimental_time_series_aggregate_functions = 1;

DROP TABLE IF EXISTS hist_samples;
CREATE TABLE hist_samples
(
    name String,
    timestamp UInt32,
    zero_threshold Float64,
    count Float64,
    positive_values Array(Float64)
) ENGINE = Memory;

INSERT INTO hist_samples VALUES
    ('valid', 110, 0.5, 4., [1., 3.]),
    ('valid', 120, 0., 8., [2., 6.]),
    ('nan first', 110, nan, 4., [1., 3.]),
    ('nan first', 120, 0.5, 8., [2., 6.]),
    ('nan second', 110, 0.5, 4., [1., 3.]),
    ('nan second', 120, nan, 8., [2., 6.]);

CREATE VIEW hist_payloads AS
SELECT name, timestamp,
    CAST(tuple(toUInt8(0), toInt8(0), zero_threshold, count, count, 0., [(toInt32(0), toUInt32(2))], positive_values,
          CAST([], 'Array(Tuple(offset Int32, length UInt32))'), CAST([], 'Array(Float64)'), CAST([], 'Array(Float64)'),
          toUInt64(0), toUInt64(0), CAST([], 'Array(UInt64)'), CAST([], 'Array(UInt64)')),
        'Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64), count_int UInt64, zero_count_int UInt64, positive_values_int Array(UInt64), negative_values_int Array(UInt64))') AS payload
FROM hist_samples;

SELECT timeSeriesHistogramDeltaToGrid(130, 130, 10, 45)(timestamp, payload) FROM hist_payloads WHERE name = 'valid';
SELECT timeSeriesHistogramInstantDeltaToGrid(130, 130, 10, 45)(timestamp, payload) FROM hist_payloads WHERE name = 'valid';

SELECT timeSeriesHistogramRateToGrid(130, 130, 10, 45)(timestamp, payload) FROM hist_payloads WHERE name = 'nan first'; -- { serverError INCORRECT_DATA }
SELECT timeSeriesHistogramRateToGrid(130, 130, 10, 45)(timestamp, payload) FROM hist_payloads WHERE name = 'nan second'; -- { serverError INCORRECT_DATA }
SELECT timeSeriesHistogramIncreaseToGrid(130, 130, 10, 45)(timestamp, payload) FROM hist_payloads WHERE name = 'nan first'; -- { serverError INCORRECT_DATA }
SELECT timeSeriesHistogramIncreaseToGrid(130, 130, 10, 45)(timestamp, payload) FROM hist_payloads WHERE name = 'nan second'; -- { serverError INCORRECT_DATA }
SELECT timeSeriesHistogramDeltaToGrid(130, 130, 10, 45)(timestamp, payload) FROM hist_payloads WHERE name = 'nan first'; -- { serverError INCORRECT_DATA }
SELECT timeSeriesHistogramDeltaToGrid(130, 130, 10, 45)(timestamp, payload) FROM hist_payloads WHERE name = 'nan second'; -- { serverError INCORRECT_DATA }
SELECT timeSeriesHistogramInstantRateToGrid(130, 130, 10, 45)(timestamp, payload) FROM hist_payloads WHERE name = 'nan first'; -- { serverError INCORRECT_DATA }
SELECT timeSeriesHistogramInstantRateToGrid(130, 130, 10, 45)(timestamp, payload) FROM hist_payloads WHERE name = 'nan second'; -- { serverError INCORRECT_DATA }
SELECT timeSeriesHistogramInstantDeltaToGrid(130, 130, 10, 45)(timestamp, payload) FROM hist_payloads WHERE name = 'nan first'; -- { serverError INCORRECT_DATA }
SELECT timeSeriesHistogramInstantDeltaToGrid(130, 130, 10, 45)(timestamp, payload) FROM hist_payloads WHERE name = 'nan second'; -- { serverError INCORRECT_DATA }

DROP VIEW hist_payloads;
DROP TABLE hist_samples;
