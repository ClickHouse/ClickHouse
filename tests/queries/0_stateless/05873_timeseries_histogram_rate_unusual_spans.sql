-- The rate-family native-histogram aggregates reject samples whose spans the kernel cannot walk:
-- a later span with a negative offset, bucket indices outside the custom bounds, and bucket indices too far apart.
-- Spans of length zero are valid and only shift the following span.

SET allow_experimental_time_series_aggregate_functions = 1;

DROP TABLE IF EXISTS hist_samples;
CREATE TABLE hist_samples
(
    name String,
    timestamp UInt32,
    `schema` Int8,
    count Float64,
    positive_spans Array(Tuple(offset Int32, length UInt32)),
    positive_values Array(Float64),
    custom_values Array(Float64)
) ENGINE = Memory;

INSERT INTO hist_samples VALUES
    ('valid', 110, 0, 1., [(-1000, 1)], [1.], []),
    ('valid', 120, 0, 3., [(-1000, 1), (2000, 1)], [1., 2.], []),
    ('empty first span', 110, 0, 1., [(1, 1)], [1.], []),
    ('empty first span', 120, 0, 7., [(1, 0), (1, 2)], [3., 4.], []),
    ('only an empty span', 110, 0, 1., [(1, 1)], [1.], []),
    ('only an empty span', 120, 0, 0., [(1, 0)], [], []),
    ('negative later offset', 110, 0, 2., [(5, 1), (-10, 1)], [1., 1.], []),
    ('negative later offset', 120, 0, 1., [(0, 1)], [1.], []),
    ('index below the custom bounds', 110, -53, 1., [(-5, 1)], [1.], [1., 2.]),
    ('index below the custom bounds', 120, -53, 1., [(0, 1)], [1.], [1., 3.]),
    ('index above the custom bounds', 110, -53, 1., [(3, 1)], [1.], [1., 2.]),
    ('index above the custom bounds', 120, -53, 1., [(0, 1)], [1.], [1., 3.]),
    ('indices too far apart', 110, 0, 1., [(-1000, 1)], [1.], []),
    ('indices too far apart', 120, 0, 2., [(2147483647, 1)], [2.], []);

CREATE VIEW hist_payloads AS
SELECT name, timestamp,
    tuple(toUInt8(0), `schema`, 0., count, count, 0., positive_spans, positive_values,
          CAST([], 'Array(Tuple(offset Int32, length UInt32))'), CAST([], 'Array(Float64)'), custom_values,
          toUInt64(count), toUInt64(0), arrayMap(v -> toUInt64(v), positive_values), CAST([], 'Array(UInt64)')) AS payload
FROM hist_samples;

SELECT timeSeriesHistogramDeltaToGrid(130, 130, 10, 45)(timestamp, payload) FROM hist_payloads WHERE name = 'valid';
SELECT timeSeriesHistogramDeltaToGrid(130, 130, 10, 45)(timestamp, payload) FROM hist_payloads WHERE name = 'empty first span';
SELECT timeSeriesHistogramDeltaToGrid(130, 130, 10, 45)(timestamp, payload) FROM hist_payloads WHERE name = 'only an empty span';
SELECT timeSeriesHistogramDeltaToGrid(130, 130, 10, 45)(timestamp, payload) FROM hist_payloads WHERE name = 'negative later offset'; -- { serverError INCORRECT_DATA }
SELECT timeSeriesHistogramDeltaToGrid(130, 130, 10, 45)(timestamp, payload) FROM hist_payloads WHERE name = 'index below the custom bounds'; -- { serverError INCORRECT_DATA }
SELECT timeSeriesHistogramDeltaToGrid(130, 130, 10, 45)(timestamp, payload) FROM hist_payloads WHERE name = 'index above the custom bounds'; -- { serverError INCORRECT_DATA }
SELECT timeSeriesHistogramDeltaToGrid(130, 130, 10, 45)(timestamp, payload) FROM hist_payloads WHERE name = 'indices too far apart'; -- { serverError INCORRECT_DATA }
SELECT timeSeriesHistogramInstantDeltaToGrid(130, 130, 10, 45)(timestamp, payload) FROM hist_payloads WHERE name = 'negative later offset'; -- { serverError INCORRECT_DATA }

DROP VIEW hist_payloads;
DROP TABLE hist_samples;
