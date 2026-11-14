-- The optional `native_histograms` argument of timeSeriesMetricTypeToSuffixes: the histogram types also get the empty suffix,
-- because a native histogram is one series named like its family (04269 covers the members of the classic types).
SELECT timeSeriesMetricTypeToSuffixes('histogram', true);
SELECT timeSeriesMetricTypeToSuffixes('gaugehistogram', true);
SELECT timeSeriesMetricTypeToSuffixes('histogram', false);

-- The other types don't depend on the argument.
SELECT timeSeriesMetricTypeToSuffixes('counter', true);
SELECT timeSeriesMetricTypeToSuffixes('summary', true);

-- The argument must be a constant.
SELECT timeSeriesMetricTypeToSuffixes('histogram', materialize(true)); -- { serverError ILLEGAL_COLUMN }

-- Works over a column of types.
SELECT type, timeSeriesMetricTypeToSuffixes(type, true) AS suffixes
FROM values('type String', 'counter', 'gauge', 'histogram', 'summary')
ORDER BY type;
