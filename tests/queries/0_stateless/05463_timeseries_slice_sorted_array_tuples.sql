-- Function `timeSeriesSliceSortedArray` accepts an array of tuples with any number of elements whose first element is the timestamp:
-- the `(timestamp, value)` pairs of the `samples` column of a TimeSeries samples table (see 05139_timeseries_slice_sorted_array_function)
-- or the histogram tuples of its `histograms` column (sliced by `timeSeriesSelector` in 05462_timeseries_selector_histograms).
-- The result keeps the type of the array, the names of the elements included.

SET session_timezone = 'UTC';
SET enable_time_series_table = 1;
SET print_pretty_type_names = 0;

SELECT '-- a tuple of three named elements';
SELECT timeSeriesSliceSortedArray([(100, 1., 'a'), (110, 2., 'b'), (120, 3., 'c'), (130, 4., 'd')]::Array(Tuple(ts UInt32, value Float64, label String)), 105, 120) AS r, toTypeName(r);

SELECT '-- a tuple of one element';
SELECT timeSeriesSliceSortedArray([tuple(100), tuple(110), tuple(120)]::Array(Tuple(UInt32)), 105, 125) AS r, toTypeName(r);

SELECT '-- the first element must be a timestamp';
SELECT timeSeriesSliceSortedArray([('a', 1)]::Array(Tuple(String, UInt32)), 0, 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
