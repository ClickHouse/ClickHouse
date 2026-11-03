-- Function `timeSeriesSliceSortedArray` accepts an array of tuples with any number of elements whose first element is the timestamp:
-- the `(timestamp, value)` pairs of the `samples` column of a TimeSeries samples table (see 05139_timeseries_slice_sorted_array_function)
-- or the histogram tuples of its `histograms` column. The result keeps the type of the array, the names of the elements included.

SET session_timezone = 'UTC';
SET enable_time_series_table = 1;
SET print_pretty_type_names = 0;

SELECT '-- a tuple of three named elements';
SELECT timeSeriesSliceSortedArray([(100, 1., 'a'), (110, 2., 'b'), (120, 3., 'c'), (130, 4., 'd')]::Array(Tuple(ts UInt32, value Float64, label String)), 105, 120) AS r, toTypeName(r);

SELECT '-- a tuple of one element';
SELECT timeSeriesSliceSortedArray([tuple(100), tuple(110), tuple(120)]::Array(Tuple(UInt32)), 105, 125) AS r, toTypeName(r);

SELECT '-- the histogram tuples of a TimeSeries table keep their type, so the elements are addressed by name';
WITH [(toDateTime64(100, 3), 0, 0, 0, 0., 1., 1, 0, 0., 0., [(0, 1)], [], [], [], []),
      (toDateTime64(110, 3), 0, 0, 0, 0., 1., 2, 0, 0., 0., [(0, 2)], [], [], [], []),
      (toDateTime64(120, 3), 0, 0, 0, 0., 1., 3, 0, 0., 0., [(0, 3)], [], [], [], [])]::Array(Tuple(timestamp DateTime64(3), flags UInt8, counter_reset_hint UInt8, schema Int8, zero_threshold Float64, sum Float64, count_int Int64, zero_count_int Int64, count_float Float64, zero_count_float Float64, positive_int Array(Tuple(index Int32, count Int64)), negative_int Array(Tuple(index Int32, count Int64)), positive_float Array(Tuple(index Int32, count Float64)), negative_float Array(Tuple(index Int32, count Float64)), custom_values Array(Float64))) AS histograms
SELECT arrayMap(h -> (h.timestamp, h.count_int, h.positive_int), timeSeriesSliceSortedArray(histograms, toDateTime64(105, 3), toDateTime64(120, 3)) AS r),
       toTypeName(r) = toTypeName(histograms);

SELECT '-- the whole array is returned when the range covers it, an empty array when it misses it';
WITH [(100, 1, 'a', [1]), (110, 2, 'b', [2])]::Array(Tuple(UInt32, UInt8, String, Array(UInt8))) AS rows
SELECT timeSeriesSliceSortedArray(rows, 0, 200), timeSeriesSliceSortedArray(rows, 200, 300);

SELECT '-- the first element must be a timestamp';
SELECT timeSeriesSliceSortedArray([('a', 1)]::Array(Tuple(String, UInt32)), 0, 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
