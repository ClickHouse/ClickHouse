-- Finite bounds whose difference overflows Float64 must give the same buckets as the scaled-down range.
SELECT x, widthBucket(x, -1e308, 1e308, 4), widthBucket(x / 1e300, -1e8, 1e8, 4) FROM (SELECT arrayJoin([-9e307, -1e307, 0, 6e307]) AS x) ORDER BY x;
SELECT widthBucket(9e307, -1e308, 1e308, 4);
SELECT x, widthBucket(x, -1.7976931348623157e308, 1.7976931348623157e308, 10) FROM (SELECT arrayJoin([-1.7976931348623157e308, 0, 1.7976931348623155e308, 1.7976931348623157e308]) AS x) ORDER BY x;
