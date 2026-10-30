-- A sorted subquery seen through a decreasing function (`negate`, `/ -2`) is sorted in the opposite
-- direction, but its NaN/NULL stay at the same end of the stream. The propagated sort property flipped
-- only `direction`, so an outer ORDER BY asking for the other NaN/NULL placement was elided as satisfied.

SET optimize_sorting_by_input_stream_properties = 1;

DROP TABLE IF EXISTS t_sort_prop_nan;
CREATE TABLE t_sort_prop_nan (x Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_sort_prop_nan VALUES (1), (nan), (2);

SELECT 'negate of an ascending subquery, DESC NULLS FIRST';
SELECT -x AS n FROM (SELECT x FROM t_sort_prop_nan ORDER BY x LIMIT 10) ORDER BY n DESC NULLS FIRST;
SELECT -x AS n FROM (SELECT x FROM t_sort_prop_nan ORDER BY x LIMIT 10) ORDER BY n DESC NULLS FIRST SETTINGS optimize_sorting_by_input_stream_properties = 0;

SELECT 'negate of a descending subquery, ASC NULLS FIRST';
SELECT -x AS n FROM (SELECT x FROM t_sort_prop_nan ORDER BY x DESC LIMIT 10) ORDER BY n ASC NULLS FIRST;
SELECT -x AS n FROM (SELECT x FROM t_sort_prop_nan ORDER BY x DESC LIMIT 10) ORDER BY n ASC NULLS FIRST SETTINGS optimize_sorting_by_input_stream_properties = 0;

SELECT 'under an outer LIMIT, where the wrong order also returns wrong rows';
SELECT -x AS n FROM (SELECT x FROM t_sort_prop_nan ORDER BY x LIMIT 10) ORDER BY n DESC NULLS FIRST LIMIT 1;
SELECT -x AS n FROM (SELECT x FROM t_sort_prop_nan ORDER BY x LIMIT 10) ORDER BY n DESC NULLS FIRST LIMIT 1 SETTINGS optimize_sorting_by_input_stream_properties = 0;

SELECT 'the placement the stream already has can still be elided';
SELECT -x AS n FROM (SELECT x FROM t_sort_prop_nan ORDER BY x LIMIT 10) ORDER BY n DESC NULLS LAST;
SELECT -x AS n FROM (SELECT x FROM t_sort_prop_nan ORDER BY x LIMIT 10) ORDER BY n DESC NULLS LAST SETTINGS optimize_sorting_by_input_stream_properties = 0;

DROP TABLE t_sort_prop_nan;

DROP TABLE IF EXISTS t_sort_prop_null;
CREATE TABLE t_sort_prop_null (x Nullable(Float64)) ENGINE = MergeTree ORDER BY tuple() SETTINGS allow_nullable_key = 1;
INSERT INTO t_sort_prop_null VALUES (1), (NULL), (2);

SELECT 'NULL through a division by a negative constant';
SELECT x / -2 AS n FROM (SELECT x FROM t_sort_prop_null ORDER BY x LIMIT 10) ORDER BY n DESC NULLS FIRST;
SELECT x / -2 AS n FROM (SELECT x FROM t_sort_prop_null ORDER BY x LIMIT 10) ORDER BY n DESC NULLS FIRST SETTINGS optimize_sorting_by_input_stream_properties = 0;

DROP TABLE t_sort_prop_null;
