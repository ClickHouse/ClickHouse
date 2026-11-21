-- Tests for the stateless function timeSeriesNormalizeTags: it handles its arguments exactly like timeSeriesTagsToGroup().

SELECT 'Sorted by name, __name__ included:';
SELECT timeSeriesNormalizeTags([('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS tags, toTypeName(tags);

SELECT 'Only the tags array, no separate pairs:';
SELECT timeSeriesNormalizeTags([('b', '2'), ('a', '1')]);

SELECT 'Empty and NULL tags array:';
SELECT timeSeriesNormalizeTags([]), timeSeriesNormalizeTags(NULL), timeSeriesNormalizeTags(CAST([], 'Array(Tuple(String, String))'));
SELECT timeSeriesNormalizeTags([], '__name__', 'http_requests_total'), timeSeriesNormalizeTags(NULL, 'region', 'us', '__name__', 'http_requests_total');

SELECT 'An empty value or a NULL value means the tag is absent:';
SELECT timeSeriesNormalizeTags([], '__name__', 'http_requests_total', 'region', ''),
       timeSeriesNormalizeTags([], '__name__', 'http_requests_total', 'region', NULL),
       timeSeriesNormalizeTags([('region', '')], '__name__', 'x'),
       timeSeriesNormalizeTags([('region', 'eu')], 'region', NULL);

SELECT 'An empty separate value does not conflict with the same tag in the array:';
SELECT timeSeriesNormalizeTags([('__name__', 'bar'), ('x', '1')], '__name__', '');

SELECT 'A duplicate tag with the same value is removed, with a different value is an error:';
SELECT timeSeriesNormalizeTags([('job', 'api')], 'job', 'api');
SELECT timeSeriesNormalizeTags([('job', 'api'), ('job', 'api')]);
SELECT timeSeriesNormalizeTags([('job', 'api')], 'job', 'web'); -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesNormalizeTags([('', 'api')]); -- { serverError BAD_ARGUMENTS }

SELECT 'Map:';
SELECT timeSeriesNormalizeTags(map('region', 'eu', 'env', 'dev'), '__name__', 'http_requests_count');

SELECT 'FixedString, LowCardinality and Nullable:';
SELECT timeSeriesNormalizeTags(CAST([('a', 'x')], 'Array(Tuple(FixedString(3), FixedString(3)))'))
     = timeSeriesNormalizeTags([], toFixedString('a', 3), toFixedString('x', 3)),
       timeSeriesNormalizeTags([], toFixedString('a', 3), toFixedString('x', 3));
SELECT timeSeriesNormalizeTags(CAST([('b', 'y'), ('a', 'x')], 'Array(Tuple(LowCardinality(String), LowCardinality(String)))'), toLowCardinality('c'), toLowCardinality('z'));
SELECT timeSeriesNormalizeTags(CAST(map('b', 'y'), 'Map(LowCardinality(String), LowCardinality(String))'), toLowCardinality('a'), toLowCardinality(toNullable('x')));

SELECT 'Constant and non-constant arguments:';
SELECT number,
       timeSeriesNormalizeTags([('region', 'eu')], 'n', toString(number), 'skip', if(number % 2 = 0, '', 'v')) AS from_const_array,
       timeSeriesNormalizeTags(materialize([('region', 'eu')]), 'n', toString(number), 'skip', if(number % 2 = 0, '', 'v')) AS from_column_array,
       from_const_array = from_column_array
FROM numbers(4) ORDER BY number;

SELECT 'Wrong arguments:';
SELECT timeSeriesNormalizeTags(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT timeSeriesNormalizeTags([], 'a'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT timeSeriesNormalizeTags([], 1, 'a'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesNormalizeTags('a'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'Equivalent to timeSeriesGroupToTags(timeSeriesTagsToGroup(...)):';
SELECT count(), countIf(timeSeriesNormalizeTags(arr, 'n', toString(number), 'e', e, 'z', z) = timeSeriesGroupToTags(timeSeriesTagsToGroup(arr, 'n', toString(number), 'e', e, 'z', z)))
FROM
(
    SELECT number,
           [('zz', 'last'), ('aa', 'first'), ('mid', if(number % 3 = 0, '', 'm'))] AS arr,
           if(number % 2 = 0, '', 'v') AS e,
           if(number % 5 = 0, NULL, 'w') AS z
    FROM numbers(30)
);
SELECT timeSeriesNormalizeTags(arr, 'n', 'v') = timeSeriesGroupToTags(timeSeriesTagsToGroup(arr, 'n', 'v'))
FROM (SELECT map('b', '2', 'a', '1', 'c', '') AS arr);
