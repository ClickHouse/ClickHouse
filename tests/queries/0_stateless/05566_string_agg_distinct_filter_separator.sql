-- STRING_AGG(DISTINCT x, sep) and STRING_AGG(x, sep) FILTER (...) take the separator as the parameter, like groupConcat(sep)(DISTINCT x).

SELECT STRING_AGG(DISTINCT x, ', ') FROM values('x String', 'a', 'b', 'a');
SELECT string_agg(DISTINCT x, '-'), group_concat(DISTINCT x, '-'), groupConcat(DISTINCT x, '-') FROM values('x String', 'a', 'b', 'a');
SELECT STRING_AGG(x, ', ') FILTER (WHERE x != 'b') FROM values('x String', 'a', 'b', 'a', 'c');
SELECT STRING_AGG(DISTINCT x, ', ') FILTER (WHERE x != 'b') FROM values('x String', 'a', 'b', 'a', 'c');
SELECT STRING_AGG(DISTINCT x, NULL) FROM values('x String', 'a', 'b'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT STRING_AGG(x, NULL) FILTER (WHERE x != 'b') FROM values('x String', 'a', 'b'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT STRING_AGG(DISTINCT x, ', ') OVER () FROM values('x String', 'a', 'b', 'a');
SELECT groupConcat(',', 2)(DISTINCT x, '/') AS s, s = groupConcat('/', 2)(DISTINCT x), length(s) FROM values('x String', 'aa', 'bb', 'cc');

SET param_sep = ' | ';
SELECT STRING_AGG(DISTINCT x, {sep:String}) FROM values('x String', 'a', 'b', 'a');

SELECT formatQuerySingleLine('SELECT STRING_AGG(DISTINCT x, \', \') FILTER (WHERE x != \'b\') FROM t');
SELECT formatQuerySingleLine('SELECT STRING_AGG(x, \', \') FROM t');

SELECT toTypeName(CAST(groupConcatDistinctState(',', 10)('a'), 'AggregateFunction(groupConcat(DISTINCT \',\', 10), String)'));
SELECT * FROM string_agg(DISTINCT 'a', ','); -- { serverError UNKNOWN_FUNCTION }

SELECT arraySort(splitByString(', ', STRING_AGG(DISTINCT toString(number % 3), ', '))) FROM remote('127.0.0.{1,2}', numbers(10));
