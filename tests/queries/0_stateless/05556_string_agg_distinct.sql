SELECT STRING_AGG(DISTINCT x, ', ') FROM values('x String', 'a', 'b', 'a');
SELECT string_agg(DISTINCT x, '-') FROM values('x String', 'a', 'b', 'a');
SELECT group_concat(DISTINCT x, '|') FROM values('x String', 'a', 'b', 'a');
SELECT groupConcat(DISTINCT x, ',') FROM values('x String', 'a', 'b', 'a');
SELECT groupConcat(',', 1)(DISTINCT x, '/') FROM values('x String', 'a', 'b', 'a');
