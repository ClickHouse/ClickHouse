SELECT formatQueryFromJSON(parseQueryToJSON('SELECT 1 SHUFFLE'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT 1 SHUFFLE LIMIT 2'));
WITH 'SELECT 1 SHUFFLE LIMIT AFTER 1 UNTIL 0' AS query
SELECT parseQueryToJSON(formatQueryFromJSON(parseQueryToJSON(query))) = parseQueryToJSON(query);
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT 1 SHUFFLE'), '"shuffle":true', '"shuffle":false'));
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT 1 ORDER BY 1'), '"type":"SelectQuery"', '"type":"SelectQuery","shuffle":true')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT 1 ORDER BY ALL'), '"type":"SelectQuery"', '"type":"SelectQuery","shuffle":true')); -- { serverError BAD_ARGUMENTS }
