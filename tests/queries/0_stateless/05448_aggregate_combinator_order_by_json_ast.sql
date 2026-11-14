-- The `ORDER BY [LIMIT N]` aggregate function combinator syntax must survive the JSON AST round trip.

SELECT formatQueryFromJSON(parseQueryToJSON('SELECT groupArray(x ORDER BY y) FROM t'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT groupArray(x ORDER BY y DESC, z ASC NULLS FIRST LIMIT 3) FROM t'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT groupArray(x ORDER BY y LIMIT 0), sum(DISTINCT x ORDER BY y LIMIT 1) FROM t'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT quantile(0.5)(x ORDER BY y LIMIT 2) FROM t'));

SELECT JSONExtractUInt(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'order_by_combinator_limit') AS limit,
    JSONHas(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'order_by_combinator_columns') AS has_columns
FROM (SELECT parseQueryToJSON('SELECT groupArray(x ORDER BY y LIMIT 7)') AS j);

-- Malformed input is rejected.
SELECT formatQueryFromJSON('{"type":"Function","name":"groupArray","arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"x"}]},"order_by_combinator_limit":1}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"Function","name":"groupArray","arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"x"}]},"order_by_combinator_columns":{"type":"ExpressionList","children":[]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"Function","name":"groupArray","arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"x"}]},"order_by_combinator_columns":{"type":"ExpressionList","children":[{"type":"Identifier","name":"y"}]}}'); -- { serverError BAD_ARGUMENTS }
