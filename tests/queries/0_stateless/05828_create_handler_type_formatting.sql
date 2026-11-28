-- The parser reads the handler type of `CREATE HANDLER` as one identifier.
-- The formatted query must keep the quotes of a type that is not a plain identifier, otherwise it is not parseable.
-- The second column shows that the formatted query is parseable and that the format is stable.

SELECT formatQuerySingleLine('CREATE HANDLER h URL \'/x\' TYPE `my type` AS SELECT 1') AS f, formatQuerySingleLine(f) = f;
SELECT formatQuerySingleLine('CREATE HANDLER h URL \'/x\' TYPE `a.b` AS SELECT 1') AS f, formatQuerySingleLine(f) = f;

-- The format of a plain type does not change.
SELECT formatQuerySingleLine('CREATE HANDLER h URL \'/x\' METHODS (GET, POST) TYPE predefined_query_handler AS SELECT 1') AS f, formatQuerySingleLine(f) = f;
