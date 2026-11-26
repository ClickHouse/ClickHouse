-- Under `sensitive` matching a double-quoted alias is the same name as the unquoted one,
-- so equal definitions under both spellings are not conflicting.
SET enable_analyzer = 1;

SELECT 1 AS x, 1 AS "x", x SETTINGS column_and_query_name_matching = 'sensitive';
SELECT 1 AS x, 2 AS "x", x SETTINGS column_and_query_name_matching = 'sensitive'; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
