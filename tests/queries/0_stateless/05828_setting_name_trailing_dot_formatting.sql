-- The parser reads a setting name as a compound identifier. The formatter writes the name without quotes
-- when each part between the dots is an identifier. A name with a trailing dot has an empty last part.
-- The formatted query must keep the quotes of such a name, otherwise the name loses the dot.
-- The second column shows that the formatted query is parseable and that the format is stable.

SELECT formatQuerySingleLine('SET `a.` = 1') AS f, formatQuerySingleLine(f) = f;
SELECT formatQuerySingleLine('SET `a.b.` = 1, `a.b` = 2, a.b.c = 3') AS f, formatQuerySingleLine(f) = f;
SELECT formatQuerySingleLine('SELECT 1 SETTINGS `max_threads.` = 1') AS f, formatQuerySingleLine(f) = f;
SELECT formatQuerySingleLine('CREATE SETTINGS PROFILE p SETTINGS `max_threads.` = 1') AS f, formatQuerySingleLine(f) = f;

-- Two different keys must not become one key.
SELECT formatQuerySingleLine('CREATE NAMED COLLECTION c AS `a.b.` = 5, `a.b` = 6') AS f, formatQuerySingleLine(f) = f;
SELECT formatQuerySingleLine('ALTER NAMED COLLECTION c SET `a.` = 4 NOT OVERRIDABLE') AS f, formatQuerySingleLine(f) = f;

-- The format of the other names does not change.
SELECT formatQuerySingleLine('SET `a.b` = 1, `a..b` = 2, `.a` = 3, `a b` = 4, `a b`.c = 5') AS f, formatQuerySingleLine(f) = f;
