-- A `LIMIT ... AFTER` / `UNTIL` boundary that starts with a word that may follow a `LIMIT` count
-- (here, quoted identifiers `settings`, `by`, `format`) is formatted in parentheses,
-- because it is printed without quotes and would be read as the count otherwise.

SELECT formatQuerySingleLine('SELECT 1 LIMIT 3 UNTIL `settings`');
SELECT formatQuerySingleLine('SELECT 1 LIMIT AFTER `by`');
SELECT formatQuerySingleLine('SELECT 1 LIMIT 1 AFTER `format` + 1');
SELECT formatQuerySingleLine('SELECT 1 LIMIT AFTER `settings` ALL UNTIL `format`');
SELECT formatQuerySingleLine('SELECT 1 LIMIT UNTIL `settings` AS s');
SELECT formatQuerySingleLine('SELECT 1 LIMIT UNTIL settings_x');

-- The formatted text parses back to the same query.
SELECT formatQuery(formatQuery(q)) = formatQuery(q) FROM
(
    SELECT arrayJoin([
        'SELECT 1 LIMIT 3 UNTIL `settings`',
        'SELECT 1 LIMIT AFTER `by`',
        'SELECT 1 LIMIT 1 AFTER `format` + 1',
        'SELECT 1 LIMIT AFTER `settings` ALL UNTIL `format`',
        'SELECT 1 LIMIT UNTIL `settings` AS s',
        'SELECT 1 LIMIT 3645369518 UNTIL `settings` INTO OUTFILE \'/tmp/file.data\' TRUNCATE FORMAT Null']) AS q
);

-- Nested boundaries that need the parentheses: each is formatted once, the time is not exponential in the nesting.
SELECT formatQuery(formatQuery(q)) = formatQuery(q), formatQuerySingleLine(q) LIKE '%UNTIL (settings + (SELECT 1 LIMIT  UNTIL (settings + (%'
FROM (SELECT concat(repeat('SELECT 1 LIMIT UNTIL `settings` + (', 25), 'SELECT 1', repeat(')', 25)) AS q);

-- Executed queries: debug builds verify that the formatted query parses back.
SELECT number AS `settings` FROM numbers(10) LIMIT UNTIL `settings` = 3;
SELECT number AS `format` FROM numbers(10) LIMIT AFTER `format` = 7;
