-- A `LIMIT` count or offset that starts with an identifier named `after` or `until` and continues is formatted in
-- parentheses: without quotes, `LIMIT after - 5` would be read back as the range `LIMIT AFTER -5`.
SELECT formatQuerySingleLine('SELECT 1 LIMIT `after` - 5');
SELECT formatQuerySingleLine('SELECT 1 LIMIT `until` - 5 AS c');
SELECT formatQuerySingleLine('SELECT 1 LIMIT 3 OFFSET `after` - 5');
SELECT formatQuerySingleLine('SELECT 1 FETCH FIRST `after`[1] ROWS ONLY');
SELECT formatQuerySingleLine('SELECT 1 LIMIT `until` - 1 BY x');
SELECT formatQuerySingleLine('SELECT 1 LIMIT `after` - 1, 2 BY x');
SELECT formatQuerySingleLine('SELECT 1 LIMIT 1 BY x LIMIT `after`(2)');
-- An identifier is still a count and is printed as before.
SELECT formatQuerySingleLine('SELECT 1 LIMIT `after`, `until` BY x LIMIT `until` AS c');
-- Nested counts are formatted once each: the time is not exponential in the nesting.
SELECT length(formatQuerySingleLine(repeat('SELECT 1 LIMIT `after` - (', 25) || 'SELECT 1' || repeat(')', 25))) > 0;

-- A view stores the formatted query and parses it back on ATTACH.
CREATE VIEW v_count AS WITH 7 AS after SELECT number FROM numbers(10) LIMIT `after` - 5;
CREATE VIEW v_offset AS WITH 7 AS until SELECT number FROM numbers(10) ORDER BY number LIMIT 3 OFFSET `until` - 5;
DETACH VIEW v_count;
ATTACH TABLE v_count;
DETACH VIEW v_offset;
ATTACH TABLE v_offset;
SELECT groupArray(number) FROM v_count;
SELECT groupArray(number) FROM v_offset;
DROP VIEW v_count;
DROP VIEW v_offset;
