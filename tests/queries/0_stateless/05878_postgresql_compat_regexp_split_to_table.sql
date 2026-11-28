-- `regexp_split_to_table(s, regexp)` of PostgreSQL returns a row for each substring.
-- It is parsed as `arrayJoin(regexp_split_to_array(s, regexp))`.
SELECT regexp_split_to_table('a1b2c', '[0-9]');
SELECT REGEXP_SPLIT_TO_TABLE('x y', ' ') AS w ORDER BY w DESC;
SELECT count() FROM (SELECT regexp_split_to_table('a,b,c,d', ','));
SELECT n, regexp_split_to_table(s, ',') FROM (SELECT 1 AS n, 'p,q' AS s);
SELECT formatQuerySingleLine('SELECT regexp_split_to_table(s, r) FROM t');

SELECT regexp_split_to_table('a'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
