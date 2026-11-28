-- Comments, boundaries of queries, recovery after syntax errors, and reserved names in the `q` dialect.

DROP TABLE IF EXISTS t_reserved;
CREATE TABLE t_reserved (a Int64, __qrow Int64) ENGINE = Memory;

SET enable_q_dialect = 1;
SET dialect = 'q';

/ A block comment between the queries: a `;` at the end of its line does not end anything.
/
this is a block comment;
"an unterminated string
\
1+1;

/ A block comment inside a query.
10+
/
skipped;
\
 20;

/ Comment-only lines do not break a statement that continues on an indented line.
1 +
-- an SQL-style note
/ a q-style note
 2;

/ A comment-only line before a line in the first column: two statements, the result is of the last one.
a: 5
-- a note
a * 2;

/ A syntax error in a query of several lines: the whole query is skipped.
select from (
  1 2 ]
  3); -- { clientError SYNTAX_ERROR }
3 + 4;

/ The names of the helper columns of the generated query are reserved.
select from t_reserved; -- { serverError BAD_ARGUMENTS }
.ch.sql "SELECT 1 AS __q1"; -- { serverError BAD_ARGUMENTS }
`__qmatch xcol ([] a: 1 2); -- { serverError BAD_ARGUMENTS }
.ch.sql "SELECT a FROM t_reserved";

/ The order of a cross join.
([] a: 1 2 3) cross ([] b: `x`y);

SET dialect = 'clickhouse';
DROP TABLE t_reserved;

SET dialect = 'q';
/ A lone backslash ends the script: the rest is a comment.
100;
\
this is not a query;
