-- More of the semantics of the `q` dialect: the computed columns of a table through the query layers, a function
-- made of many functions, `and` and `or` of numbers, Match of values of different types, the sum of a minute and
-- a second, the buckets of `xbar` of a month, a date, and a timestamp, and the division of q with the setting of
-- the integer division of SQL.

SET max_threads = 1;
SET enable_q_dialect = 1;

CREATE TABLE computed (a Int64, m Int64 MATERIALIZED a * 2, al Int64 ALIAS a + 1) ENGINE = Memory;
INSERT INTO computed VALUES (1), (2), (3);
SET dialect = 'q';
/ The materialized and alias columns go through the query layers: the number of the row and a window function,
/ an update, a delete, a sort, a take from the end.
select m, i from computed;
select m, s:sums al from computed;
update a:a+10 from computed;
delete from computed where a>avg a;
`m xdesc computed;
-2#computed;
select[-1] al from computed;

SET max_parser_depth = 20;
/ A function made of many functions, one inside of another, is under the limit of the depth as well.
f:+; f:f'; f:f'; f:f'; f:f'; f:f'; f:f'; f:f'; f:f'; f:f'; f:f'; f:f'; f:f'; f:f'; f:f'; f:f'; f:f'; f:f'; f:f'; f:f'; f:f'; f:f'; f:f'; f:f'; f:f'; f:f'; 1; -- { serverError TOO_DEEP_RECURSION }
g:{x+y}; g:g[g;]; g:g[g;]; g:g[g;]; g:g[g;]; g:g[g;]; g:g[g;]; g:g[g;]; g:g[g;]; g:g[g;]; g:g[g;]; g:g[g;]; g:g[g;]; g:g[g;]; g:g[g;]; g:g[g;]; g:g[g;]; g:g[g;]; g:g[g;]; g:g[g;]; g:g[g;]; g:g[g;]; g:g[g;]; g:g[g;]; g:g[g;]; g:g[g;]; 1; -- { serverError TOO_DEEP_RECURSION }
f:+; f:f'; f:f'; f:f'; 1;
SET max_parser_depth = 1000;

/ `and` and `or` are the lesser and the greater of two values.
2 and 3;
2 or 3;
1b and 0b;
1b or 0b;
1 5 3 and 4 2 6;
1 5 3 or 4 2 6;
select a:x and 2, o:x or 2 from ([] x:1 2 3);
2 and 0N;
2 or 0N;
1 0N 3 and 2 2 2;

/ Match is strict about the types, unlike the equality.
1~1h;
1=1h;
1~1;
1 2 3~1 2 3h;
1 2 3~1 2 3;
1~1f;
`a~"a";
1 2~1;
(0N 1)~0N 1;
select m:x~1 2 3 from ([] x:1 2 3);
(1+1i)~2;
(1+1i)~2i;
(`short$1)~1h;

/ A minute and a second make a second, in either order.
`long$01:00+00:00:01;
`long$00:00:01+01:00;
01:00+00:00:01;
select s:`long$m+00:00:01 from ([] m:01:00 02:00);
t:([] m:01:00 02:00); u:update s:m+00:00:01 from t; select n:`long$s from u;

/ The buckets of `xbar` start at 2000.01.01, the zero of the temporal types.
7 xbar 2000.01m;
7 xbar 2000.08m;
7 xbar 2001.03m;
3 xbar 2024.05m;
5 xbar 2000.01.03;
7 xbar 2024.05.17;
0D00:00:07 xbar 2000.01.01D00:00:10;
select b:7 xbar d from ([] d:2024.05.17 2024.05.18);

SET sql_compatible_division_for_integers = 1;
/ The division of q is a floating point one, whatever the division of the integers of SQL is.
3%2;
select r:x%2 from ([] x:1 2 3);
ratios 1 2 4;
1 2 wavg 3 4;
`float$2000.01.02T12:00:00.000;
"p"$42;
.ch.sql "SELECT 3 / 2 AS x";
SET sql_compatible_division_for_integers = 0;
.ch.sql "SELECT 3 / 2 AS x";

SET dialect = 'clickhouse';
DROP TABLE computed;
