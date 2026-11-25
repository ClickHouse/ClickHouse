-- The semantics of the `q` dialect: the order of evaluation, the nulls of the columns, the temporal values,
-- `med`, `cross`, `aj0`, the list functions of the columns, and the temporary tables.

SET max_threads = 1;

CREATE TEMPORARY TABLE t_q_temporary (a Int64, b String);
INSERT INTO t_q_temporary VALUES (1, 'x'), (2, 'y');

SET enable_q_dialect = 1;
SET dialect = 'q';

/ The evaluation goes from right to left: the arguments of a function and the items of a list.
a:0; a+(a:2);
x:10; (x+5;x:20;x-5);
f:{x-y}; b:1; f[b; b:5];

/ The nulls of a column stay in their places when the column is collected into a list.
select x, y by g from ([] g:1 1 2; x:0N 2 3; y:10 20 0N);
t:([] x:0N 2 3); t`x;
t:([] x:0N 2 3); count t`x;
select first x, last y by g from ([] g:1 1 2; x:0N 2 3; y:10 20 0N);
select by g from ([] g:1 1 2; x:1 0N 3);
select distinct x from ([] x:0N 2 0N 2);

/ The difference of two timestamps is exact to the nanosecond, for the literals and for the columns.
2024.01.15D10:30:00.000000001 - 2024.01.15D10:30:00;
2024.01.15D10:30:00 - 2024.01.17D10:30:00.000000001;
select d: t - 2024.01.15D10:30:00 from ([] t: 2024.01.15D10:30:00.000000001 2024.01.15D10:30:01);
10:30:00.000 + 00:00:01.500;
10:30:00 - 00:00:05;
0D00:00:00.000000001 + 10:30:00;

/ The temporal literals: the ranges of the types, and no overflow in between.
2299.12.30T23:59:59.999;
2262.04.10D23:47:16.854775807;
0D23:59:59.999999999;
-0D00:00:01;
90u;
3600v;
2147483647t;
106752D; -- { clientError SYNTAX_ERROR }
106751D23:47:16.854775808; -- { clientError SYNTAX_ERROR }
2300.01.01D00:00:00; -- { clientError SYNTAX_ERROR }
1700.01.01D00:00:00; -- { clientError SYNTAX_ERROR }
2147483648t; -- { clientError SYNTAX_ERROR }
99999999999u; -- { clientError SYNTAX_ERROR }
99999999999v; -- { clientError SYNTAX_ERROR }
9999999999d; -- { clientError SYNTAX_ERROR }
9999999999m; -- { clientError SYNTAX_ERROR }

/ The median of an even count of values is the mean of the two middle ones.
med 1 2;
med 1 2 3 10;
med 3 1 2;
select med x from ([] x:1 2);
select med x by g from ([] g:1 1 2 2 2; x:1 2 10 60 20);
update m: med x by g from ([] g:1 1 2; x:1 2 10);

/ `cross` of two lists is the list of the pairs.
(1 2) cross (3 4);
count (1 2) cross (3 4);
((1 2) cross (3 4)) 1;
first (1 2) cross (3 4);

/ `aj` keeps the time of the left table, `aj0` takes the time of the matched row.
t:([] sym:`a`a`b; time:101 105 103; price:1 2 3); u:([] sym:`a`a`b; time:100 104 110; bid:10 20 30); aj[`sym`time; t; u];
t:([] sym:`a`a`b; time:101 105 103; price:1 2 3); u:([] sym:`a`a`b; time:100 104 110; bid:10 20 30); aj0[`sym`time; t; u];

/ `#`, `except`, and `inter` with a column.
select 2#x from ([] x:1 2 3);
select -2#x from ([] x:1 2 3);
select x except 2 from ([] x:1 2 3);
select x inter 2 3 4 from ([] x:1 2 3);
select 2#x by g from ([] g:1 1 1 2; x:1 2 3 4);

/ A temporary table of the session.
select from t_q_temporary;
select s: sum a from t_q_temporary;
count t_q_temporary;
