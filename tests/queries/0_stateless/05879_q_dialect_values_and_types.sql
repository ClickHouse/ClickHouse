-- More of the semantics of the `q` dialect: the value of `exec`, a projection of a projection, an adverb without
-- arguments, a list of strings as a column, the aggregates of lists with nulls, the arithmetic of a temporal value
-- and a number, the q types of the columns of a table, the named casts, and the settings that construct the result.

SET max_threads = 1;
SET enable_q_dialect = 1;
SET dialect = 'q';

/ `exec` returns a value: a list, an atom for an aggregate, a dictionary with the by phrase.
exec x from ([] x:10 20);
v:exec x from ([] x:10 20); v 0;
exec sum x from ([] x:10 20);
1 + exec max x from ([] x:10 20);
exec sum x by g from ([] g:1 1 2; x:10 20 30);
d:exec sum x by g from ([] g:1 1 2; x:10 20 30); d 2;
exec x, y from ([] x:1 2; y:3 4);

/ A projection of a projection keeps the arguments of the first one.
f:{[x;y;z] x+y+z}; g:f[1]; h:g[;3]; h[2];
f:{[x;y;z] x-y-z}; g:f[;;3]; h:g[;2]; h[10];

/ An adverb needs arguments.
f:+/; f . (); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
f:{x+y}\; f . (); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

/ A list of strings is the rows of a column.
t:([] s:`a`b); u:([] x:t`s); count u;
t:([] s:`a`b); u:([] x:t`s); u;

/ The aggregates of a list skip the nulls, like the aggregates of a column.
sum 1 0N 2;
avg 1 0N 2;
min 3 0N 2;
max 3 0N 2;
prd 2 0N 3;
sums 1 0N 2;
maxs 1 0N 3 2;
2 msum 1 0N 2 4;
2 mavg 1 0N 2 4;

/ A temporal value plus a number: the number counts the units of the type.
2024.01m+1;
1+2024.01m;
2024.01m-1;
2024.01.15+1;
2024.01.15D10:30:00+1;
10:30+1;
10:30:00+1;
10:30:00.000+1;
0D00:00:01-1;

/ A column of a table keeps its q type: a month is not a date, and a minute is not a second.
select m: x+1 from ([] x:2024.01m 2024.12m);
select n:`long$x from ([] x:2003.07m);
select n:`long$x from ([] x:00:42 01:00);
t:([] x:2003.07m 2003.08m); select n:`long$x from t where x>2003.07m;
t:([] x:2003.07m 2003.08m); select n:`long$x from 1#t;
t:([] k:1 2; x:2003.07m 2003.08m); select n:`long$first x by k from t;
t:([] x:2003.07m 2003.08m); u:update y:x+1 from t; select n:`long$y from u;

/ The name of a type casts a number like its letter does, and is an accessor of a temporal value.
`date$42;
`month$42;
`minute$42;
`second$42;
`time$42;
`date$2024.01.15D10:30:00;
`month$2024.01.15;

SET filter = 'n > 0';
/ The settings that construct the result apply to the compiled query.
.ch.sql "SELECT number AS n FROM numbers(3)";
SET filter = '';
.ch.sql "SELECT number AS n FROM numbers(3)";
