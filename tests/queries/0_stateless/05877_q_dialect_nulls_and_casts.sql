-- More of the semantics of the `q` dialect: a projection applied in several steps, the nulls in the comparisons,
-- in the joins, and in `wavg`, the casts, `delete` of all the rows, and the memory limit during the compilation.

SET max_threads = 1;
SET enable_q_dialect = 1;
SET dialect = 'q';

/ A projection applied in several steps keeps its arguments.
f:{[x;y;z] x+y+z}; g:f[1]; h:g[2]; h[3];
f:{[x;y;z] x-y-z}; g:f[;2]; h:g[10]; h[3];

/ The nulls are equal to each other and less than every other value.
0N=0N;
0N<>0N;
0N<1;
1<0N;
0N<=0N;
0N>=1;
1>0N;
0N 1 2=0N 1 3;
select from ([] x:0N 1) where x=0N;
select from ([] x:0N 1 2) where x<2;
select e: x=y, l: x<y, g: x>=y from ([] x:0N 1 0N; y:0N 0N 5);
(0N 1) ~ (0N 1);
differ 0N 0N 1 1 0N;
select d: differ x from ([] x:0N 0N 1 1);

/ A null key matches a null key in a join.
t:([] k:0N 1; a:10 20); u:([k:0N 1] b:100 200); t lj u;
t:([] k:0N 1 2; a:10 20 30); u:([k:0N 1] b:100 200); t ij u;

/ A null in either argument of `wavg` excludes the pair.
select w: weight wavg price from ([] weight:1 1; price:10 0N);
1 1 2 wavg 10 0N 40;
select w: weight wavg price by g from ([] g:1 1 2; weight:1 1 3; price:10 0N 5);
update w: weight wavg price by g from ([] g:1 1 2; weight:1 1 3; price:10 0N 5);

/ A cast rounds a number to an integer, and a number is a count of the units of a temporal type since 2000.01.01.
`int$3.7 1.2;
`long$-3.7;
`short$41.6;
"p"$42;
"z"$1.5;
"d"$42;
"m"$42;
"n"$42;
"t"$42;
"v"$42;
"u"$42;
select t: "t"$x from ([] x:42 1000);
`long$2000.01.01D00:00:00.000000042;
`long$2000.02.12;
`long$2003.07m;
`long$0D00:00:00.000000042;
`long$00:00:00.042;
`long$00:00:42;
`long$00:42;
`float$2000.01.02T12:00:00.000;

/ `delete` without the where phrase deletes all the rows.
t:([] a:1 2); count delete from t;
t:([] a:1 2); delete from t;
t:([] a:1 2); delete from t where a=1;
t:([k:1 2] a:3 4); cols delete from t;

SET max_memory_usage = 20000000;
/ The compilation of a program is under the memory limit of the query: a table that is not even used.
t:([] a:til 10000000);1; -- { serverError MEMORY_LIMIT_EXCEEDED }
SET max_memory_usage = 0;
t:([] a:til 3);1;
