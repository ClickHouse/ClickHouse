-- More of the semantics of the `q` dialect: the limit and the order of `select[n; >c]` before the select phrase, the sort
-- that keeps the order of the equal rows, `all` and `any`, a take longer than the list, the running and moving
-- aggregates of long lists, and the q types of renamed columns.

SET max_threads = 1;
SET enable_q_dialect = 1;
SET dialect = 'q';

/ The limit applies to the rows that the where phrase leaves, before the select phrase.
select[2] s:sum x from ([] x:1 2 3);
select[-2] s:sum x from ([] x:1 2 3);
select[2] s:sums x from ([] x:1 2 3);
select[3] s:sum x by g from ([] g:1 2 1 2; x:1 2 3 4);
select[1] s:sum x from ([] x:1 2 3) where x>1;
select[2] from ([] x:1 2 3) where x>1;

/ The order applies before the limit, also by a column that is not selected, and the equal rows keep their order.
select[>x] y from ([] x:1 3 3 2; y:`a`b`c`d);
select[<x] y from ([] x:2 1 2 1; y:`a`b`c`d);
select[2;>x] y from ([] x:1 3 3 2; y:`a`b`c`d);
select[2;>x] s:sum x from ([] x:1 3 3 2);
`x xasc ([] x:2 1 2 1; y:`a`b`c`d);
`x xdesc ([] x:2 1 2 1; y:`a`b`c`d);

/ `all` and `any` test whether the values are not zero.
select a:all x, b:any x from ([] x:256 512);
select a:all x, b:any x from ([] x:0.5 0);
select a:all x, b:any x from ([] x:0 0);
all 256 512;
any 0 0.5;
any 0 0;
all 2;
any 0;
all 1 0N;
any 0N 0;

/ A take longer than the list repeats the items, from the end as well.
-5#1 2;
5#1 2;
-2#1 2 3;
-3#9;
3#9;
-5#"ab";
5#"ab";
-1#"ab";
n:-4; n#1 2 3;

SET max_memory_usage = 100000000;
/ The running and the moving aggregates of a long list do not make a slice of the list for every item.
last sums til 200000;
last maxs til 200000;
last mins til 200000;
last 1000 msum til 200000;
last 1000 mmax til 200000;
SET max_memory_usage = 0;
/ The sums take a null for zero, the maximum and the minimum skip the nulls.
sums 0N 1 0N 2;
maxs 0N 1 0N 3 2;
mins 0N 3 0N 1 2;
2 msum 0N 0N 1 2;
2 mavg 1 0N 2 4;
select s:sums x from ([] x:0N 1 0N 2);

/ A renamed column keeps its q type.
t:([] x:2003.07m); u:`y xcol t; select n:`long$y from u;
t:([] x:00:42); u:`y xcol t; select n:`long$y from u;
t:([] x:2003.07m; y:2003.07.01); u:`y`x xcol t; select a:`long$y, b:`long$x from u;
