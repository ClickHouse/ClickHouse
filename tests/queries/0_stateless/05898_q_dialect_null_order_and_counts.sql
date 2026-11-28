-- More of the semantics of the `q` dialect: a null is less than every other value in the sorts, the predecessor
-- of the first item of `deltas` and `ratios`, `where` of a list of integers, and over and scan with a function
-- that returns a function.

SET max_threads = 1;
SET enable_q_dialect = 1;
SET dialect = 'q';

/ A null is the first in the ascending order and the last in the descending one: a sorted limit, a sort of a table,
/ the order of the groups.
select[1;<x] from ([] x:0N 1);
select[1;>x] from ([] x:0N 1);
`x xasc ([] x:3 0N 1);
`x xdesc ([] x:3 0N 1);
`x xasc ([] x:2.5 0n 1.5);
select n:count i by x from ([] x:2 0N 1 2);
/ The same for the lists and their grades.
asc 1 0N;
asc 3 1 2;
asc 2.5 0n 1.5;
desc 1 0N 2;
iasc 0N 1;
iasc 2 0N 1 0N;
idesc 2 0N 1;

/ The predecessor of the first item is 0 for `deltas` and 1 for `ratios`. A null inside is a predecessor as it is.
deltas 1 0N 3;
deltas 1 2 4;
ratios 1 0N 3;
ratios 1 2 4;
select d:deltas x, r:ratios x from ([] x:1 0N 3);
select d:deltas x, r:ratios x from ([] x:2 4 8);

/ `where` of a list of integers is the index of every item, as many times as the item says.
where 2 3 0 1;
where 1010b;
where 1 2 3 > 1;
where -1 2; -- { serverError TOO_LARGE_ARRAY_SIZE }
where 1.5 2; -- { serverError TYPE_MISMATCH }

/ Over and scan with a function that returns a function.
f:{[x;y] +}; f/1 2; -- { serverError NOT_IMPLEMENTED }
f:{[x;y] +}; f\1 2; -- { serverError NOT_IMPLEMENTED }
{x+y}/1 2 3;
{x+y}\1 2 3;
