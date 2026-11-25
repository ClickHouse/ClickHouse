-- More of the semantics of the `q` dialect: the depth of a list made across the statements, an assignment inside of
-- a constraint, several constraints with aggregates in `update` and `delete`, a temporal value and a computed number,
-- the difference of two months and of two dates, `div` of large and negative integers, `union` of tables, the running
-- products of a long list, the query parameters of an embedded query, and the materialized and alias columns of a table.

SET max_threads = 1;
SET enable_q_dialect = 1;
SET dialect = 'q';

SET max_parser_depth = 20;
/ A list that every statement makes deeper is under the limit of the depth, like a deep program.
a:1; a:,a; a:,a; a:,a; a:,a; a:,a; a:,a; a:,a; a:,a; a:,a; a:,a; a:,a; a:,a; a:,a; a:,a; a:,a; a:,a; a:,a; a:,a; a:,a; a:,a; a:,a; a:,a; a:,a; a:,a; a:,a; a+1; -- { serverError TOO_DEEP_RECURSION }
a:1; a:,a; a:,a; a:,a; a+1;
SET max_parser_depth = 1000;

/ An assignment inside of a constraint happens once, also when the constraint comes after a simple one.
a:0; t:select from ([] x:1 2 3 10) where x>0, x>(a:a+1)+avg x; a;
a:0; t:select from ([] x:1 2 3 10) where x>0, x>(a:a+1)+avg x; t;

/ Several constraints with aggregates in `update` and `delete`: each applies to the rows that the previous ones select.
update y:1 from ([] x:1 2 3 4) where x>avg x, x>avg x;
delete from ([] x:1 2 3 4) where x>avg x, x>avg x;
update y:1 from ([] x:1 2 3 4) where x>1, x>avg x, x<5;
delete from ([] x:1 2 3 4 5) where x>1, x>avg x;
update y:sum x from ([] x:1 2 3 4) where x>avg x;

/ A temporal value and a computed number; the difference of two months and of two dates.
2024.01m+(1+1i);
2024.01m+1+1;
2024.01.01+2*3;
2024.02m-2024.01m;
2024.03.01-2024.01.01;
`long$2024.03.01-2024.01.01;

/ `div` rounds down, and is exact for the integers.
9007199254740993 div 1;
-7 div 2;
7 div -2;
-7 div -2;
7 div 2;
-6 div 3;
7.5 div 2;
-7.5 div 2;
select q:x div 2 from ([] x:-3 -2 -1 0 1 2 3);

/ `union` of tables has the distinct rows.
t:([] a:1 2); u:t union t; count u;
([] a:1 2 3) union ([] a:2 3 4);
1 2 union 2 3;

SET max_memory_usage = 100000000;
/ The running products of a long list do not make the prefix of every item.
last prds 50000#1;
count prds til 50000;
SET max_memory_usage = 0;
prds 1 2 3 4;
prds 1 0N 3;
prds 1.5 2 0.5;

SET param_n = 7;
/ The query parameters of an embedded query.
.ch.sql "SELECT {n:UInt64} AS x";
select y:x+1 from .ch.sql "SELECT {n:UInt64} AS x";
SET param_c = 'number';
.ch.sql "SELECT {c:Identifier} AS x FROM numbers(2)";

SET dialect = 'clickhouse';
CREATE TABLE t_computed (a Int64, m Int64 MATERIALIZED a * 2, al Int64 ALIAS a + 1) ENGINE = Memory;
INSERT INTO t_computed VALUES (1), (2);
SET dialect = 'q';
/ The materialized and alias columns of a table are its columns.
select m, al from t_computed;
cols t_computed;
select from t_computed;
SET dialect = 'clickhouse';
DROP TABLE t_computed;
