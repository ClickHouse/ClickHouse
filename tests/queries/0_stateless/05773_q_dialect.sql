-- Tests of the `q` dialect: the language of kdb+, compiled into ClickHouse queries.

SET max_threads = 1;

DROP TABLE IF EXISTS trades;
DROP TABLE IF EXISTS quotes;
CREATE TABLE trades (time DateTime64(9), sym String, price Float64, size Int64) ENGINE = MergeTree ORDER BY (sym, time);
INSERT INTO trades VALUES ('2024-01-15 09:30:00', 'IBM', 100, 10), ('2024-01-15 09:31:30', 'IBM', 101, 20), ('2024-01-15 09:32:00', 'MSFT', 400, 5), ('2024-01-15 09:36:00', 'IBM', 102, 30), ('2024-01-15 09:37:00', 'MSFT', 401, 15);
CREATE TABLE quotes (time DateTime64(9), sym String, bid Float64, ask Float64) ENGINE = MergeTree ORDER BY (sym, time);
INSERT INTO quotes VALUES ('2024-01-15 09:29:00', 'IBM', 99.5, 100.5), ('2024-01-15 09:31:00', 'IBM', 100.5, 101.5), ('2024-01-15 09:31:00', 'MSFT', 399, 401);

SET dialect = 'q';
1+1; -- { serverError SUPPORT_IS_DISABLED }
SET enable_q_dialect = 1;

/ Atoms, lists, and right-to-left evaluation.
1+2;
2*3+4;
1 2 3 + 10;
til 5;
sum 1+til 10;
`a`b`c;
"hello";
count "hello";
reverse "abc";
3 # 1 2;
-2 # 1 2 3 4;
2 _ 1 2 3 4;
1 2 3 ? 2;
1 2 3 4 % 2;
neg 1 -2 3;
-7 mod 3;
7 div 2;
$[1 > 2; `yes; `no];
0N;
null 0N 1;
0w;

/ Temporal literals.
2024.01.15;
2024.01m;
2024.01.15D10:30:00 + 0D01:15:00;
10:30:00.000 + 00:00:01.500;
2024.01.15D10:31:30 - 2024.01.15D10:30:00;

/ Casts.
`int$3.7 1.2;
"J"$"42";
"D"$"2024.01.15";
string 42;
"," vs "a,b,c";
"-" sv ("a"; "b");

/ Lambdas, projections, and adverbs.
{x*x} each til 4;
f: {x+y}; f[1;2];
g: {x*y}[10]; g 1 2 3;
{[a;b] a-b}[10;3];
+/ 1 2 3 4;
+\ 1 2 3 4;
{x+y}/[0; 1 2 3];
{x*y}\[1 2 3 4];
max each (1 2 3; 4 5 6);
1 2 3 ,/: 10 20;
(1 2 3) +\: 10 20;
-': 1 4 9 16;

/ Dictionaries.
`a`b`c ! 1 2 3;
d: `a`b ! 10 20; d[`b];
flip `a`b ! (1 2 3; 4 5 6);

/ Table literals and q-sql.
t: ([] s: `a`b`a`b; px: 1.5 2.5 3.5 4.5); select avg px by s from t;
t: ([] s: `a`b`a`b; px: 1.5 2.5 3.5 4.5); select from t where px > 2;
t: ([] s: `a`b`a`b; px: 1.5 2.5 3.5 4.5); select s, px, d: px - avg px from t;
t: ([] s: `a`b`a`b; px: 1.5 2.5 3.5 4.5); update c: sums px by s from t;
t: ([] s: `a`b`a`b; px: 1.5 2.5 3.5 4.5); update px: 0 from t where s = `b;
t: ([] s: `a`b`a`b; px: 1.5 2.5 3.5 4.5); exec px from t where s = `a;
t: ([] s: `a`b`a`b; px: 1.5 2.5 3.5 4.5); delete from t where s = `b;
t: ([] s: `a`b`a`b; px: 1.5 2.5 3.5 4.5); meta t;
t: ([] s: `a`b`a`b; px: 1.5 2.5 3.5 4.5); select px by s from t;
t: ([] s: `a`b`a`b; px: 1.5 2.5 3.5 4.5); select by s from t;
t: ([] s: `a`b`a`b; px: 1.5 2.5 3.5 4.5); select sum px, sum px from t;

/ ClickHouse tables.
select vwap: size wavg price, n: count i, total: sum size by sym from trades;
select open: first price, high: max price, low: min price, close: last price by sym, bucket: 5 xbar time.minute from `time xasc trades;
select from trades where price = (max; price) fby sym;
select from trades where size > avg size;
select sym, price, d: deltas price from `time xasc trades where sym = `IBM;
select count i by sym from trades where sym in `IBM`AAPL;
select from trades where sym like "I*";
select from trades where size within 10 20;
select[2] from `price xdesc trades;
select[>price] sym, price from trades;
select sym, p2: price * 2 from trades where size > 10, size < 30;
update price: price * 10 from trades where sym = `MSFT;
delete price, size from trades;
select sum size by sym from select from trades where size > 5;
count trades;
cols trades;
exec distinct sym from trades;
meta trades;

/ Joins.
aj[`sym`time; `time xasc trades; quotes];
refs: ([sym: `IBM`MSFT] name: ("International Business Machines"; "Microsoft")); `time xasc trades lj refs;
refs: ([sym: `IBM] sector: enlist `tech); trades ij refs;
([] a: 1 2) uj ([] b: `x`y`z);
([] a: 1 2) , ([] a: 3 4);
([] a: 1 2) cross ([] b: `x`y);

/ ClickHouse queries as tables.
.ch.sql "SELECT number AS n FROM numbers(3)";
select s: sum n, m: max n from .ch.sql "SELECT number AS n FROM numbers(10)";
sq: {x*x}; select n, sq n from .ch.sql "SELECT number AS n FROM numbers(4)";
select n, s: sums n, p: prev n, nx: next n, m: 2 mavg n from .ch.sql "SELECT number AS n FROM numbers(5)";
select x: n by g: n mod 2 from .ch.sql "SELECT number AS n FROM numbers(6)";
select distinct g: n mod 3 from .ch.sql "SELECT number AS n FROM numbers(9)";
-2 # .ch.sql "SELECT number AS n FROM numbers(5)";

/ Errors.
foo; -- { serverError UNKNOWN_IDENTIFIER }
select from nosuchtable; -- { serverError UNKNOWN_IDENTIFIER }
1 2 3 + 1 2; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
{x,y}\[1 2 3]; -- { serverError NOT_IMPLEMENTED }
1 + `a; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SET dialect = 'clickhouse';
SELECT 'back to SQL';

DROP TABLE trades;
DROP TABLE quotes;
