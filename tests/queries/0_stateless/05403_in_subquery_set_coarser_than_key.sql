-- Tags: no-parallel-replicas
-- no-parallel-replicas: the EXPLAIN checks read the local key analysis.

-- `IN (subquery)` casts the key into the subquery's type, while index analysis casts the subquery values into
-- the key type; when the first cast merges distinct keys the index must not prune (#124823).

CREATE TABLE t_dt (d DateTime('UTC')) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 1;
INSERT INTO t_dt VALUES ('2023-02-01 12:00:01'), ('2023-02-03 13:00:00');
SELECT count() FROM t_dt WHERE d IN (SELECT toDate('2023-02-01'));
SELECT count() FROM t_dt WHERE d NOT IN (SELECT toDate('2023-02-01'));

CREATE TABLE t_dt64 (d DateTime64(6, 'UTC')) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 1;
INSERT INTO t_dt64 VALUES ('2023-02-01 12:00:00.000001'), ('2023-02-01 13:00:00');
SELECT count() FROM t_dt64 WHERE d IN (SELECT toDateTime64('2023-02-01 12:00:00', 3, 'UTC'));

CREATE TABLE t_dec (x Decimal64(3)) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 1;
INSERT INTO t_dec VALUES (1.234), (2);
SELECT count() FROM t_dec WHERE x IN (SELECT toDecimal32(1.23, 2));

-- The AST fuzzer shape: `01891_not_in_partition_prune` with `t1.date` turned into a `DateTime`.
CREATE TABLE t1 (date DateTime('Asia/Tokyo'), a Float64, b String) ENGINE = MergeTree ORDER BY date SETTINGS index_granularity = 1;
CREATE TABLE t2 (date Date, a Float64, b String) ENGINE = MergeTree ORDER BY date;
INSERT INTO t1 (a, b) VALUES (1, 'one'), (2, 'two');
INSERT INTO t2 (a, b) VALUES (2, 'two'), (3, 'three');
SELECT a, b FROM t1 WHERE (date, a, b) IN (SELECT date, a, b FROM t2) ORDER BY a;
SELECT a, b FROM t1 WHERE NOT ((date, a, b) NOT IN (SELECT date, a, b FROM t2)) ORDER BY a;

CREATE TABLE t_part (d DateTime('UTC')) ENGINE = MergeTree PARTITION BY d ORDER BY tuple();
INSERT INTO t_part VALUES ('2023-02-01 12:00:01'), ('2023-02-03 13:00:00');
SELECT count() FROM t_part WHERE d IN (SELECT toDate('2023-02-01'));

CREATE TABLE t_hash (x Decimal(10, 4)) ENGINE = MergeTree PARTITION BY cityHash64(x) ORDER BY tuple();
INSERT INTO t_hash VALUES (1.2300), (1.2345), (2.0000);
SELECT x FROM t_hash WHERE x IN (SELECT toDecimal32(1.23, 2)) ORDER BY x;

CREATE TABLE t_skip (d DateTime('UTC'), INDEX i d TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_skip VALUES ('2023-02-01 12:00:01'), ('2023-02-03 13:00:00');
SELECT count() FROM t_skip WHERE d IN (SELECT toDate('2023-02-01')) SETTINGS use_skip_indexes_on_data_read = 0;

CREATE TABLE t_stat (d DateTime('Asia/Tokyo'), b String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS add_minmax_index_for_string_columns = 1, auto_statistics_types = 'minmax';
INSERT INTO t_stat VALUES (0, 'two');
SELECT count() FROM t_stat WHERE (d, b) IN (SELECT toDate('1970-01-01'), 'two') SETTINGS use_statistics_for_part_pruning = 1, use_skip_indexes_on_data_read = 0;

-- A set whose cast merges nothing keeps pruning.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT d FROM t_dt WHERE d IN (SELECT toDateTime('2023-02-01 12:00:01', 'UTC'))) WHERE explain ILIKE '%Granules: 1/2%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT d FROM t_dt WHERE toDate(d) IN (SELECT toDate('2023-02-01'))) WHERE explain ILIKE '%Granules: 1/2%';
CREATE TABLE t_date (d Date) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 1;
INSERT INTO t_date VALUES ('2023-02-01'), ('2023-02-03');
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT d FROM t_date WHERE d IN (SELECT toDateTime('2023-02-01 00:00:00', 'UTC'))) WHERE explain ILIKE '%Granules: 1/2%';

DROP TABLE t_dt;
DROP TABLE t_dt64;
DROP TABLE t_dec;
DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t_part;
DROP TABLE t_hash;
DROP TABLE t_skip;
DROP TABLE t_stat;
DROP TABLE t_date;
