-- `CHECK` constraints are built with the analyzer when a row is inserted.

-- A scalar subquery, a bare subquery and `exists` used to fail every `INSERT`.
CREATE TABLE t_ref (id Int32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_ref VALUES (1), (2), (3);

CREATE TABLE t_scalar (x Int32, CONSTRAINT c CHECK x < (SELECT 100)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_scalar VALUES (1);
INSERT INTO t_scalar VALUES (100); -- { serverError VIOLATED_CONSTRAINT }
SELECT 'scalar', * FROM t_scalar;

CREATE TABLE t_bare (x Int32, CONSTRAINT c CHECK (SELECT 1)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_bare VALUES (1);
ALTER TABLE t_bare ADD CONSTRAINT c0 CHECK (SELECT 0);
INSERT INTO t_bare VALUES (2); -- { serverError VIOLATED_CONSTRAINT }
SELECT 'bare', * FROM t_bare;

CREATE TABLE t_exists (x Int32, CONSTRAINT c CHECK exists(SELECT 1 FROM t_ref WHERE id = 3)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_exists VALUES (1);
ALTER TABLE t_exists ADD CONSTRAINT c0 CHECK exists(SELECT 1 FROM t_ref WHERE id = 4);
INSERT INTO t_exists VALUES (2); -- { serverError VIOLATED_CONSTRAINT }
SELECT 'exists', * FROM t_exists;

-- `IN` a subquery, a table and a literal set.
CREATE TABLE t_in
(
    x Int32,
    y Int32,
    z Int32,
    CONSTRAINT c1 CHECK x IN (SELECT id FROM t_ref),
    CONSTRAINT c2 CHECK y IN t_ref,
    CONSTRAINT c3 CHECK z IN (1, 2)
) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_in VALUES (1, 2, 1), (3, 3, 2);
INSERT INTO t_in VALUES (4, 1, 1); -- { serverError VIOLATED_CONSTRAINT }
INSERT INTO t_in VALUES (1, 4, 1); -- { serverError VIOLATED_CONSTRAINT }
INSERT INTO t_in VALUES (1, 1, 3); -- { serverError VIOLATED_CONSTRAINT }
SELECT 'in', * FROM t_in ORDER BY x;

-- Two constraints on the same column.
CREATE TABLE t_two (x Int32, CONSTRAINT c1 CHECK x > 0, CONSTRAINT c2 CHECK x < 10) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_two VALUES (5);
INSERT INTO t_two VALUES (0); -- { serverError VIOLATED_CONSTRAINT }
INSERT INTO t_two VALUES (10); -- { serverError VIOLATED_CONSTRAINT }
SELECT 'two', * FROM t_two;

-- Subcolumns are extracted from their parent column.
CREATE TABLE t_sub
(
    tup Tuple(a Int32, b Nullable(Int32)),
    j JSON(t Int64),
    m Map(String, Int32),
    CONSTRAINT c_tuple CHECK tup.a > 0,
    CONSTRAINT c_nullable CHECK tup.b.null = 0,
    CONSTRAINT c_json CHECK isNotNull(j.p),
    CONSTRAINT c_json_typed CHECK j.q.:Int64 > 0,
    CONSTRAINT c_json_declared CHECK j.t > 0,
    CONSTRAINT c_map CHECK has(m.keys, 'k')
) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_sub VALUES ((1, 1), '{"p": 1, "q": 1, "t": 1}', {'k': 1});
INSERT INTO t_sub VALUES ((0, 1), '{"p": 1, "q": 1, "t": 1}', {'k': 1}); -- { serverError VIOLATED_CONSTRAINT }
INSERT INTO t_sub VALUES ((1, NULL), '{"p": 1, "q": 1, "t": 1}', {'k': 1}); -- { serverError VIOLATED_CONSTRAINT }
INSERT INTO t_sub VALUES ((1, 1), '{"q": 1, "t": 1}', {'k': 1}); -- { serverError VIOLATED_CONSTRAINT }
INSERT INTO t_sub VALUES ((1, 1), '{"p": 1, "q": 0, "t": 1}', {'k': 1}); -- { serverError VIOLATED_CONSTRAINT }
INSERT INTO t_sub VALUES ((1, 1), '{"p": 1, "q": 1, "t": 0}', {'k': 1}); -- { serverError VIOLATED_CONSTRAINT }
INSERT INTO t_sub VALUES ((1, 1), '{"p": 1, "q": 1, "t": 1}', {'l': 1}); -- { serverError VIOLATED_CONSTRAINT }
SELECT 'sub', count() FROM t_sub;

-- The lambda argument `x` shadows the column `x`.
CREATE TABLE t_lambda (x Int32, arr Array(Int32), CONSTRAINT c CHECK arrayAll(x -> x > 0, arr)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lambda VALUES (-1, [1, 2]);
INSERT INTO t_lambda VALUES (1, [-1]); -- { serverError VIOLATED_CONSTRAINT }
SELECT 'lambda', * FROM t_lambda;

-- Constant constraints.
CREATE TABLE t_const (x Int32, CONSTRAINT c CHECK 2 > 1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_const VALUES (1);
SELECT 'const', * FROM t_const;
CREATE TABLE t_const_false (x Int32, CONSTRAINT c CHECK 1 > 2) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_const_false VALUES (1); -- { serverError VIOLATED_CONSTRAINT }

-- "Column values" of a violation are in the order of the header, the parent of a subcolumn included.
CREATE TABLE t_order (x Int32, tup Tuple(a Int32), z Int32, CONSTRAINT c CHECK z > 0 AND tup.a > 0 AND x > 0) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_order SETTINGS async_insert = 0 VALUES (0, tuple(1), 2); -- { serverError VIOLATED_CONSTRAINT }
SYSTEM FLUSH LOGS query_log;
SELECT 'order', extract(exception, 'Column values: (.*?)\\. \\(')
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'ExceptionWhileProcessing' AND query LIKE 'INSERT INTO t_order%';
