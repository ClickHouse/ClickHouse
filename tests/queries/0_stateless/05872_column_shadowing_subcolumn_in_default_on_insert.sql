-- A `DEFAULT` or `MATERIALIZED` expression that reads a subcolumn takes it, on `INSERT`, from the column that
-- the table gives the name: next to a `JSON` column `a` and a `Tuple` column `a.x`, the subcolumn `a.x.y` is
-- the element `y` of `a.x`, not the path `x.y` of `a`.

DROP TABLE IF EXISTS t_shadow_default;
CREATE TABLE t_shadow_default
(
    id UInt64,
    a JSON,
    `a.x` Tuple(y UInt64),
    d UInt64 DEFAULT toUInt64(`a.x.y`),
    m UInt64 MATERIALIZED toUInt64(`a.x.y`)
)
ENGINE = MergeTree ORDER BY id;

SELECT '-- INSERT ... VALUES';
INSERT INTO t_shadow_default (id, a, `a.x`) VALUES (1, '{"x" : {"y" : 11}}', tuple(44));
SELECT id, `a.x.y`, d, m FROM t_shadow_default ORDER BY id;

SELECT '-- INSERT ... SELECT';
TRUNCATE TABLE t_shadow_default;
INSERT INTO t_shadow_default (id, a, `a.x`) SELECT 2, '{"x" : {"y" : 11}}', tuple(55);
SELECT id, `a.x.y`, d, m FROM t_shadow_default ORDER BY id;

SELECT '-- defaults of the fields omitted in the input format';
TRUNCATE TABLE t_shadow_default;
INSERT INTO t_shadow_default FORMAT JSONEachRow {"id" : 3, "a" : {"x" : {"y" : 11}}, "a.x" : {"y" : 66}}

SELECT id, `a.x.y`, d, m FROM t_shadow_default ORDER BY id;

SELECT '-- INSERT through a materialized view';
DROP TABLE IF EXISTS t_shadow_default_src;
DROP TABLE IF EXISTS t_shadow_default_mv;
TRUNCATE TABLE t_shadow_default;
CREATE TABLE t_shadow_default_src (id UInt64, a JSON, `a.x` Tuple(y UInt64)) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW t_shadow_default_mv TO t_shadow_default AS SELECT id, a, `a.x` FROM t_shadow_default_src;
INSERT INTO t_shadow_default_src VALUES (4, '{"x" : {"y" : 11}}', tuple(77));
SELECT id, `a.x.y`, d, m FROM t_shadow_default ORDER BY id;

DROP TABLE t_shadow_default_mv;
DROP TABLE t_shadow_default_src;
DROP TABLE t_shadow_default;

-- The `INSERT` omits the column `a.x`. The expressions read the subcolumn of the value that the column gets:
-- the default value of its type or the value of its `DEFAULT` expression, and not the path `x.y` of `a`.
DROP TABLE IF EXISTS t_shadow_default_omitted;
DROP TABLE IF EXISTS t_shadow_default_omitted_expr;
CREATE TABLE t_shadow_default_omitted
(
    id UInt64,
    a JSON,
    `a.x` Tuple(y UInt64),
    d UInt64 DEFAULT toUInt64(`a.x.y`),
    m UInt64 MATERIALIZED toUInt64(`a.x.y`)
)
ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_shadow_default_omitted_expr
(
    id UInt64,
    a JSON,
    `a.x` Tuple(y UInt64) DEFAULT tuple(toUInt64(44)),
    d UInt64 DEFAULT toUInt64(`a.x.y`),
    m UInt64 MATERIALIZED toUInt64(a.x.y)
)
ENGINE = MergeTree ORDER BY id;

SELECT '-- omitted column: INSERT ... VALUES';
INSERT INTO t_shadow_default_omitted (id, a) VALUES (1, '{"x" : {"y" : 11}}');
INSERT INTO t_shadow_default_omitted_expr (id, a) VALUES (1, '{"x" : {"y" : 11}}');

SELECT '-- omitted column: INSERT ... SELECT';
INSERT INTO t_shadow_default_omitted (id, a) SELECT 2, '{"x" : {"y" : 11}}';
INSERT INTO t_shadow_default_omitted_expr (id, a) SELECT 2, '{"x" : {"y" : 11}}';

SELECT '-- omitted column: defaults of the fields omitted in the input format';
INSERT INTO t_shadow_default_omitted FORMAT JSONEachRow {"id" : 3, "a" : {"x" : {"y" : 11}}}

INSERT INTO t_shadow_default_omitted_expr FORMAT JSONEachRow {"id" : 3, "a" : {"x" : {"y" : 11}}}

SELECT '-- omitted column: INSERT through a materialized view';
DROP TABLE IF EXISTS t_shadow_default_omitted_src;
DROP TABLE IF EXISTS t_shadow_default_omitted_mv;
DROP TABLE IF EXISTS t_shadow_default_omitted_expr_mv;
CREATE TABLE t_shadow_default_omitted_src (id UInt64, a JSON) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW t_shadow_default_omitted_mv TO t_shadow_default_omitted AS SELECT id, a FROM t_shadow_default_omitted_src;
CREATE MATERIALIZED VIEW t_shadow_default_omitted_expr_mv TO t_shadow_default_omitted_expr AS SELECT id, a FROM t_shadow_default_omitted_src;
INSERT INTO t_shadow_default_omitted_src VALUES (4, '{"x" : {"y" : 11}}');

SELECT id, `a.x.y`, d, m FROM t_shadow_default_omitted ORDER BY id;
SELECT id, `a.x.y`, d, m FROM t_shadow_default_omitted_expr ORDER BY id;

DROP TABLE t_shadow_default_omitted_mv;
DROP TABLE t_shadow_default_omitted_expr_mv;
DROP TABLE t_shadow_default_omitted_src;
DROP TABLE t_shadow_default_omitted;
DROP TABLE t_shadow_default_omitted_expr;
