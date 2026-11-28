-- A `DEFAULT` or `MATERIALIZED` expression that reads a subcolumn of an `EPHEMERAL` column takes it, on `INSERT`,
-- from that column: next to a `Map` column `a` and an `EPHEMERAL` `Tuple` column `a.key_x`, the subcolumn
-- `a.key_x.y` is the element `y` of `a.key_x`, not the value of the key `x.y` of `a`.

DROP TABLE IF EXISTS t_shadow_ephemeral;
CREATE TABLE t_shadow_ephemeral
(
    id UInt64,
    a Map(String, UInt64),
    `a.key_x` Tuple(y UInt64) EPHEMERAL,
    d UInt64 DEFAULT `a.key_x.y`,
    m UInt64 MATERIALIZED `a.key_x.y`
)
ENGINE = MergeTree ORDER BY id;

SELECT '-- the EPHEMERAL column is supplied';
INSERT INTO t_shadow_ephemeral (id, a, `a.key_x`) VALUES (1, map('x.y', 11), tuple(44));
INSERT INTO t_shadow_ephemeral (id, a, `a.key_x`) SELECT 2, map('x.y', 12), tuple(45);
SELECT '-- the EPHEMERAL column is omitted';
INSERT INTO t_shadow_ephemeral (id, a) VALUES (3, map('x.y', 13));
SELECT id, a, d, m FROM t_shadow_ephemeral ORDER BY id;

DROP TABLE t_shadow_ephemeral;

SELECT '-- the EPHEMERAL column has an explicit default';
DROP TABLE IF EXISTS t_shadow_ephemeral_default;
CREATE TABLE t_shadow_ephemeral_default
(
    id UInt64,
    a Map(String, UInt64),
    `a.key_x` Tuple(y UInt64) EPHEMERAL tuple(55),
    d UInt64 DEFAULT `a.key_x.y`,
    m UInt64 MATERIALIZED `a.key_x.y`
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_shadow_ephemeral_default (id, a, `a.key_x`) VALUES (1, map('x.y', 11), tuple(44));
INSERT INTO t_shadow_ephemeral_default (id, a) VALUES (2, map('x.y', 12));
INSERT INTO t_shadow_ephemeral_default (id, a) SELECT 3, map('x.y', 13);
SELECT id, a, d, m FROM t_shadow_ephemeral_default ORDER BY id;

DROP TABLE t_shadow_ephemeral_default;
