-- A MATERIALIZED column recomputed by a mutation or a column TTL must keep the name and the alias of a
-- subcolumn it reads, as at INSERT.

SET mutations_sync = 2;

-- The name of the subcolumn is visible to `formatRow`.
DROP TABLE IF EXISTS t_name;
CREATE TABLE t_name (t Tuple(a UInt32), m String MATERIALIZED formatRowNoNewline('JSONEachRow', t.a)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_name (t) VALUES (tuple(100));
SELECT t, m FROM t_name;
ALTER TABLE t_name UPDATE t = tuple(200) WHERE 1;
SELECT t, m FROM t_name;
DROP TABLE t_name;

-- An alias of the subcolumn is referenced later in the expression.
DROP TABLE IF EXISTS t_alias;
CREATE TABLE t_alias (t Tuple(a UInt32), z UInt32, m String MATERIALIZED concat(toString(t.a AS r), '-', toString(r))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_alias (t, z) VALUES (tuple(100), 5);
ALTER TABLE t_alias UPDATE z = 6 WHERE 1;
SELECT z, m FROM t_alias;
ALTER TABLE t_alias UPDATE t = tuple(200) WHERE 1;
SELECT t, m FROM t_alias;
ALTER TABLE t_alias CLEAR COLUMN t;
SELECT t, m FROM t_alias;
DROP TABLE t_alias;

-- The alias has the name of another column.
DROP TABLE IF EXISTS t_shadow;
CREATE TABLE t_shadow (t Tuple(a UInt32), z UInt32, m String MATERIALIZED concat(toString(t.a AS z), '-', toString(z))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_shadow (t, z) VALUES (tuple(100), 5);
SELECT t, z, m FROM t_shadow;
ALTER TABLE t_shadow UPDATE t = tuple(200) WHERE 1;
SELECT t, z, m FROM t_shadow;
ALTER TABLE t_shadow CLEAR COLUMN t;
SELECT t, z, m FROM t_shadow;
DROP TABLE t_shadow;

-- The subcolumn is read through an ALIAS column.
DROP TABLE IF EXISTS t_alias_column;
CREATE TABLE t_alias_column (t Tuple(a UInt32), ac UInt32 ALIAS t.a, m String MATERIALIZED formatRowNoNewline('JSONEachRow', ac)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_alias_column (t) VALUES (tuple(100));
SELECT t, m FROM t_alias_column;
ALTER TABLE t_alias_column UPDATE t = tuple(200) WHERE 1;
SELECT t, m FROM t_alias_column;
DROP TABLE t_alias_column;

-- A column TTL resets the parent column; `m_root` reads the subcolumn as the whole expression.
DROP TABLE IF EXISTS t_ttl;
CREATE TABLE t_ttl
(
    d DateTime,
    t Tuple(a Int32) TTL d + INTERVAL 1 SECOND,
    z Int32,
    m_name String MATERIALIZED formatRowNoNewline('JSONEachRow', t.a),
    m_shadow String MATERIALIZED concat(toString(t.a AS z), '-', toString(z)),
    m_root Int32 MATERIALIZED t.a
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_ttl (d, t, z) VALUES ('2000-01-01 00:00:00', (7), 5);
OPTIMIZE TABLE t_ttl FINAL;
SELECT t, m_name, m_shadow, m_root FROM t_ttl;
DROP TABLE t_ttl;

DROP TABLE IF EXISTS t_ttl_alias;
CREATE TABLE t_ttl_alias
(
    d DateTime,
    t Tuple(a Int32) TTL d + INTERVAL 1 SECOND,
    m_alias String MATERIALIZED concat(toString(t.a AS r), '-', toString(r))
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_ttl_alias (d, t) VALUES ('2000-01-01 00:00:00', (7));
OPTIMIZE TABLE t_ttl_alias FINAL;
SELECT t, m_alias FROM t_ttl_alias;
DROP TABLE t_ttl_alias;

-- The name of the subcolumn is already an alias in the expression, so the subcolumn is not named.
DROP TABLE IF EXISTS t_collision;
CREATE TABLE t_collision (t Tuple(a UInt32), z UInt32, m String MATERIALIZED concat(toString(t.a), '-', arrayStringConcat(arrayMap(x -> toString(x AS `t.a`), [z])))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_collision (t, z) VALUES (tuple(100), 5);
SELECT t, z, m FROM t_collision;
ALTER TABLE t_collision UPDATE t = tuple(200) WHERE 1;
SELECT t, z, m FROM t_collision;
DROP TABLE t_collision;

-- An alias of the subcolumn is also given to another expression inside a lambda.
DROP TABLE IF EXISTS t_lambda_alias;
CREATE TABLE t_lambda_alias (t Tuple(a UInt32), z UInt32, m String MATERIALIZED concat(toString(t.a AS r), '-', arrayStringConcat(arrayMap(x -> toString(x AS r), [z])))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lambda_alias (t, z) VALUES (tuple(100), 5);
SELECT t, z, m FROM t_lambda_alias;
ALTER TABLE t_lambda_alias UPDATE t = tuple(200) WHERE 1;
SELECT t, z, m FROM t_lambda_alias;
ALTER TABLE t_lambda_alias UPDATE z = 6 WHERE 1;
SELECT t, z, m FROM t_lambda_alias;
DROP TABLE t_lambda_alias;

-- An alias of the subcolumn set inside a lambda has the name of a column read outside it.
DROP TABLE IF EXISTS t_lambda_outer;
CREATE TABLE t_lambda_outer (t Tuple(a UInt32), z UInt32, m String MATERIALIZED concat(arrayStringConcat(arrayMap(x -> toString(t.a AS z), [1])), '-', toString(z))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lambda_outer (t, z) VALUES (tuple(100), 5);
SELECT t, z, m FROM t_lambda_outer;
ALTER TABLE t_lambda_outer UPDATE z = 6 WHERE 1;
SELECT t, z, m FROM t_lambda_outer;
ALTER TABLE t_lambda_outer UPDATE t = tuple(200) WHERE 1;
SELECT t, z, m FROM t_lambda_outer;
DROP TABLE t_lambda_outer;

-- The same, recomputed by a column TTL.
DROP TABLE IF EXISTS t_ttl_lambda;
CREATE TABLE t_ttl_lambda
(
    d DateTime,
    t Tuple(a Int32) TTL d + INTERVAL 1 SECOND,
    z Int32,
    m String MATERIALIZED concat(arrayStringConcat(arrayMap(x -> toString(t.a AS z), [1])), '-', toString(z))
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_ttl_lambda (d, t, z) VALUES ('2000-01-01 00:00:00', (7), 5);
OPTIMIZE TABLE t_ttl_lambda FINAL;
SELECT t, z, m FROM t_ttl_lambda;
DROP TABLE t_ttl_lambda;

-- An alias of the subcolumn set and read inside a lambda.
DROP TABLE IF EXISTS t_lambda_inner;
CREATE TABLE t_lambda_inner (t Tuple(a UInt32), m String MATERIALIZED arrayStringConcat(arrayMap(x -> concat(toString(t.a AS r), '-', toString(r)), [1]))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lambda_inner (t) VALUES (tuple(100));
SELECT t, m FROM t_lambda_inner;
ALTER TABLE t_lambda_inner UPDATE t = tuple(200) WHERE 1;
SELECT t, m FROM t_lambda_inner;
DROP TABLE t_lambda_inner;

-- A lightweight UPDATE recomputes through the same expression.
DROP TABLE IF EXISTS t_lwu;
CREATE TABLE t_lwu (k UInt8, t Tuple(a UInt32), z UInt32, m String MATERIALIZED concat(toString(t.a AS z), '-', toString(z)), m_name String MATERIALIZED formatRowNoNewline('JSONEachRow', t.a)) ENGINE = MergeTree ORDER BY k SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_lwu (k, t, z) VALUES (1, tuple(100), 5);
SET enable_lightweight_update = 1;
UPDATE t_lwu SET t = tuple(200) WHERE k = 1;
SELECT t, z, m, m_name FROM t_lwu;
OPTIMIZE TABLE t_lwu FINAL;
SELECT t, z, m, m_name FROM t_lwu;
DROP TABLE t_lwu;
