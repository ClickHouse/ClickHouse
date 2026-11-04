-- A MATERIALIZED column recomputed by a mutation or a column TTL must keep the name and the alias of a
-- subcolumn it reads, as at INSERT.

SET mutations_sync = 2;

-- The name of the subcolumn is visible to `formatRow`.
DROP TABLE IF EXISTS t_name;
CREATE TABLE t_name (t Tuple(a UInt32), m String MATERIALIZED formatRowNoNewline('JSONEachRow', t.a)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_name (t) VALUES (tuple(100));
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
ALTER TABLE t_shadow UPDATE t = tuple(200) WHERE 1;
SELECT t, z, m FROM t_shadow;
ALTER TABLE t_shadow CLEAR COLUMN t;
SELECT t, z, m FROM t_shadow;
DROP TABLE t_shadow;

-- The subcolumn is read through an ALIAS column.
DROP TABLE IF EXISTS t_alias_column;
CREATE TABLE t_alias_column (t Tuple(a UInt32), ac UInt32 ALIAS t.a, m String MATERIALIZED formatRowNoNewline('JSONEachRow', ac)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_alias_column (t) VALUES (tuple(100));
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
ALTER TABLE t_collision UPDATE t = tuple(200) WHERE 1;
SELECT t, z, m FROM t_collision;
DROP TABLE t_collision;
