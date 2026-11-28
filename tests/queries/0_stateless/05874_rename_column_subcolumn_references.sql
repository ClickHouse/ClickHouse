-- Renaming a column also renames the references to its subcolumns in column defaults and TTLs, the table TTL,
-- constraints and skip indices, but not the references to an independent physical column `s.x`.

DROP TABLE IF EXISTS t_default;
DROP TABLE IF EXISTS t_ttl;
DROP TABLE IF EXISTS t_constraint;
DROP TABLE IF EXISTS t_index;
DROP TABLE IF EXISTS t_independent;

CREATE TABLE t_default
(
    s Tuple(x UInt32, y Tuple(z UInt32)),
    d UInt32 DEFAULT s.x,
    m UInt32 MATERIALIZED s.y.z,
    a UInt32 ALIAS `s.x` + 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_default (s) VALUES ((7, tuple(70)));
ALTER TABLE t_default RENAME COLUMN s TO renamed_s;
INSERT INTO t_default (renamed_s) VALUES ((9, tuple(90)));
SELECT name, default_expression FROM system.columns WHERE database = currentDatabase() AND table = 't_default' ORDER BY position;
DETACH TABLE t_default;
ATTACH TABLE t_default;
SELECT renamed_s.x, d, m, a FROM t_default ORDER BY d;

CREATE TABLE t_ttl
(
    s Tuple(x DateTime),
    v UInt32 TTL s.x + INTERVAL 1 DAY
)
ENGINE = MergeTree ORDER BY tuple() TTL s.x + INTERVAL 1 DAY;
INSERT INTO t_ttl VALUES (tuple(toDateTime('2100-01-01 00:00:00')), 3);
ALTER TABLE t_ttl RENAME COLUMN s TO renamed_s;
SELECT countSubstrings(create_table_query, 'TTL renamed_s.x + toIntervalDay(1)') FROM system.tables WHERE database = currentDatabase() AND name = 't_ttl';
DETACH TABLE t_ttl;
ATTACH TABLE t_ttl;
SELECT renamed_s.x, v FROM t_ttl;

CREATE TABLE t_constraint
(
    s Tuple(x Int32),
    CONSTRAINT positive_x CHECK s.x > 0
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_constraint VALUES (tuple(3));
ALTER TABLE t_constraint RENAME COLUMN s TO renamed_s;
INSERT INTO t_constraint VALUES (tuple(-3)); -- { serverError VIOLATED_CONSTRAINT }
DETACH TABLE t_constraint;
ATTACH TABLE t_constraint;
INSERT INTO t_constraint VALUES (tuple(5));
SELECT renamed_s.x FROM t_constraint ORDER BY renamed_s.x;

CREATE TABLE t_index
(
    id UInt64,
    s Tuple(x UInt64),
    INDEX idx s.x TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_index VALUES (1, tuple(10)), (2, tuple(20));
ALTER TABLE t_index RENAME COLUMN s TO renamed_s;
SELECT expr FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_index';
DETACH TABLE t_index;
ATTACH TABLE t_index;
SELECT id FROM t_index WHERE renamed_s.x = 20 SETTINGS force_data_skipping_indices = 'idx';

-- `s.x` is a column of its own, so renaming `s` keeps the references to it.
CREATE TABLE t_independent
(
    s UInt32,
    `s.x` UInt32,
    d UInt32 DEFAULT s.x,
    CONSTRAINT positive_x CHECK s.x > 0,
    INDEX idx s.x TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS share_nested_offsets = 0;
INSERT INTO t_independent (s, `s.x`) VALUES (1, 20);
ALTER TABLE t_independent RENAME COLUMN s TO renamed_s;
SELECT default_expression FROM system.columns WHERE database = currentDatabase() AND table = 't_independent' AND name = 'd';
SELECT expr FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_independent';
SELECT renamed_s, `s.x`, d FROM t_independent;

DROP TABLE t_default;
DROP TABLE t_ttl;
DROP TABLE t_constraint;
DROP TABLE t_index;
DROP TABLE t_independent;

-- A rename that would make the reference resolve to another column is refused.
CREATE TABLE t_rebind
(
    s Tuple(x UInt32),
    `r.x` UInt32 ALIAS 100,
    a UInt32 ALIAS s.x
)
ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_rebind RENAME COLUMN s TO r; -- { serverError BAD_ARGUMENTS }

-- `s.x` is an alias column that owns the reference `s.x.y`.
CREATE TABLE t_alias_owner
(
    s UInt32,
    `s.x` Tuple(y UInt32) ALIAS CAST(tuple(20), 'Tuple(y UInt32)'),
    d UInt32 ALIAS s.x.y
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS share_nested_offsets = 0;
INSERT INTO t_alias_owner (s) VALUES (1);
ALTER TABLE t_alias_owner RENAME COLUMN s TO r;
SELECT r, d FROM t_alias_owner;

-- The column name `n.x` spans two parts of the reference `n.x.z`.
CREATE TABLE t_nested
(
    n Nested(x Tuple(z UInt32)),
    d Array(UInt32) DEFAULT n.x.z
)
ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_nested RENAME COLUMN `n.x` TO `n.y`;
SELECT default_expression FROM system.columns WHERE database = currentDatabase() AND table = 't_nested' AND name = 'd';
INSERT INTO t_nested (`n.y`) VALUES ([tuple(7)]);
SELECT d FROM t_nested;

-- `s.x.y` refers to the subcolumn of `s`, not to the element of the alias column `s.x`.
CREATE TABLE t_alias_shadowed
(
    s Tuple(x Tuple(y UInt32)),
    `s.x` Tuple(y UInt32) ALIAS CAST(tuple(20), 'Tuple(y UInt32)'),
    a UInt32 ALIAS s.x.y
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS share_nested_offsets = 0;
INSERT INTO t_alias_shadowed (s) VALUES (tuple(tuple(7)));
ALTER TABLE t_alias_shadowed RENAME COLUMN s TO r;
DETACH TABLE t_alias_shadowed;
ATTACH TABLE t_alias_shadowed;
SELECT r.x.y, a FROM t_alias_shadowed;

-- The column name `s` is a part of the first part of the reference `s.x`.z.
CREATE TABLE t_quoted
(
    s Tuple(x Tuple(z UInt32)),
    a UInt32 ALIAS `s.x`.z
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_quoted (s) VALUES (tuple(tuple(7)));
ALTER TABLE t_quoted RENAME COLUMN s TO r;
SELECT a FROM t_quoted;

-- The column `r.x` is dropped before the rename in the same query.
CREATE TABLE t_drop_rename
(
    s Tuple(x Int32),
    `r.x` UInt32 ALIAS 100,
    CONSTRAINT c CHECK s.x > 0
)
ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_drop_rename DROP COLUMN `r.x`, RENAME COLUMN s TO r;
INSERT INTO t_drop_rename (r) VALUES (tuple(-1)); -- { serverError VIOLATED_CONSTRAINT }
INSERT INTO t_drop_rename (r) VALUES (tuple(1));
SELECT r.x FROM t_drop_rename;

-- The new name is a lambda parameter where `s.x` is used; the lambda parameter `s` is not the column.
CREATE TABLE t_lambda
(
    s Tuple(x UInt32),
    a Array(UInt32) ALIAS arrayMap(r -> s.x + r.x, [CAST(tuple(1), 'Tuple(x UInt32)')]),
    b Array(UInt32) ALIAS arrayMap(s -> s.x, [CAST(tuple(1), 'Tuple(x UInt32)')])
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lambda (s) VALUES (tuple(7));
ALTER TABLE t_lambda RENAME COLUMN s TO r; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_lambda RENAME COLUMN s TO q;
SELECT a, b FROM t_lambda;
SELECT default_expression FROM system.columns WHERE database = currentDatabase() AND table = 't_lambda' AND name = 'b';

-- The constraint that uses `s.x` is replaced before the rename in the same query.
CREATE TABLE t_replace_constraint
(
    s Tuple(x Int32),
    `r.x` UInt32 ALIAS 100,
    CONSTRAINT c CHECK s.x > 0
)
ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_replace_constraint DROP CONSTRAINT c, ADD CONSTRAINT c CHECK 1, RENAME COLUMN s TO r;
INSERT INTO t_replace_constraint (r) VALUES (tuple(-1));
SELECT r FROM t_replace_constraint;

-- After the rename `r.x.y` still refers to the subcolumn of `r`, not to the element of the alias column `r.x`.
CREATE TABLE t_alias_prefix
(
    s Tuple(x Tuple(y UInt32)),
    `r.x` Tuple(y UInt32) ALIAS CAST(tuple(20), 'Tuple(y UInt32)'),
    a UInt32 ALIAS s.x.y
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS share_nested_offsets = 0;
INSERT INTO t_alias_prefix (s) VALUES (tuple(tuple(7)));
ALTER TABLE t_alias_prefix RENAME COLUMN s TO r;
DETACH TABLE t_alias_prefix;
ATTACH TABLE t_alias_prefix;
SELECT a, r.x.y FROM t_alias_prefix;

DROP TABLE t_rebind;
DROP TABLE t_alias_owner;
DROP TABLE t_nested;
DROP TABLE t_alias_shadowed;
DROP TABLE t_quoted;
DROP TABLE t_drop_rename;
DROP TABLE t_lambda;
DROP TABLE t_replace_constraint;
DROP TABLE t_alias_prefix;
