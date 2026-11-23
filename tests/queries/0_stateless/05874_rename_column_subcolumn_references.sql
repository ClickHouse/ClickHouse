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
