-- A column TTL on one `Nested` subcolumn resets it to arrays of defaults of the same size,
-- because its array sizes are shared with the other subcolumns, which keep their values.

DROP TABLE IF EXISTS t_nested_ttl_wide;
DROP TABLE IF EXISTS t_nested_ttl_rows;
DROP TABLE IF EXISTS t_nested_ttl_compact;
DROP TABLE IF EXISTS t_nested_ttl_enum;
DROP TABLE IF EXISTS t_nested_ttl_dependent;
DROP TABLE IF EXISTS t_nested_ttl_single;
DROP TABLE IF EXISTS t_nested_ttl_added;

-- A Wide part with every row expired, the type change also rewrites the subcolumn.
CREATE TABLE t_nested_ttl_wide (id UInt64, ts DateTime, n Nested(a String, b String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_nested_ttl_wide VALUES (1, '2020-01-01 00:00:00', ['a', 'b'], ['x', 'y']);
ALTER TABLE t_nested_ttl_wide MODIFY COLUMN `n.a` Array(Nullable(String)) TTL ts + INTERVAL 1 DAY;
ALTER TABLE t_nested_ttl_wide MATERIALIZE TTL SETTINGS mutations_sync = 2;
SELECT 'wide', id, `n.a`, `n.b` FROM t_nested_ttl_wide;

-- Only some rows of the part are expired.
CREATE TABLE t_nested_ttl_rows (id UInt64, ts DateTime, n Nested(a String, b String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_nested_ttl_rows VALUES (1, '2020-01-01 00:00:00', ['a', 'b'], ['x', 'y']), (2, '2100-01-01 00:00:00', ['c'], ['z']);
ALTER TABLE t_nested_ttl_rows MODIFY COLUMN `n.a` Array(String) TTL ts + INTERVAL 1 DAY SETTINGS materialize_ttl_after_modify = 0;
OPTIMIZE TABLE t_nested_ttl_rows FINAL;
SELECT 'rows', id, `n.a`, `n.b` FROM t_nested_ttl_rows ORDER BY id;

-- A Compact part stores the sizes of each subcolumn separately, but it gets the same default.
CREATE TABLE t_nested_ttl_compact (id UInt64, ts DateTime, n Nested(a String, b String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000;

INSERT INTO t_nested_ttl_compact VALUES (1, '2020-01-01 00:00:00', ['a', 'b'], ['x', 'y']);
ALTER TABLE t_nested_ttl_compact MODIFY COLUMN `n.a` Array(String) TTL ts + INTERVAL 1 DAY;
ALTER TABLE t_nested_ttl_compact MATERIALIZE TTL SETTINGS mutations_sync = 2;
SELECT 'compact', id, `n.a`, `n.b` FROM t_nested_ttl_compact;

-- The default of an `Enum` element is its first value, not zero.
CREATE TABLE t_nested_ttl_enum (id UInt64, ts DateTime, n Nested(a Enum8('ok' = 1, 'other' = 2), b String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_nested_ttl_enum VALUES (1, '2020-01-01 00:00:00', ['other', 'other'], ['x', 'y']), (2, '2100-01-01 00:00:00', ['other'], ['z']);
ALTER TABLE t_nested_ttl_enum MODIFY COLUMN `n.a` Array(Enum8('ok' = 1, 'other' = 2)) TTL ts + INTERVAL 1 DAY SETTINGS materialize_ttl_after_modify = 0;
OPTIMIZE TABLE t_nested_ttl_enum FINAL;
SELECT 'enum', id, `n.a`, `n.b` FROM t_nested_ttl_enum ORDER BY id;

-- The `MATERIALIZED` subcolumns are computed from the same sizes, also in a later merge.
CREATE TABLE t_nested_ttl_dependent
(
    id UInt64, ts DateTime, n Nested(a String, c String),
    `n.b` Array(String) MATERIALIZED arrayMap(x -> concat('b', x), `n.a`),
    m UInt64 MATERIALIZED length(`n.a`)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_nested_ttl_dependent (id, ts, `n.a`, `n.c`) VALUES (1, '2020-01-01 00:00:00', ['a', 'b'], ['x', 'y']);
ALTER TABLE t_nested_ttl_dependent MODIFY COLUMN `n.a` Array(String) TTL ts + INTERVAL 1 DAY;
ALTER TABLE t_nested_ttl_dependent MATERIALIZE TTL SETTINGS mutations_sync = 2;
SELECT 'dependent', `n.a`, `n.b`, `n.c`, m FROM t_nested_ttl_dependent;
OPTIMIZE TABLE t_nested_ttl_dependent FINAL SETTINGS optimize_skip_merged_partitions = 0;
SELECT 'dependent', `n.a`, `n.b`, `n.c`, m FROM t_nested_ttl_dependent;

-- The sizes are kept when every subcolumn expires.
CREATE TABLE t_nested_ttl_single (id UInt64, ts DateTime, n Nested(a String, b String), m UInt64 MATERIALIZED length(`n.a`))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_nested_ttl_single VALUES (1, '2020-01-01 00:00:00', ['a', 'b'], ['x', 'y']);
ALTER TABLE t_nested_ttl_single
    MODIFY COLUMN `n.a` Array(String) TTL ts + INTERVAL 1 DAY,
    MODIFY COLUMN `n.b` Array(String) TTL ts + INTERVAL 1 DAY
SETTINGS materialize_ttl_after_modify = 0;
OPTIMIZE TABLE t_nested_ttl_single FINAL;
SELECT 'single', `n.a`, `n.b`, m, length(`n.a`) FROM t_nested_ttl_single;

-- A subcolumn that no part stores gets the sizes of the other subcolumns, as in a read.
-- A vertical merge does not use the TTL transform for these columns, so it is disabled here.
CREATE TABLE t_nested_ttl_added (id UInt64, n Nested(a String, c String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, enable_vertical_merge_algorithm = 0;

INSERT INTO t_nested_ttl_added VALUES (1, ['a', 'b'], ['x', 'y']);
ALTER TABLE t_nested_ttl_added ADD COLUMN `n.d` Array(String);
ALTER TABLE t_nested_ttl_added ADD COLUMN `n.e` Array(String) MATERIALIZED arrayMap(x -> concat('e', x), `n.d`);
ALTER TABLE t_nested_ttl_added ADD COLUMN ts DateTime DEFAULT '2020-01-01 00:00:00';
ALTER TABLE t_nested_ttl_added MODIFY TTL ts + INTERVAL 1 DAY WHERE id = 100 SETTINGS materialize_ttl_after_modify = 0;
OPTIMIZE TABLE t_nested_ttl_added FINAL;
SELECT 'added', `n.a`, `n.c`, `n.d`, `n.e` FROM t_nested_ttl_added;

DROP TABLE t_nested_ttl_wide;
DROP TABLE t_nested_ttl_rows;
DROP TABLE t_nested_ttl_compact;
DROP TABLE t_nested_ttl_enum;
DROP TABLE t_nested_ttl_dependent;
DROP TABLE t_nested_ttl_single;
DROP TABLE t_nested_ttl_added;
