-- A `MODIFY COLUMN` that changes the type of a column rebuilt only the projections having the column in their key. A
-- projection that computes something from the column, such as `toHour(dt)` or an aggregate state, kept the values it
-- computed with the old type, or the rows that its `WHERE` selected with it, and returned them for the queries it served.

DROP TABLE IF EXISTS t_aggregate;
DROP TABLE IF EXISTS t_expression;
DROP TABLE IF EXISTS t_plain;
DROP TABLE IF EXISTS t_filter;
DROP TABLE IF EXISTS t_absent;
DROP TABLE IF EXISTS t_default;
DROP TABLE IF EXISTS t_alias;
DROP TABLE IF EXISTS t_subcolumn;
DROP TABLE IF EXISTS t_default_subcolumn;

CREATE TABLE t_aggregate (id UInt64, dt DateTime('UTC'), PROJECTION p (SELECT toHour(dt) AS h, count() GROUP BY h))
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_aggregate SELECT number, toDateTime('2026-01-01 00:00:00', 'UTC') + INTERVAL number HOUR FROM numbers(5);
ALTER TABLE t_aggregate MODIFY COLUMN dt DateTime64(0, 'Asia/Tokyo') SETTINGS mutations_sync = 2;
SELECT groupArray(h) FROM (SELECT toHour(dt) AS h, count() FROM t_aggregate GROUP BY h ORDER BY h)
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT groupArray(h) FROM (SELECT toHour(dt) AS h, count() FROM t_aggregate GROUP BY h ORDER BY h)
SETTINGS optimize_use_projections = 0;

CREATE TABLE t_expression (id UInt64, dt DateTime('UTC'), PROJECTION p (SELECT id, toHour(dt) AS h ORDER BY h))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_expression SELECT number, toDateTime('2026-01-01 00:00:00', 'UTC') + INTERVAL number HOUR FROM numbers(5);
ALTER TABLE t_expression MODIFY COLUMN dt DateTime64(0, 'Asia/Tokyo') SETTINGS mutations_sync = 2;
SELECT groupArray(id) FROM t_expression WHERE toHour(dt) = 9 SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT groupArray(id) FROM t_expression WHERE toHour(dt) = 9 SETTINGS optimize_use_projections = 0;

-- A projection with a `WHERE` on the column selected its rows with the old type, even if it stores the column as is.
CREATE TABLE t_filter (id UInt64, dt DateTime('UTC'), PROJECTION p (SELECT id, dt WHERE toHour(dt) = 9 ORDER BY id))
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_filter SELECT number, toDateTime('2026-01-01 00:00:00', 'UTC') + INTERVAL number HOUR FROM numbers(24);
ALTER TABLE t_filter MODIFY COLUMN dt DateTime64(0, 'Asia/Tokyo') SETTINGS mutations_sync = 2;
SELECT groupArray(id) FROM t_filter WHERE toHour(dt) = 9
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1, optimize_read_in_order = 0;
SELECT groupArray(id) FROM t_filter WHERE toHour(dt) = 9 SETTINGS optimize_use_projections = 0;

-- A part that does not store the column, as it predates `ADD COLUMN ... DEFAULT`, computed the projection from the default.
CREATE TABLE t_absent (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_absent SELECT number FROM numbers(5);
ALTER TABLE t_absent ADD COLUMN dt DateTime('UTC') DEFAULT toDateTime('2026-01-01 00:00:00', 'UTC') + INTERVAL id HOUR;
ALTER TABLE t_absent ADD PROJECTION agg (SELECT toHour(dt) AS h, count() GROUP BY h);
ALTER TABLE t_absent MATERIALIZE PROJECTION agg SETTINGS mutations_sync = 2;
ALTER TABLE t_absent MODIFY COLUMN dt DateTime64(0, 'Asia/Tokyo') SETTINGS mutations_sync = 2;
SELECT groupArray(h) FROM (SELECT toHour(dt) AS h, count() FROM t_absent GROUP BY h ORDER BY h)
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT groupArray(h) FROM (SELECT toHour(dt) AS h, count() FROM t_absent GROUP BY h ORDER BY h) SETTINGS optimize_use_projections = 0;
-- The column is still not written to the part (a compact or packed part reads it for its projections, so it writes it).
SELECT countIf(column = 'dt') FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_absent' AND active;

-- A column that the part does not store, added with a default computed from the column, is computed from the default when
-- read, so the projection that reads it computed from the column too.
CREATE TABLE t_default (id UInt64, dt DateTime('UTC')) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_default SELECT number, toDateTime('2026-01-01 00:00:00', 'UTC') + INTERVAL number HOUR FROM numbers(5);
ALTER TABLE t_default ADD COLUMN h UInt8 DEFAULT toHour(dt);
ALTER TABLE t_default ADD PROJECTION p (SELECT h, count() GROUP BY h);
ALTER TABLE t_default MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;
ALTER TABLE t_default MODIFY COLUMN dt DateTime64(0, 'Asia/Tokyo') SETTINGS mutations_sync = 2;
SELECT groupArray(h) FROM (SELECT h, count() FROM t_default GROUP BY h ORDER BY h)
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT groupArray(h) FROM (SELECT h, count() FROM t_default GROUP BY h ORDER BY h) SETTINGS optimize_use_projections = 0;

-- An `ALIAS` is computed from its expression when read, even if the part still stores the data of the former column.
CREATE TABLE t_alias (id UInt64, dt DateTime('UTC'), h UInt8) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_alias SELECT number, toDateTime('2026-01-01 00:00:00', 'UTC') + INTERVAL number HOUR, toUInt8(number) FROM numbers(5);
ALTER TABLE t_alias ADD COLUMN w UInt8 DEFAULT h;
ALTER TABLE t_alias ADD PROJECTION p (SELECT w, count() GROUP BY w);
ALTER TABLE t_alias MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;
ALTER TABLE t_alias MODIFY COLUMN h UInt8 ALIAS toHour(dt);
ALTER TABLE t_alias MODIFY COLUMN dt DateTime64(0, 'Asia/Tokyo') SETTINGS mutations_sync = 2;
SELECT groupArray(w) FROM (SELECT w, count() FROM t_alias GROUP BY w ORDER BY w)
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT groupArray(w) FROM (SELECT w, count() FROM t_alias GROUP BY w ORDER BY w) SETTINGS optimize_use_projections = 0;

-- A projection that reads a subcolumn of the column, such as `x.dt` of a `Tuple`, computed from the column too.
CREATE TABLE t_subcolumn (id UInt64, x Tuple(dt DateTime('UTC'))) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_subcolumn SELECT number, tuple(toDateTime('2026-01-01 00:00:00', 'UTC') + INTERVAL number HOUR) FROM numbers(5);
ALTER TABLE t_subcolumn ADD PROJECTION p (SELECT toHour(x.dt) AS h, count() GROUP BY h);
ALTER TABLE t_subcolumn MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;
ALTER TABLE t_subcolumn MODIFY COLUMN x Tuple(dt DateTime64(0, 'Asia/Tokyo')) SETTINGS mutations_sync = 2;
SELECT groupArray(h) FROM (SELECT toHour(x.dt) AS h, count() FROM t_subcolumn GROUP BY h ORDER BY h)
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT groupArray(h) FROM (SELECT toHour(x.dt) AS h, count() FROM t_subcolumn GROUP BY h ORDER BY h) SETTINGS optimize_use_projections = 0;

-- A projection that reads a subcolumn of a column computed from its default, such as `x.v`, computed from the column too.
CREATE TABLE t_default_subcolumn (id UInt64, dt DateTime('UTC')) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_default_subcolumn SELECT number, toDateTime('2026-01-01 00:00:00', 'UTC') + INTERVAL number HOUR FROM numbers(5);
ALTER TABLE t_default_subcolumn ADD COLUMN x Tuple(v UInt8) DEFAULT tuple(toHour(dt));
ALTER TABLE t_default_subcolumn ADD PROJECTION p (SELECT toUInt16(x.v) AS h, count() GROUP BY h);
ALTER TABLE t_default_subcolumn MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;
ALTER TABLE t_default_subcolumn MODIFY COLUMN dt DateTime64(0, 'Asia/Tokyo') SETTINGS mutations_sync = 2;
SELECT groupArray(h) FROM (SELECT toUInt16(x.v) AS h, count() FROM t_default_subcolumn GROUP BY h ORDER BY h)
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT groupArray(h) FROM (SELECT toUInt16(x.v) AS h, count() FROM t_default_subcolumn GROUP BY h ORDER BY h) SETTINGS optimize_use_projections = 0;

-- A projection that stores the column as is answers correctly without being rebuilt.
CREATE TABLE t_plain (id UInt64, dt DateTime('UTC'), PROJECTION p (SELECT * ORDER BY dt)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_plain SELECT number, toDateTime('2026-01-01 00:00:00', 'UTC') + INTERVAL number HOUR FROM numbers(5);
ALTER TABLE t_plain MODIFY COLUMN dt DateTime64(0, 'Asia/Tokyo') SETTINGS mutations_sync = 2;
SELECT groupArray(toHour(dt)) FROM (SELECT dt FROM t_plain WHERE dt >= toDateTime64('2026-01-01 10:00:00', 0, 'Asia/Tokyo') ORDER BY dt)
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_aggregate;
DROP TABLE t_expression;
DROP TABLE t_plain;
DROP TABLE t_filter;
DROP TABLE t_absent;
DROP TABLE t_default;
DROP TABLE t_alias;
DROP TABLE t_subcolumn;
DROP TABLE t_default_subcolumn;
