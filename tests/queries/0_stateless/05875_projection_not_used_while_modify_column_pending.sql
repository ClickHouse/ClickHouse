-- A `MODIFY COLUMN` that changes the type of a column converts what a part stores when it is read, until the mutation rewrites
-- the part, but a projection of the part keeps what it computed from the column with the old type until the mutation rebuilds it.
-- Such a projection part is not used, neither to serve the query nor to filter the parts, while the mutation is pending, so the
-- reads with and without projections agree. The merges are stopped to keep the mutation pending.

SET optimize_use_projections = 1, optimize_use_projection_filtering = 1;

SELECT '-- an aggregate projection';
CREATE TABLE t_aggregate (id UInt64, dt DateTime('UTC'), PROJECTION p (SELECT toHour(dt) AS h, count() GROUP BY h))
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_aggregate SELECT number, toDateTime('2026-01-01 00:00:00', 'UTC') + INTERVAL number HOUR FROM numbers(5);
SYSTEM STOP MERGES t_aggregate;
ALTER TABLE t_aggregate MODIFY COLUMN dt DateTime64(0, 'Asia/Tokyo') SETTINGS alter_sync = 0;
SELECT groupArray(h) FROM (SELECT toHour(dt) AS h, count() FROM t_aggregate GROUP BY h ORDER BY h);
SELECT groupArray(h) FROM (SELECT toHour(dt) AS h, count() FROM t_aggregate GROUP BY h ORDER BY h) SETTINGS optimize_use_projections = 0;
SELECT toHour(dt) AS h, count() FROM t_aggregate GROUP BY h SETTINGS force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }
-- A part written with the new type uses its projection, while the part that the mutation has not rewritten is read as is.
INSERT INTO t_aggregate SELECT number + 5, toDateTime64('2026-01-01 05:00:00', 0, 'UTC') + INTERVAL number HOUR FROM numbers(5);
SELECT groupArray(h) FROM (SELECT toHour(dt) AS h, count() FROM t_aggregate GROUP BY h ORDER BY h);
SELECT groupArray(h) FROM (SELECT toHour(dt) AS h, count() FROM t_aggregate GROUP BY h ORDER BY h) SETTINGS optimize_use_projections = 0;
SYSTEM START MERGES t_aggregate;
DROP TABLE t_aggregate;

SELECT '-- a projection ordered by an expression of the column';
CREATE TABLE t_expression (id UInt64, dt DateTime('UTC'), PROJECTION p (SELECT id, toHour(dt) AS h ORDER BY h))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_expression SELECT number, toDateTime('2026-01-01 00:00:00', 'UTC') + INTERVAL number HOUR FROM numbers(24);
SYSTEM STOP MERGES t_expression;
ALTER TABLE t_expression MODIFY COLUMN dt DateTime64(0, 'Asia/Tokyo') SETTINGS alter_sync = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_expression WHERE toHour(dt) = 9 ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_expression WHERE toHour(dt) = 9 ORDER BY id) SETTINGS optimize_use_projections = 0;
SYSTEM START MERGES t_expression;
DROP TABLE t_expression;

SELECT '-- a projection that selects its rows by an expression of the column';
CREATE TABLE t_filter (id UInt64, dt DateTime('UTC'), PROJECTION p (SELECT id, dt WHERE toHour(dt) = 9 ORDER BY id))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_filter SELECT number, toDateTime('2026-01-01 00:00:00', 'UTC') + INTERVAL number HOUR FROM numbers(24);
SYSTEM STOP MERGES t_filter;
ALTER TABLE t_filter MODIFY COLUMN dt DateTime64(0, 'Asia/Tokyo') SETTINGS alter_sync = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_filter WHERE toHour(dt) = 9 ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_filter WHERE toHour(dt) = 9 ORDER BY id) SETTINGS optimize_use_projections = 0;
SYSTEM START MERGES t_filter;
DROP TABLE t_filter;

SELECT '-- a projection of a column that the part does not store, computed from its default';
CREATE TABLE t_default (id UInt64, dt DateTime('UTC')) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_default SELECT number, toDateTime('2026-01-01 00:00:00', 'UTC') + INTERVAL number HOUR FROM numbers(5);
ALTER TABLE t_default ADD COLUMN h UInt8 DEFAULT toHour(dt);
ALTER TABLE t_default ADD PROJECTION p (SELECT h, count() GROUP BY h);
ALTER TABLE t_default MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;
SYSTEM STOP MERGES t_default;
ALTER TABLE t_default MODIFY COLUMN dt DateTime64(0, 'Asia/Tokyo') SETTINGS alter_sync = 0;
SELECT groupArray(h) FROM (SELECT h, count() FROM t_default GROUP BY h ORDER BY h);
SELECT groupArray(h) FROM (SELECT h, count() FROM t_default GROUP BY h ORDER BY h) SETTINGS optimize_use_projections = 0;
SYSTEM START MERGES t_default;
DROP TABLE t_default;

SELECT '-- a projection of the column that the part does not store, as it predates `ADD COLUMN ... DEFAULT`';
CREATE TABLE t_absent (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_absent SELECT number FROM numbers(5);
ALTER TABLE t_absent ADD COLUMN dt DateTime('UTC') DEFAULT toDateTime('2026-01-01 00:00:00', 'UTC') + INTERVAL id HOUR;
ALTER TABLE t_absent ADD PROJECTION p (SELECT toHour(dt) AS h, count() GROUP BY h);
ALTER TABLE t_absent MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;
SYSTEM STOP MERGES t_absent;
ALTER TABLE t_absent MODIFY COLUMN dt DateTime64(0, 'Asia/Tokyo') SETTINGS alter_sync = 0;
SELECT groupArray(h) FROM (SELECT toHour(dt) AS h, count() FROM t_absent GROUP BY h ORDER BY h);
SELECT groupArray(h) FROM (SELECT toHour(dt) AS h, count() FROM t_absent GROUP BY h ORDER BY h) SETTINGS optimize_use_projections = 0;
SYSTEM START MERGES t_absent;
DROP TABLE t_absent;

SELECT '-- a projection that stores the column as is is still used';
CREATE TABLE t_plain (id UInt64, dt DateTime('UTC'), PROJECTION p (SELECT id, dt ORDER BY id))
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_plain SELECT number, toDateTime('2026-01-01 00:00:00', 'UTC') + INTERVAL number HOUR FROM numbers(24);
SYSTEM STOP MERGES t_plain;
ALTER TABLE t_plain MODIFY COLUMN dt DateTime64(0, 'Asia/Tokyo') SETTINGS alter_sync = 0;
SELECT groupArray(toHour(dt)) FROM (SELECT dt FROM t_plain WHERE id >= 20 ORDER BY id) SETTINGS force_optimize_projection = 1;
SYSTEM START MERGES t_plain;
DROP TABLE t_plain;
