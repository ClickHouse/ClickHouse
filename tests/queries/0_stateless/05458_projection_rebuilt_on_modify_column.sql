-- A `MODIFY COLUMN` that changes the type of a column rebuilt only the projections having the column in their key. A
-- projection that computes something from the column, such as `toHour(dt)` or an aggregate state, kept the values it
-- computed with the old type, and returned them for the queries it served.

DROP TABLE IF EXISTS t_aggregate;
DROP TABLE IF EXISTS t_expression;
DROP TABLE IF EXISTS t_plain;

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

-- A projection that stores the column as is answers correctly without being rebuilt.
CREATE TABLE t_plain (id UInt64, dt DateTime('UTC'), PROJECTION p (SELECT * ORDER BY dt)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_plain SELECT number, toDateTime('2026-01-01 00:00:00', 'UTC') + INTERVAL number HOUR FROM numbers(5);
ALTER TABLE t_plain MODIFY COLUMN dt DateTime64(0, 'Asia/Tokyo') SETTINGS mutations_sync = 2;
SELECT groupArray(toHour(dt)) FROM (SELECT dt FROM t_plain WHERE dt >= toDateTime64('2026-01-01 10:00:00', 0, 'Asia/Tokyo') ORDER BY dt)
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_aggregate;
DROP TABLE t_expression;
DROP TABLE t_plain;
