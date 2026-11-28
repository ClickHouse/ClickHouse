-- Two constants with the same instant and different time zones are different constants: `toHour` differs for them.
-- An aggregate projection must not answer a query whose key differs from the projection key only in such a constant.

DROP TABLE IF EXISTS t_projection_constant_time_zone;

CREATE TABLE t_projection_constant_time_zone
(
    k UInt64,
    PROJECTION p (SELECT toHour(toDateTime('2020-01-01 23:00:00', 'UTC') + k) AS h, count() GROUP BY h)
)
ENGINE = MergeTree ORDER BY k;

INSERT INTO t_projection_constant_time_zone SELECT number * 3600 FROM numbers(3);

SET optimize_use_projections = 1;

SELECT toHour(toDateTime('2020-01-01 23:00:00', 'UTC') + k) AS h, count()
FROM t_projection_constant_time_zone GROUP BY h ORDER BY h
SETTINGS force_optimize_projection = 1;

SELECT toHour(toDateTime('2020-01-02 08:00:00', 'Asia/Tokyo') + k) AS h, count()
FROM t_projection_constant_time_zone GROUP BY h ORDER BY h;

SELECT toHour(toDateTime('2020-01-02 08:00:00', 'Asia/Tokyo') + k) AS h, count()
FROM t_projection_constant_time_zone GROUP BY h ORDER BY h
SETTINGS force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }

DROP TABLE t_projection_constant_time_zone;
