-- Two `DateTime` keys in the repeated local-time interval of a daylight saving clock change have the same
-- text form, so they must not be pushed into the subquery as `m.key_<text>` subcolumns.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS t_map_datetime_keys;
CREATE TABLE t_map_datetime_keys (m Map(DateTime('Europe/Berlin'), UInt32)) ENGINE = Memory;
INSERT INTO t_map_datetime_keys SELECT map(
    toDateTime(1729989000, 'Europe/Berlin'), toUInt32(11),
    toDateTime(1729992600, 'Europe/Berlin'), toUInt32(22));

SELECT m[toDateTime(1729989000, 'Europe/Berlin')], m[toDateTime(1729992600, 'Europe/Berlin')]
FROM (SELECT m FROM t_map_datetime_keys);

WITH s AS (SELECT m FROM t_map_datetime_keys)
SELECT m[toDateTime(1729989000, 'Europe/Berlin')], m[toDateTime(1729992600, 'Europe/Berlin')] FROM s;

SELECT m[toDateTime(1729989000, 'Europe/Berlin')], m[toDateTime(1729992600, 'Europe/Berlin')]
FROM (SELECT m FROM t_map_datetime_keys UNION ALL SELECT m FROM t_map_datetime_keys);

-- A key outside of the repeated interval is still pushed down.
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT m[toDateTime(1729900000, 'Europe/Berlin')] FROM (SELECT m FROM t_map_datetime_keys))
WHERE explain LIKE '%column_name: m.key_%';
SELECT m[toDateTime(1729989000, 'Europe/Berlin')] FROM (SELECT m FROM t_map_datetime_keys);

DROP TABLE t_map_datetime_keys;
