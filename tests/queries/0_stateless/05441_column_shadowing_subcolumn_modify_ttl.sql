-- `MODIFY TTL` may not start reading a name whose meaning differs between the table and a part that misses the column.

-- A subcolumn of a column `a.x` added next to a `JSON` column `a`: the old part would read the path `x.y` of `a`.
DROP TABLE IF EXISTS t_json_subcolumn;
CREATE TABLE t_json_subcolumn (id UInt64, a JSON) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_subcolumn VALUES (1, '{"x":{"y":0}}');
ALTER TABLE t_json_subcolumn ADD COLUMN `a.x` Tuple(y UInt64) DEFAULT tuple(toUInt64(1));
SELECT `a.x`, `a.x.y` FROM t_json_subcolumn;
ALTER TABLE t_json_subcolumn MODIFY TTL addDays(toDate('2000-01-01'), 36500 * toUInt64(`a.x.y`))
SETTINGS materialize_ttl_after_modify = 0, allow_suspicious_ttl_expressions = 1; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT `a.x`, `a.x.y` FROM t_json_subcolumn;
DROP TABLE t_json_subcolumn;

-- A column declared from the start and recorded as missing by `skip_empty_columns_on_insert` reads its default,
-- so a TTL over it is accepted.
DROP TABLE IF EXISTS t_recorded_missing;
CREATE TABLE t_recorded_missing (a Array(UInt64), `a.size0` UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
INSERT INTO t_recorded_missing (a) VALUES ([1]);
SELECT a, `a.size0` FROM t_recorded_missing;
ALTER TABLE t_recorded_missing MODIFY TTL addDays(toDate('2000-01-01'), 36500 * `a.size0`) SETTINGS materialize_ttl_after_modify = 0;
SELECT a, `a.size0` FROM t_recorded_missing;
DROP TABLE t_recorded_missing;
