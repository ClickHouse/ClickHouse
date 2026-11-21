-- Changing only the time zone of a `DateTime` column is a metadata change, so the check that forbids changing the type of a
-- column inside a key expression did not run. An expression such as `toHour(dt)` gives other values afterwards, and the
-- primary key and the partitions of the existing parts no longer matched it: pruning dropped their rows.

DROP TABLE IF EXISTS t_sorting_key;
DROP TABLE IF EXISTS t_partition_key;
DROP TABLE IF EXISTS t_nullable_key;
DROP TABLE IF EXISTS t_plain_key;
DROP TABLE IF EXISTS t_not_in_key;
DROP TABLE IF EXISTS t_array_key;
DROP TABLE IF EXISTS t_tuple_key;
DROP TABLE IF EXISTS t_version_key;
DROP TABLE IF EXISTS t_aggregate_key;
DROP TABLE IF EXISTS t_variant_key;
DROP TABLE IF EXISTS t_json_key;
DROP TABLE IF EXISTS t_folded_key;
DROP TABLE IF EXISTS t_folded_src;
DROP TABLE IF EXISTS t_folded_dst;
DROP TABLE IF EXISTS t_lambda_key;
DROP TABLE IF EXISTS t_lambda_capture;
DROP TABLE IF EXISTS t_lambda_src;
DROP TABLE IF EXISTS t_lambda_dst;
DROP TABLE IF EXISTS t_lambda_quoted;
DROP TABLE IF EXISTS t_lambda_quoted_src;
DROP TABLE IF EXISTS t_lambda_quoted_dst;
DROP TABLE IF EXISTS t_lambda_element;
DROP TABLE IF EXISTS t_transfer_src;
DROP TABLE IF EXISTS t_transfer_dst;
DROP TABLE IF EXISTS t_transfer_plain_src;
DROP TABLE IF EXISTS t_transfer_plain_dst;
DROP TABLE IF EXISTS t_plain_subcolumn_key;

CREATE TABLE t_sorting_key (id UInt64, dt DateTime('UTC')) ENGINE = MergeTree ORDER BY toHour(dt);
INSERT INTO t_sorting_key SELECT number, toDateTime('2026-01-01 00:00:00', 'UTC') + INTERVAL number HOUR FROM numbers(24);
ALTER TABLE t_sorting_key MODIFY COLUMN dt DateTime('Asia/Tokyo'); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT count() FROM t_sorting_key WHERE toHour(dt) = 2;

CREATE TABLE t_partition_key (id UInt64, dt DateTime64(3, 'UTC')) ENGINE = MergeTree PARTITION BY toHour(dt) ORDER BY id;
INSERT INTO t_partition_key SELECT number, toDateTime64('2026-01-01 00:00:00', 3, 'UTC') + INTERVAL number HOUR FROM numbers(24);
ALTER TABLE t_partition_key MODIFY COLUMN dt DateTime64(3, 'Asia/Tokyo'); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT count() FROM t_partition_key WHERE toHour(dt) = 2;

CREATE TABLE t_nullable_key (id UInt64, dt Nullable(DateTime('UTC'))) ENGINE = MergeTree PARTITION BY toHour(dt) ORDER BY id
    SETTINGS allow_nullable_key = 1;
INSERT INTO t_nullable_key SELECT number, toDateTime('2026-01-01 00:00:00', 'UTC') + INTERVAL number HOUR FROM numbers(24);
ALTER TABLE t_nullable_key MODIFY COLUMN dt Nullable(DateTime('Asia/Tokyo')); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT count() FROM t_nullable_key WHERE toHour(dt) = 2;

-- So do a `DateTime` inside a composite type and the version column of `ReplacingMergeTree`.
CREATE TABLE t_array_key (dt Array(DateTime('UTC'))) ENGINE = MergeTree ORDER BY toHour(dt[1]) SETTINGS index_granularity = 1;
INSERT INTO t_array_key SELECT [toDateTime('2026-01-01 00:00:00', 'UTC') + INTERVAL number HOUR] FROM numbers(24);
ALTER TABLE t_array_key MODIFY COLUMN dt Array(DateTime('Asia/Tokyo')); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT count() FROM t_array_key WHERE toHour(dt[1]) = 2;

CREATE TABLE t_tuple_key (id UInt64, t Tuple(a UInt8, dt DateTime('UTC'))) ENGINE = MergeTree PARTITION BY toHour(t.dt) ORDER BY id;
ALTER TABLE t_tuple_key MODIFY COLUMN t Tuple(a UInt8, dt DateTime('Asia/Tokyo')); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

CREATE TABLE t_version_key (id UInt64, dt DateTime('UTC')) ENGINE = ReplacingMergeTree(dt) PARTITION BY toHour(dt) ORDER BY id;
INSERT INTO t_version_key SELECT number, toDateTime('2026-01-01 00:00:00', 'UTC') + INTERVAL number HOUR FROM numbers(24);
ALTER TABLE t_version_key MODIFY COLUMN dt DateTime('Asia/Tokyo'); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT count() FROM t_version_key WHERE toHour(dt) = 2;

CREATE TABLE t_aggregate_key (id UInt64, dt AggregateFunction(min, DateTime('UTC'))) ENGINE = MergeTree ORDER BY toHour(finalizeAggregation(dt));
ALTER TABLE t_aggregate_key MODIFY COLUMN dt AggregateFunction(min, DateTime('Asia/Tokyo')); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

CREATE TABLE t_variant_key (id UInt64, dt Variant(DateTime('UTC'), UInt64)) ENGINE = MergeTree ORDER BY toString(dt);
ALTER TABLE t_variant_key MODIFY COLUMN dt Variant(DateTime('Asia/Tokyo'), UInt64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

CREATE TABLE t_json_key (id UInt64, j JSON(a DateTime('UTC'))) ENGINE = MergeTree ORDER BY toString(j);
ALTER TABLE t_json_key MODIFY COLUMN j JSON(a DateTime('Asia/Tokyo')); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- A function that depends only on the type of the column, such as `timezoneOf(dt)`, is folded to a constant in the key, but
-- the key still depends on the time zone.
CREATE TABLE t_folded_key (id UInt64, dt DateTime('UTC')) ENGINE = MergeTree PARTITION BY id + length(timezoneOf(dt)) ORDER BY id;
ALTER TABLE t_folded_key MODIFY COLUMN dt DateTime('Asia/Tokyo'); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- `ATTACH PARTITION FROM`, `REPLACE PARTITION` and `MOVE PARTITION TO TABLE` take the parts with their keys from a table
-- whose column has another time zone.
CREATE TABLE t_transfer_src (id UInt64, dt DateTime('UTC')) ENGINE = MergeTree ORDER BY toHour(dt);
CREATE TABLE t_transfer_dst (id UInt64, dt DateTime('Asia/Tokyo')) ENGINE = MergeTree ORDER BY toHour(dt);
INSERT INTO t_transfer_src SELECT number, toDateTime('2026-01-01 00:00:00', 'UTC') + INTERVAL number HOUR FROM numbers(24);
ALTER TABLE t_transfer_dst ATTACH PARTITION tuple() FROM t_transfer_src; -- { serverError INCOMPATIBLE_COLUMNS }
ALTER TABLE t_transfer_dst REPLACE PARTITION tuple() FROM t_transfer_src; -- { serverError INCOMPATIBLE_COLUMNS }
ALTER TABLE t_transfer_src MOVE PARTITION tuple() TO TABLE t_transfer_dst; -- { serverError INCOMPATIBLE_COLUMNS }
SELECT count() FROM t_transfer_dst;

CREATE TABLE t_folded_src (id UInt64, dt DateTime('UTC')) ENGINE = MergeTree PARTITION BY id + length(timezoneOf(dt)) ORDER BY id;
CREATE TABLE t_folded_dst (id UInt64, dt DateTime('Asia/Tokyo')) ENGINE = MergeTree PARTITION BY id + length(timezoneOf(dt)) ORDER BY id;
INSERT INTO t_folded_src VALUES (0, '2026-01-01 00:00:00');
ALTER TABLE t_folded_dst ATTACH PARTITION 3 FROM t_folded_src; -- { serverError INCOMPATIBLE_COLUMNS }

-- A parameter of a lambda is not the column with the same name: the key below reads only `arr`.
CREATE TABLE t_lambda_key (dt DateTime('UTC'), arr Array(DateTime('UTC'))) ENGINE = MergeTree ORDER BY arrayMap(dt -> toHour(dt), arr);
ALTER TABLE t_lambda_key MODIFY COLUMN dt DateTime('Asia/Tokyo');
ALTER TABLE t_lambda_key MODIFY COLUMN arr Array(DateTime('Asia/Tokyo')); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'lambda parameter', type FROM system.columns WHERE database = currentDatabase() AND table = 't_lambda_key' AND name = 'dt';
CREATE TABLE t_lambda_capture (dt DateTime('UTC'), arr Array(UInt8)) ENGINE = MergeTree ORDER BY arrayMap(x -> x + toHour(dt), arr);
ALTER TABLE t_lambda_capture MODIFY COLUMN dt DateTime('Asia/Tokyo'); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
CREATE TABLE t_lambda_src (dt DateTime('UTC'), arr Array(DateTime('UTC'))) ENGINE = MergeTree ORDER BY arrayMap(dt -> toHour(dt), arr);
CREATE TABLE t_lambda_dst (dt DateTime('Asia/Tokyo'), arr Array(DateTime('UTC'))) ENGINE = MergeTree ORDER BY arrayMap(dt -> toHour(dt), arr);
INSERT INTO t_lambda_src VALUES ('2026-01-01 00:00:00', ['2026-01-01 02:00:00']);
ALTER TABLE t_lambda_dst ATTACH PARTITION tuple() FROM t_lambda_src;
SELECT 'lambda parameter attached', count() FROM t_lambda_dst;

-- A quoted `n.dt` is one identifier, the column of a `Nested` here, also inside a lambda with a parameter `n`, while an unquoted
-- `n.dt` there is an element of the parameter.
CREATE TABLE t_lambda_quoted (id UInt64, n Nested(dt DateTime('UTC'))) ENGINE = MergeTree ORDER BY arraySum(arrayMap(n -> toHour(`n.dt`[1]), [0]));
ALTER TABLE t_lambda_quoted MODIFY COLUMN `n.dt` Array(DateTime('Asia/Tokyo')); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
CREATE TABLE t_lambda_quoted_src (id UInt64, n Nested(dt DateTime('UTC'))) ENGINE = MergeTree ORDER BY arraySum(arrayMap(n -> toHour(`n.dt`[1]), [0]));
CREATE TABLE t_lambda_quoted_dst (id UInt64, n Nested(dt DateTime('Asia/Tokyo'))) ENGINE = MergeTree ORDER BY arraySum(arrayMap(n -> toHour(`n.dt`[1]), [0]));
INSERT INTO t_lambda_quoted_src VALUES (1, ['2026-01-01 02:00:00']);
ALTER TABLE t_lambda_quoted_dst ATTACH PARTITION tuple() FROM t_lambda_quoted_src; -- { serverError INCOMPATIBLE_COLUMNS }
CREATE TABLE t_lambda_element (`n.dt` DateTime('UTC'), arr Array(Tuple(dt DateTime('UTC')))) ENGINE = MergeTree ORDER BY arrayMap(n -> toHour(n.dt), arr);
ALTER TABLE t_lambda_element MODIFY COLUMN `n.dt` DateTime('Asia/Tokyo');
ALTER TABLE t_lambda_element MODIFY COLUMN arr Array(Tuple(dt DateTime('Asia/Tokyo'))); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'lambda element', type FROM system.columns WHERE database = currentDatabase() AND table = 't_lambda_element' AND name = 'n.dt';

-- A column used in the key as is, and a column outside the keys, can still change their time zone.
CREATE TABLE t_plain_key (id UInt64, dt DateTime('UTC')) ENGINE = MergeTree ORDER BY dt;
INSERT INTO t_plain_key VALUES (1, '2026-01-01 02:00:00');
ALTER TABLE t_plain_key MODIFY COLUMN dt DateTime('Asia/Tokyo');
SELECT toHour(dt), count() FROM t_plain_key WHERE dt = toDateTime('2026-01-01 11:00:00', 'Asia/Tokyo') GROUP BY 1;

CREATE TABLE t_plain_subcolumn_key (id UInt64, t Tuple(a UInt8, dt DateTime('UTC'))) ENGINE = MergeTree ORDER BY t.dt;
ALTER TABLE t_plain_subcolumn_key MODIFY COLUMN t Tuple(a UInt8, dt DateTime('Asia/Tokyo'));
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_plain_subcolumn_key' AND name = 't';

CREATE TABLE t_transfer_plain_src (id UInt64, dt DateTime('UTC')) ENGINE = MergeTree ORDER BY dt;
CREATE TABLE t_transfer_plain_dst (id UInt64, dt DateTime('Asia/Tokyo')) ENGINE = MergeTree ORDER BY dt;
INSERT INTO t_transfer_plain_src VALUES (1, '2026-01-01 02:00:00');
ALTER TABLE t_transfer_plain_dst ATTACH PARTITION tuple() FROM t_transfer_plain_src;
SELECT toHour(dt), count() FROM t_transfer_plain_dst WHERE dt = toDateTime('2026-01-01 11:00:00', 'Asia/Tokyo') GROUP BY 1;

CREATE TABLE t_not_in_key (id UInt64, dt DateTime('UTC')) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_not_in_key MODIFY COLUMN dt DateTime('Asia/Tokyo');
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_not_in_key' AND name = 'dt';

DROP TABLE t_sorting_key;
DROP TABLE t_partition_key;
DROP TABLE t_nullable_key;
DROP TABLE t_plain_key;
DROP TABLE t_not_in_key;
DROP TABLE t_array_key;
DROP TABLE t_tuple_key;
DROP TABLE t_version_key;
DROP TABLE t_plain_subcolumn_key;
DROP TABLE t_aggregate_key;
DROP TABLE t_variant_key;
DROP TABLE t_json_key;
DROP TABLE t_transfer_src;
DROP TABLE t_transfer_dst;
DROP TABLE t_transfer_plain_src;
DROP TABLE t_transfer_plain_dst;
DROP TABLE t_folded_key;
DROP TABLE t_folded_src;
DROP TABLE t_folded_dst;
DROP TABLE t_lambda_key;
DROP TABLE t_lambda_capture;
DROP TABLE t_lambda_src;
DROP TABLE t_lambda_dst;
DROP TABLE t_lambda_quoted;
DROP TABLE t_lambda_quoted_src;
DROP TABLE t_lambda_quoted_dst;
DROP TABLE t_lambda_element;
