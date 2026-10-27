-- Changing only the time zone of a `DateTime` column is a metadata change, so the check that forbids changing the type of a
-- column inside a key expression did not run. An expression such as `toHour(dt)` gives other values afterwards, and the
-- primary key and the partitions of the existing parts no longer matched it: pruning dropped their rows.

DROP TABLE IF EXISTS t_sorting_key;
DROP TABLE IF EXISTS t_partition_key;
DROP TABLE IF EXISTS t_nullable_key;
DROP TABLE IF EXISTS t_plain_key;
DROP TABLE IF EXISTS t_not_in_key;

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

-- A column used in the key as is, and a column outside the keys, can still change their time zone.
CREATE TABLE t_plain_key (id UInt64, dt DateTime('UTC')) ENGINE = MergeTree ORDER BY dt;
INSERT INTO t_plain_key VALUES (1, '2026-01-01 02:00:00');
ALTER TABLE t_plain_key MODIFY COLUMN dt DateTime('Asia/Tokyo');
SELECT toHour(dt), count() FROM t_plain_key WHERE dt = toDateTime('2026-01-01 11:00:00', 'Asia/Tokyo') GROUP BY 1;

CREATE TABLE t_not_in_key (id UInt64, dt DateTime('UTC')) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_not_in_key MODIFY COLUMN dt DateTime('Asia/Tokyo');
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_not_in_key' AND name = 'dt';

DROP TABLE t_sorting_key;
DROP TABLE t_partition_key;
DROP TABLE t_nullable_key;
DROP TABLE t_plain_key;
DROP TABLE t_not_in_key;
