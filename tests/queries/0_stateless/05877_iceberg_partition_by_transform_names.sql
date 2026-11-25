-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
-- Tag no-replicated-database: the replicated DDL worker evaluates the `IcebergLocal` arguments without the session, so it does not see the `TEMPORARY TABLE iceberg_path`

-- The Iceberg transform names `year`, `month`, `day` and `hour` in an Iceberg table's PARTITION BY and ORDER BY
-- are the Iceberg transforms, not the ClickHouse aliases `toYear`, `toMonth`, `toDayOfMonth` and `toHour`.

SET allow_insert_into_iceberg = 1, use_iceberg_metadata_files_cache = 0, iceberg_delete_data_on_drop = 0, async_insert = 0;

CREATE TEMPORARY TABLE iceberg_path AS
WITH if(changed, trimBoth(value), 'user_files/') AS user_files_path,
    concat(
        if(startsWith(user_files_path, '/'), '', (SELECT path FROM system.disks WHERE name = 'default')),
        user_files_path, '/', currentDatabase(), '/05877_iceberg_partition_by_transform_names/') AS base
SELECT
    concat(base, 'part/') AS part,
    concat(base, 'sort/') AS sort,
    concat(base, 'day_of_month/') AS day_of_month,
    concat(base, 'day_of_month_alias/') AS day_of_month_alias
FROM system.server_settings WHERE name = 'user_files_path';

CREATE TABLE t_part (k Int64, a DateTime('UTC'), b Date, c DateTime('UTC'), d DateTime('UTC'))
ENGINE = IcebergLocal((SELECT part FROM iceberg_path), 'Parquet')
PARTITION BY (day(a), year(b), MONTH(c), Hour(d));

-- The Iceberg columns are `DateTime64(6)` without a time zone, so the instants are given in UTC explicitly.
INSERT INTO t_part SELECT 1, x, toDate(x), x, x FROM (SELECT toDateTime('2026-01-01 10:00:00', 'UTC') AS x);
INSERT INTO t_part SELECT 2, x, toDate(x), x, x FROM (SELECT toDateTime('2025-07-04 05:00:00', 'UTC') AS x);

SELECT 'partition', partition, record_count FROM system.iceberg_files
WHERE database = currentDatabase() AND table = 't_part' ORDER BY partition;
SELECT 'transforms', icebergDay(a), icebergYear(b), icebergMonth(c), icebergHour(d) FROM t_part ORDER BY icebergDay(a);

ALTER TABLE t_part ADD COLUMN z Nullable(Int32);
SELECT 'after alter', count(), countIf(z IS NULL) FROM t_part;

CREATE TABLE t_sort (k Int64, a DateTime('UTC'))
ENGINE = IcebergLocal((SELECT sort FROM iceberg_path), 'Parquet')
ORDER BY (day(a));
INSERT INTO t_sort SELECT 1, toDateTime('2026-01-01 10:00:00', 'UTC');
SELECT 'sort', k, toString(a, 'UTC') FROM t_sort;
SELECT 'sort key', sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 't_sort';

-- The ClickHouse functions keep their own meaning: day of month is not an Iceberg transform.
CREATE TABLE t_day_of_month (k Int64, a DateTime('UTC'))
ENGINE = IcebergLocal((SELECT day_of_month FROM iceberg_path), 'Parquet')
PARTITION BY (toDayOfMonth(a)); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_day_of_month_alias (k Int64, a DateTime('UTC'))
ENGINE = IcebergLocal((SELECT day_of_month_alias FROM iceberg_path), 'Parquet')
PARTITION BY (DAYOFMONTH(a)); -- { serverError BAD_ARGUMENTS }

-- Other engines still normalize the aliases.
CREATE TABLE t_mt (b Date) ENGINE = MergeTree PARTITION BY YEAR(b) ORDER BY tuple();
SELECT 'merge tree', partition_key FROM system.tables WHERE database = currentDatabase() AND name = 't_mt';

DROP TABLE t_part;
DROP TABLE t_sort;
DROP TABLE t_mt;
