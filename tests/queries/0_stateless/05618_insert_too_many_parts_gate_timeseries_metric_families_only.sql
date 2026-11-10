-- A `TimeSeries` table writes only into the target tables of the columns its INSERT lists: an INSERT of the
-- metric family columns writes only into the "metric families" table. When such an INSERT reaches the
-- `TimeSeries` table through a forwarding storage - a `Distributed` table writing into its local shard, or a
-- `Buffer` writing a block directly - the `Too many parts` checks registered in advance for the hidden write
-- must cover only those target tables: the "samples" and "tags" tables are never touched, so their limits must
-- not reject the query.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_05618_dist;
DROP TABLE IF EXISTS ts_05618_buf;
DROP TABLE IF EXISTS ts_05618;
DROP TABLE IF EXISTS ts_05618_data;
DROP TABLE IF EXISTS ts_05618_tags;
DROP TABLE IF EXISTS ts_05618_metrics;

CREATE TABLE ts_05618_data (id UInt64, timestamp DateTime64(3), value Float64)
    ENGINE = MergeTree ORDER BY (id, timestamp) SETTINGS parts_to_throw_insert = 1;
CREATE TABLE ts_05618_tags (id UInt64, metric_name LowCardinality(String), tags Map(LowCardinality(String), String), min_time DateTime64(3), max_time DateTime64(3))
    ENGINE = MergeTree ORDER BY id SETTINGS parts_to_throw_insert = 1;
CREATE TABLE ts_05618_metrics (metric_family String, type String, unit String, help String)
    ENGINE = MergeTree ORDER BY metric_family;
CREATE TABLE ts_05618 ENGINE = TimeSeries SETTINGS version = 9
    SAMPLES ts_05618_data TAGS ts_05618_tags METRIC FAMILIES ts_05618_metrics;

-- One part in each of the "samples" and "tags" tables: any further write into them is rejected.
INSERT INTO ts_05618_data VALUES (1, 0, 1);
INSERT INTO ts_05618_tags VALUES (1, 'metric', {}, 0, 0);

-- The forwarding storages have only the metric family columns, so their nested INSERTs list only these.
CREATE TABLE ts_05618_dist (metric_family String, type String, unit String, help String)
    ENGINE = Distributed(test_shard_localhost, currentDatabase(), ts_05618);
-- `max_rows = 1`: a block of two rows is written into the destination directly.
CREATE TABLE ts_05618_buf (metric_family String, type String, unit String, help String)
    ENGINE = Buffer(currentDatabase(), ts_05618, 1, 1000000, 1000000, 1, 1, 1000000000, 1000000000);

INSERT INTO ts_05618_dist SETTINGS prefer_localhost_replica = 1, distributed_foreground_insert = 1
    VALUES ('family_1', 'gauge', '', 'help');
INSERT INTO ts_05618_buf VALUES ('family_2', 'gauge', '', 'help'), ('family_3', 'counter', '', 'help');

SELECT metric_family, type FROM ts_05618_metrics ORDER BY metric_family;
SELECT count() FROM ts_05618_data;
SELECT count() FROM ts_05618_tags;

DROP TABLE ts_05618_dist;
DROP TABLE ts_05618_buf;
DROP TABLE ts_05618;
DROP TABLE ts_05618_data;
DROP TABLE ts_05618_tags;
DROP TABLE ts_05618_metrics;
