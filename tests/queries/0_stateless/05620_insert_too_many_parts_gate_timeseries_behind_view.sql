-- A `TimeSeries` table writes only into the target tables of the columns its INSERT lists. A `MaterializedView`
-- passes the INSERT, with its column list, through to its target table. So when a `Buffer` writes a block of the
-- metric family columns directly into a materialized view whose target is a `TimeSeries` table, the `Too many parts`
-- checks registered in advance for that hidden write must cover only the "metric families" table: the "samples"
-- and "tags" tables are never touched, so their limits must not reject the query.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_05620_buf;
DROP TABLE IF EXISTS ts_05620_mv;
DROP TABLE IF EXISTS ts_05620_src;
DROP TABLE IF EXISTS ts_05620;
DROP TABLE IF EXISTS ts_05620_data;
DROP TABLE IF EXISTS ts_05620_tags;
DROP TABLE IF EXISTS ts_05620_metrics;

CREATE TABLE ts_05620_data (id UInt64, timestamp DateTime64(3), value Float64)
    ENGINE = MergeTree ORDER BY (id, timestamp) SETTINGS parts_to_throw_insert = 1;
CREATE TABLE ts_05620_tags (id UInt64, metric_name LowCardinality(String), tags Map(LowCardinality(String), String), min_time DateTime64(3), max_time DateTime64(3))
    ENGINE = MergeTree ORDER BY id SETTINGS parts_to_throw_insert = 1;
CREATE TABLE ts_05620_metrics (metric_family String, type String, unit String, help String)
    ENGINE = MergeTree ORDER BY metric_family;
CREATE TABLE ts_05620 ENGINE = TimeSeries SETTINGS version = 9
    SAMPLES ts_05620_data TAGS ts_05620_tags METRIC FAMILIES ts_05620_metrics;

-- One part in each of the "samples" and "tags" tables: any further write into them is rejected.
INSERT INTO ts_05620_data VALUES (1, 0, 1);
INSERT INTO ts_05620_tags VALUES (1, 'metric', {}, 0, 0);

CREATE TABLE ts_05620_src (metric_family String, type String, unit String, help String) ENGINE = Null;
CREATE MATERIALIZED VIEW ts_05620_mv TO ts_05620 (metric_family String, type String, unit String, help String)
    AS SELECT metric_family, type, unit, help FROM ts_05620_src;

-- `max_rows = 1`: a block of two rows is written into the destination directly.
CREATE TABLE ts_05620_buf (metric_family String, type String, unit String, help String)
    ENGINE = Buffer(currentDatabase(), ts_05620_mv, 1, 1000000, 1000000, 1, 1, 1000000000, 1000000000);

INSERT INTO ts_05620_buf VALUES ('family_1', 'gauge', '', 'help'), ('family_2', 'counter', '', 'help');

SELECT metric_family, type FROM ts_05620_metrics ORDER BY metric_family;
SELECT count() FROM ts_05620_data;
SELECT count() FROM ts_05620_tags;

DROP TABLE ts_05620_buf;
DROP TABLE ts_05620_mv;
DROP TABLE ts_05620_src;
DROP TABLE ts_05620;
DROP TABLE ts_05620_data;
DROP TABLE ts_05620_tags;
DROP TABLE ts_05620_metrics;
