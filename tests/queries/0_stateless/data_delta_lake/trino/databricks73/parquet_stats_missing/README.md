Data generated using Databricks 7.3 with statistics disabled to test handling of missing values:

```
DROP TABLE IF EXISTS parquet_stats_missing;

CREATE TABLE parquet_stats_missing (
`i` INT
)
USING delta LOCATION 's3://starburst-alex/ajo/delta_7_3/parquet_stats_missing';

ALTER TABLE parquet_stats_missing SET TBLPROPERTIES ( delta.checkpoint.writeStatsAsStruct = true, delta.checkpoint.writeStatsAsJson = false , delta.dataSkippingNumIndexedCols=0);

INSERT INTO parquet_stats_missing VALUES 1;
INSERT INTO parquet_stats_missing VALUES 2;
INSERT INTO parquet_stats_missing VALUES 3;

INSERT INTO parquet_stats_missing VALUES 4;
INSERT INTO parquet_stats_missing VALUES 5;
INSERT INTO parquet_stats_missing VALUES 6;

INSERT INTO parquet_stats_missing VALUES 7;
INSERT INTO parquet_stats_missing VALUES 8;
INSERT INTO parquet_stats_missing VALUES null;
```
