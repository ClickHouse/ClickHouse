Created via a Databricks notebook:

```
CREATE TABLE IF NOT EXISTS `nested_timestamps` (`col1` ARRAY<STRUCT<`ts`: TIMESTAMP>>) USING DELTA LOCATION 's3://starburst-alex/ajo/nested_timestamps';
INSERT INTO nested_timestamps VALUES (array(struct(TIMESTAMP '2010-02-03 12:11:10')));
```
