Data generated using Apache Spark 3.4.0 & Delta Lake OSS 2.4.0.

This test resource is used to verify whether the reading from Delta Lake tables with 
multi-part checkpoint files works as expected.

Trino
```
CREATE TABLE multipartcheckpoint(c integer) with (checkpoint_interval = 6);
```

From https://docs.delta.io/latest/optimizations-oss.html

> In Delta Lake, by default each checkpoint is written as a single Parquet file. To to use this feature, 
> set the SQL configuration ``spark.databricks.delta.checkpoint.partSize=<n>``, where n is the limit of 
> number of actions (such as `AddFile`) at which Delta Lake on Apache Spark will start parallelizing the 
> checkpoint and attempt to write a maximum of this many actions per checkpoint file.

Spark
```
SET spark.databricks.delta.checkpoint.partSize=3;
INSERT INTO multipartcheckpoint values 1;
INSERT INTO multipartcheckpoint values 2;
INSERT INTO multipartcheckpoint values 3;
INSERT INTO multipartcheckpoint values 4;
INSERT INTO multipartcheckpoint values 5;
INSERT INTO multipartcheckpoint values 6;
INSERT INTO multipartcheckpoint values 7;
```
