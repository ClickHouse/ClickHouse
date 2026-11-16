These tables were created to use compression codec values that are invalid.
These would be created from trino 477 onwards due to this bug.
https://github.com/trinodb/trino/issues/28293

I wrote 3 invalid tables like this using `IcebergFlociQueryRunnerMain.main` ...
```
CREATE SCHEMA iceberg.my_schema;
CREATE TABLE iceberg.my_schema.none_avro (a VARCHAR) WITH (format='avro',compression_codec='NONE',location='s3://test-bucket/none_avro/');
CREATE TABLE iceberg.my_schema.none_parquet (a VARCHAR) WITH (format='parquet',compression_codec='NONE',location='s3://test-bucket/none_parquet');
CREATE TABLE iceberg.my_schema.orc_gzip (a VARCHAR) WITH (format='orc',compression_codec='GZIP',location='s3://test-bucket/orc_gzip');

INSERT INTO iceberg.my_schema.none_avro VALUES ('Hello World!');
INSERT INTO iceberg.my_schema.none_parquet VALUES ('Hello World!');
INSERT INTO iceberg.my_schema.orc_gzip VALUES ('Hello World!');
```

I then copied the contents of the three data directories from the minio UI.
