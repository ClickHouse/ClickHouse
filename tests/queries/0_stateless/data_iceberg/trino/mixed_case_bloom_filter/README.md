Created with Trino using
```
CREATE TABLE mixed_case_bloom_filter (dataColumn INTEGER) WITH (format = 'PARQUET', parquet_bloom_filter_columns = ARRAY['dataColumn'])
```
with some manual edits of the metadata.json file to have the correct mixed-casing in the schema and table properties.
