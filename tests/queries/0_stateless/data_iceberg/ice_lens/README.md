# Iceberg tables from ice-lens

The tables in this directory are copied from mmdemirbas/ice-lens at commit
https://github.com/mmdemirbas/ice-lens/tree/578128eb5bee276205b6b7849f9f856aa94a497c/example/iceberg/default
(Apache License 2.0, https://github.com/mmdemirbas/ice-lens/blob/main/LICENSE):
`orcfmt` and `avrofmt`. The files are not modified.

Both are format version 2 tables written by Spark 3.5.5 (Apache Iceberg 1.8.1) with `write.format.default`
= `orc` / `avro` and merge-on-read deletes, so the data files and the position delete file are ORC / Avro.
Each table has 5 rows in 2 data files (ids 1-3 and 4-5) and one position delete (position 1 of the first
data file, `bravo`). The table locations are `/wh/default/<table>`; ClickHouse maps them to the local directory.
