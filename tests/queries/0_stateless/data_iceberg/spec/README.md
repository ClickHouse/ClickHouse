# Iceberg specification fixtures

Small fixtures of other Iceberg implementations and of the Iceberg verification suites, all under the Apache License 2.0.
The files are unchanged; only the directory layout is flattened.

- `iceberg-verification/table-spec/{schema,types}/*/cases.json`: the schema and type cases of
  https://github.com/apache/iceberg-verification/tree/12b050929377853252123e345fa0659452da4a73/table-spec
  (https://github.com/apache/iceberg-verification/blob/main/LICENSE). Each case is a schema or a type with a `valid`
  flag. `iceberg-verification/wrap_cases.py` (written for these tests) wraps each case into a minimal table metadata file.
- `iceberg/table-metadata/TableMetadata*.json`: `core/src/test/resources` of
  https://github.com/apache/iceberg/tree/cd4c24c9adb73bf9324d8e4835e73c97c3a1491a/core/src/test/resources
  (https://github.com/apache/iceberg/blob/main/LICENSE). The expected results are from `TestTableMetadata`, and from
  `src/iceberg/test/metadata_serde_test.cc` of
  https://github.com/apache/iceberg-cpp/tree/6f1f04c9ee6458797fc1e6190550fac2665ca081, which has copies of the same files
  (they differ only in formatting), including the `TableMetadataStatisticsFiles.json` and
  `TableMetadataPartitionStatisticsFiles.json` pair.
- `iceberg/deletes/*.bin`: `core/src/test/resources/org/apache/iceberg/deletes` of the same commit: deletion vector blobs
  (`*-position-index.bin`, `TestBitmapPositionDeleteIndex`) and portable 64-bit Roaring bitmaps (`64map*.bin`, the official
  examples of the Roaring format, `TestRoaringPositionBitmap`). `iceberg/wrap_dv.py` (written for these tests) puts them into
  Puffin files. The expected positions are from these Java tests and from `table/dv/roaring_bitmap_test.go` and
  `table/dv/deletion_vector_test.go` of iceberg-go.
- `iceberg/puffin-v1/*.bin`: `core/src/test/resources/org/apache/iceberg/puffin/v1` of the same commit, Puffin files with
  blobs that are not deletion vectors (`TestPuffinReader`).
- `iceberg-go/*.puffin`: from https://github.com/apache/iceberg-go/tree/2e69b8adb8f0db0ecdd7a48cf4280737f4704fe6
  (https://github.com/apache/iceberg-go/blob/main/LICENSE): `single-blob-dv.puffin` and `multi-blob-dv.puffin`
  (`table/dv/testdata/deletes`, written by Apache Iceberg Java, `table/dv/dv_cross_client_test.go`),
  `deletion-vector-v1.puffin` and `compressed-footer-lz4-cli.puffin` (`puffin/testdata`, `puffin/dv_golden_test.go`,
  `puffin/puffin_test.go`).
- `iceberg-testing/bucket/cases.jsonl`: `table-spec/transforms/bucket/cases.jsonl` of
  https://github.com/sungwy/iceberg-testing/tree/1bd33d3b3410d214ab942b704b35ed0cac44d951
  (https://github.com/sungwy/iceberg-testing/blob/poc/LICENSE.txt), the hashes of the `bucket` transform.

Not included: the view metadata files of `apache/iceberg` (`core/src/test/resources/org/apache/iceberg/view`), because
ClickHouse does not read Iceberg views; the copies of the same deletion vector and Puffin files in iceberg-go and iceberg-cpp;
the other surfaces of `sungwy/iceberg-testing` (decimal, fixed and geometry type encodings, delete file examples), which are
not checkable with SQL functions.
