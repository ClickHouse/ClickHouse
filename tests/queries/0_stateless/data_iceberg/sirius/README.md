# Iceberg tables from the Sirius test suite

The tables in this directory are copied from the Sirius (GPU-native SQL engine for DuckDB) C++
integration test data, `test/cpp/integration/data/iceberg_*` at commit
https://github.com/sirius-db/sirius/tree/eb9c3a132d3dcd00d8a480852534f3a3b8cf490c/test/cpp/integration/data
(Apache License 2.0, https://github.com/sirius-db/sirius/blob/eb9c3a132d3dcd00d8a480852534f3a3b8cf490c/LICENSE).

All 26 Iceberg tables of the suite are included: 22 hand-forged tables (format versions 1, 2 and 3:
positional and equality deletes, deletion vectors, retired delete entries, schema evolution,
type promotion, name mapping, `deflate` manifests) and 4 tables written by PyIceberg
(`iceberg_conformance/*`).

The metadata stores paths relative to the Sirius repository root (`test/cpp/integration/data/...`),
in the metadata JSON as well as in the manifest lists and manifests. The directory layout
`test/cpp/integration/data/` is kept, so the tables resolve with this directory as the current
directory; the tests `cd` here and use relative paths. The files are not modified.

The expected results in the tests `05730_iceberg_sirius_*` - `05734_iceberg_sirius_*` are taken from
the Sirius tests: `test/cpp/integration/test_gpu_execution_multi_format.cpp` (literal expected rows
of every Iceberg test case) and `test/cpp/integration/data/iceberg_conformance/expectations.json`
(rows recorded from PyIceberg's own scan). The table `iceberg_v3_dv_misbound` (deletion vectors whose
manifest entries carry each other's `content_offset`) is expected to be refused.

Not included:

- The generator scripts (`*.py`), the conformance `README.md` and `expectations.json`, and the
  PyIceberg SQL catalogs `iceberg_conformance/*/catalog.db` (SQLite files not used for reading).
- The non-Iceberg test data of the same directory (`hive_partitioned`, `duckdb`).
