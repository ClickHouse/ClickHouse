# Iceberg tables from the parquetry test kit

The tables in this directory are copied from the parquetry (tileverse-io) test kit,
`internal/parquetry-testkit/src/main/resources/iceberg-*` at commit
https://github.com/tileverse-io/parquetry/tree/fba9d4db27f93fd7e21f2dd121bd81f40646347e/internal/parquetry-testkit/src/main/resources
(Apache License 2.0, https://github.com/tileverse-io/parquetry/blob/fba9d4db27f93fd7e21f2dd121bd81f40646347e/LICENSE).

`iceberg-geo-testbed` is vendored by parquetry from https://github.com/jatorre/iceberg-geo-testbed at commit
`59f47a71f641f14994f54e279791e8188edc723f` (Apache License 2.0,
https://github.com/jatorre/iceberg-geo-testbed/blob/59f47a71f641f14994f54e279791e8188edc723f/LICENSE).

All 16 tables are included, the files are not modified:

| corpus | tables | writer |
|---|---|---|
| `iceberg-deletes` | `positional` (v2) | Spark 3.4.1 + Iceberg 1.7.1 |
| `iceberg-deletes` | `equality` (v2), `equality-evolved` (v2), `deletion-vectors` (v3) | Iceberg 1.11.0 Java API |
| `iceberg-scalar-types` | `scalars` (v3) | Iceberg 1.11.0 Java API |
| `iceberg-row-lineage` | `fresh` (v3), `materialized` (v3) | Iceberg 1.11.0 Java API |
| `iceberg-partitioned` | `by_category`, `by_category_omitted` (v2) | PyIceberg 0.11.1 |
| `iceberg-name-mapping` | `migrated` (v2) | PyIceberg static catalog writer of the geo testbed |
| `iceberg-geo-testbed` | `v2_flat_columns`, `v2_bbox_struct`, `v2_geo_convention`, `v3_geometry`, `v3_geometry_lineage`, `v3_minimal` | PyIceberg static catalog writer |

All locations in the metadata are rooted at `file:///iceberg-<corpus>/<table>` (rewritten by parquetry from the
generation paths); they resolve relative to the table directory. The tests run with this directory as the current
directory.

The expected results are taken from the generator documentation in
`internal/parquetry-testkit/fixtures-gen/*/README.md` (row sets reported by Iceberg's own reader as the oracle,
per-row `_row_id` and `_last_updated_sequence_number`), `iceberg-partitioned/README.md`, `iceberg-geo-testbed/README.md`
and the parquetry tests (`integrations/parquetry-iceberg/src/test/java/io/tileverse/parquetry/iceberg/*IT.java`).
The testbed's California probe (`engines/duckdb/run.py` in the testbed) gives 196 rows. Values that upstream does not
state (pseudo-random columns) are checked with aggregates that PyIceberg 0.12.0 gives as well, and against the
Parquet files directly.

The values of `iceberg-scalar-types/scalars` are functions of the row id (1..12, ids 2 and 7 are deleted by equality
deletes keyed on `u` and `dec`); partition `ts` is `2024-01-01 00:00:00` for ids 1-4, `2024-06-15 12:30:45.123456`
for 5-8, `2025-03-10 08:00:00` for 9-12:

| column | type | value |
|---|---|---|
| `tstz` | `timestamptz` | `ts` plus `id` minutes, UTC |
| `ts_ns`, `tstz_ns` | `timestamp_ns`, `timestamptz_ns` | `2024-01-01 00:00:00` plus `id` nanoseconds |
| `t` | `time` | `id:30:15.25` |
| `dec` | `decimal(9, 2)` | `(id - 6) * 1.25` |
| `bigdec` | `decimal(20, 3)` | `123456789012345.678 * id` |
| `u` | `uuid` | most significant 64 bits `id`, least significant `id * 1000003` |
| `fx` | `fixed[4]` | bytes `id, 2*id, 3*id, 4*id` |
| `bin`, `label` | `binary`, `string` | `bin-<id>`, `row-<id>` |
| `d` | `date` | `2024-01-01` plus `id` days |

Notes:

- `iceberg-deletes/positional`: parquetry expects 890 live rows (110 positions deleted). When the table was vendored,
  the `lower_bounds`/`upper_bounds` of `file_path` (field id 2147483546) of the position delete file were not
  rewritten and still name the data file at its generation path (`/Users/.../work/warehouse/db/events/data/...`).
  Apache Iceberg (`DeleteFileIndex`) and PyIceberg use these bounds and do not apply the delete file to the data file,
  so the reference has 1000 rows, as these readers return.
- Not included: the generator scripts (`*.py`), the corpus `README.md` and `LICENSE` files (see the links above).
