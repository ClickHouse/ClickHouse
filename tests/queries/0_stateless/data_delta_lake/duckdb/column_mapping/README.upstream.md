# Column-mapping fixtures

Delta logs, hand-written unless named `spark_*`. Each has a `metaData` action whose `schemaString` carries
`delta.columnMapping.physicalName` and `delta.columnMapping.id` per field, plus a `protocol` action
enabling the `columnMapping` feature (except `mode_without_feature*`, which are about its absence).

**Write fixtures start empty**: `write_field_ids.test` and `invalid_annotations.test` inspect parquet
files they just wrote, so the assertions are about what our writer emits. **Read fixtures carry one
data file**, laid out the way a foreign writer would, so the assertions are about how the reader
resolves it. The table says which is which.

| Fixture | Mode | Schema | Why it exists |
|---|---|---|---|
| `name_mode` | `name` | `id`, `code` | A reader matches physical names, so a file written with logical names reads back as NULL with no error. |
| `id_mode` | `id` | `id`, `code` | A reader matches parquet field ids and must refuse a file that has none, which makes the table unreadable rather than merely wrong. |
| `not_null_name_mode` | `name` | `id NOT NULL`, `code` | Constraints are keyed on the logical name while statistics arrive under the physical one. |
| `nested_name_mode` | `name` | `id`, `s STRUCT(x)` | Write fixture. The struct and its child each carry their own physical name and id, and the file must use both. |
| `partitioned_name_mode` | `name` | `id`, `p` partitioned on `p` | Write fixture. The log must key partition values by `p`'s physical name, `col-bbbb`. Written by `write_partitioned.test`. |
| `name_mode_nested_ids` | `name` | `a`, `l BIGINT[]`, `m MAP` | Read fixture. The list and map carry `delta.columnMapping.nested.ids` as UniForm writes them; a name-mode reader must keep resolving their children by name, not by those ids. Read by `name_mode_nested_ids.test`. |
| `id_mode_logical_names` | `id` | `a`, `b` | Read fixture. The file names its columns `a`/`b` and carries field ids 1/2; only the ids connect file to schema. Read by `read_by_mode.test` and `filename_option.test`. |
| `id_mode_physical_names` | `id` | `a`, `b` | Read fixture. Spark's layout: physical column names and field ids both present. |
| `id_mode_nested_struct` | `id` | `nested_col STRUCT(x, y)` | Read fixture. Struct and children each carry an id and a physical name; the file keeps the logical names. |
| `id_mode_containers` | `id` | `a`, `l BIGINT[]`, `m MAP` | Read fixture. Spark's layout for containers: ids on the columns, none on the list element or map entries, no `nested.ids`. |
| `id_mode_containers_nested_ids` | `id` | `a`, `l BIGINT[]`, `m MAP` | Read fixture. The IcebergCompat layout: `nested.ids` in the log, matching ids on the file's children, logical column names. |
| `id_mode_no_field_ids_physical` | `id` | `a`, `b` | Read fixture, non-conforming: the file has no parquet field ids and names its columns by physical name. Refused under the strict policy, read under the lenient one. |
| `id_mode_no_field_ids_logical` | `id` | `a`, `b` | Read fixture, non-conforming: no field ids, logical column names. What DuckDB itself wrote into id-mode tables before it emitted field ids. |
| `id_mode_no_field_ids_unmatched` | `id` | `a`, `b` | Read fixture, non-conforming: no field ids, and columns named `x`, `y` that match nothing. Refused under both policies. |
| `id_mode_missing_column` | `id` | `a`, `b`, `c` | Read fixture, two files: the first predates `c` and carries ids 1 and 2 only. An id a file lacks reads as NULL for that file. |
| `name_mode_physical_names` | `name` | `a`, `b` | Read fixture. The file names its columns by physical name, as name mode requires. |
| `invalid_id_null` | `name` | as `name_mode`, `id`'s id is `null` | A JSON `null` id must be an error, not a value. Kernel refuses it when loading the schema. |
| `invalid_id_string` | `name` | as `name_mode`, `id`'s id is `"1"` | A numeric-looking string is still not a number. Kernel refuses it too. |
| `invalid_nested_id_range` | `id` | `a`, `l BIGINT[]`, `m MAP`, one nested id above `INT32` max | Read fixture. A nested id that cannot be a parquet field id must be a readable error at bind, not an abort inside the kernel's schema callback. Read by `invalid_annotations.test`. |
| `invalid_physical_name_empty` | `name` | as `name_mode`, `id`'s physical name is `""` | Kernel accepts an empty physical name on read (delta-spark accepts them); it names no column, so DuckDB refuses it. |
| `mode_without_feature` | `id`, ignored | `a`, `b`; reader version 1 | Read fixture. The mode property without the `columnMapping` feature, which the protocol says to ignore; the file names its columns by logical name. Read by `mode_without_feature.test`. |
| `mode_without_feature_v3` | `name`, ignored | `a`, `b`; reader version 3, no `columnMapping` in the feature lists | Read and write fixture, as above. A write must use the logical names too. |
| `name_mode_legacy_protocol` | `name` | `a`, `b`; reader version 2, no feature lists | Read fixture. Reader version 2 implies the feature; the file names its columns by physical name. |
| `partitioned_two_columns_name_mode` | `name` | `p1`, `id`, `p2`, `v` partitioned on `p1`, `p2` | Read fixture. One partition column first, one between data columns, and a NULL partition. Read by `spark_partitioned.test`. |
| `spark_partitioned_name_mode`, `spark_partitioned_id_mode` | `name`, `id` | `id`, `p`, `v` partitioned on `p` | Read fixture, written by Spark. Partition values are keyed by `p`'s physical name, and the kernel places `p` after the physical name of `id`. Read by `spark_partitioned.test`. |
| `spark_renamed_name_mode`, `spark_renamed_id_mode` | `name`, `id` | `id`, `who` (was `s`) | Read and write fixture, written by Spark: one row before `RENAME COLUMN s TO who`, one after. Read by `spark_renamed.test`. |
| `spark_nested_name_mode`, `spark_nested_id_mode` | `name`, `id` | `id`, `s STRUCT(x2, y NOT NULL)`, `l INT[]`, `ls STRUCT(a)[]`, `m MAP(VARCHAR, STRUCT(b))` | Read and write fixture, written by Spark: every nested shape, with `s.x` renamed to `s.x2` between its two inserts. A DuckDB append must match Spark's names and field ids at every level. Appended to by `write_nested.test`. |
| `spark_quoted_name_mode`, `spark_quoted_id_mode` | `name`, `id` | `id`, `s STRUCT("a""b" NOT NULL, "c.d" NOT NULL)` | Read and write fixture, written by Spark. NOT NULL paths with a quote and a dot in a field name must still resolve. Appended to by `write_nested.test`. |
| `reserved_physical_name_name_mode`, `reserved_physical_name_id_mode`, `reserved_physical_name_upper_name_mode` | `name`, `id`, `name` | `s STRUCT(x)`, `x` written as `__duckdb_field_id` (`__DUCKDB_FIELD_ID` in the upper-case table) | Write fixture. That name is the parquet writer's key for a nested column's own id, so the write must be refused. Refused by `write_nested.test`. |
| `unmapped_quoted_not_null` | none | `id`, `s STRUCT("a""b" NOT NULL, c)` | Write fixture. A NOT NULL path with a quote must be enforced without column mapping too. Written by `write_nested.test`. |

The `spark_*` tables are written by Spark through `scripts/data_generator/generate_column_mapping_data.py` and committed.
The others are hand-written rather than generated because the field metadata above is the entire input to the write
path, and because there is no way here to generate with Spark and read back with DuckDB in one run.
Spark-generated equivalents live under `data/generated/simple_table_column_mapped*` but cannot stand
in: they were created with type widening, which also enables `checkConstraints`, `generatedColumns`,
`invariants` and `changeDataFeed`, and the kernel refuses to write a table carrying writer features it
does not support.

For evidence that another engine accepts the result, see `scripts/verify_column_mapping_roundtrip.py`.
