-- Tags: no-fasttest, no-msan
-- Tag no-fasttest: delta-kernel pulls in extra dependencies.
-- Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

-- A Delta Lake table derives the columns written to its data files from its own metadata (an
-- existing partitioned table keeps its partition columns out of the data files), so the Parquet
-- `field_id` settings cannot be validated against the definition and are rejected in it.

DROP TABLE IF EXISTS t_delta_field_ids;

CREATE TABLE t_delta_field_ids (p Int32, x Int32)
ENGINE = DeltaLakeLocal('/nonexistent/05397_parquet_field_ids_delta_lake_definition/', 'Parquet')
SETTINGS output_format_parquet_column_field_ids = {'p': '1', 'x': '2'}; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_delta_field_ids (p Int32, x Int32)
ENGINE = DeltaLakeLocal('/nonexistent/05397_parquet_field_ids_delta_lake_definition/', 'Parquet')
SETTINGS output_format_parquet_column_field_ids = {'x': '2'}; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_delta_field_ids (p Int32, x Int32)
ENGINE = DeltaLakeLocal('/nonexistent/05397_parquet_field_ids_delta_lake_definition/', 'Parquet')
SETTINGS output_format_parquet_auto_assign_field_ids = 1; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_delta_field_ids';
