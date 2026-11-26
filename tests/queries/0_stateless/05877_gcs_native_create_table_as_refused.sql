-- Tags: no-fasttest
-- Tag no-fasttest: the `gcs` table function needs the S3 support, which the fast-test build does not have.

-- A table created with `CREATE TABLE ... AS gcs(...)` is stored without the backend choice, so it would use the
-- S3-compatibility backend after it is attached again under the default `use_native_gcs = 0`. Creating one with
-- `use_native_gcs` is refused; without it the definition is accepted as before.

SET use_native_gcs = 1;
CREATE TABLE t_native AS gcs('http://localhost:11111/test/05877.tsv', NOSIGN, 'TSV', 'x UInt8'); -- { serverError BAD_ARGUMENTS, SUPPORT_IS_DISABLED }

SET use_native_gcs = 0;
CREATE TABLE t_compat AS gcs('http://localhost:11111/test/05877.tsv', NOSIGN, 'TSV', 'x UInt8');
SELECT name FROM system.tables WHERE database = currentDatabase() ORDER BY name;
DROP TABLE t_compat;
