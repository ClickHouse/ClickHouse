-- Tags: no-fasttest
-- Tag no-fasttest: the `gcs` table function needs the S3 support, which the fast-test build does not have.

-- A table created with `CREATE TABLE ... AS gcs(...)` is stored without the backend choice, so it would use the
-- S3-compatibility backend after it is attached again under the default `use_native_gcs = 0`. Creating one with
-- `use_native_gcs` is refused; without it the definition is accepted as before, and it is attached on that backend.

SET use_native_gcs = 1;
CREATE TABLE t_native AS gcs('http://localhost:11111/test/05877.tsv', NOSIGN, 'TSV', 'x UInt8'); -- { serverError BAD_ARGUMENTS, SUPPORT_IS_DISABLED }

SET use_native_gcs = 0;
CREATE TABLE t_compat AS gcs('http://localhost:11111/test/05877.tsv', NOSIGN, 'TSV', 'x UInt8');
-- HMAC keys are accepted only by the S3-compatibility backend, which a stored definition keeps whatever the setting of
-- the session that attaches it.
CREATE TABLE t_hmac AS gcs('http://localhost:11111/test/05877.tsv', 'hmac-key', 'hmac-secret', 'TSV', 'x UInt8');
DETACH TABLE t_compat;
DETACH TABLE t_hmac;
SET use_native_gcs = 1;
ATTACH TABLE t_compat;
ATTACH TABLE t_hmac;
SELECT name FROM system.tables WHERE database = currentDatabase() ORDER BY name;
DROP TABLE t_compat;
DROP TABLE t_hmac;
