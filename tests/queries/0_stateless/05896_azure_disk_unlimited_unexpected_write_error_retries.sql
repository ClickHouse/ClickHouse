-- Tags: no-fasttest, no-distributed-cache
-- Tag no-fasttest: needs Azurite
-- Tag no-distributed-cache: Not supported auth type

-- A dynamic Azure disk runs the access check on creation, which gives that write three times the retries.
-- With an unlimited `max_unexpected_write_error_retries` the multiplication must not overflow.

DROP TABLE IF EXISTS t_azure_unlimited_retries;

CREATE TABLE t_azure_unlimited_retries (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = azure_blob_storage,
    metadata_type = local,
    endpoint = 'http://localhost:10000/devstoreaccount1/cont-05896/data',
    account_name = 'devstoreaccount1',
    account_key = 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==',
    max_unexpected_write_error_retries = 18446744073709551615);

INSERT INTO t_azure_unlimited_retries VALUES (1);
SELECT x FROM t_azure_unlimited_retries;

DROP TABLE t_azure_unlimited_retries;
