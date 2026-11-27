-- Tags: no-fasttest
-- Tag no-fasttest: data lake table functions need Avro, and `s3_disk` comes from CI fixtures

-- Only Iceberg and Delta Lake table functions support the `disk` setting. Paimon and Hudi used to
-- get a Delta Lake configuration over the disk, or a null configuration (a segmentation fault)
-- in builds without delta-kernel-rs.

SELECT * FROM paimonLocal('05630_paimon/', SETTINGS disk = 'default'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM paimon('http://localhost:11111/test/05630_paimon/', SETTINGS disk = 's3_disk'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM paimonS3('http://localhost:11111/test/05630_paimon/', SETTINGS disk = 's3_disk'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM hudi('http://localhost:11111/test/05630_hudi/', SETTINGS disk = 's3_disk'); -- { serverError BAD_ARGUMENTS }
