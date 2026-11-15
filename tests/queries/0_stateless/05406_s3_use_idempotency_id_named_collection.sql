-- Tags: no-fasttest
-- The request setting `use_idempotency_id` is accepted as an override of an S3 named collection.

INSERT INTO FUNCTION s3(s3_conn, filename = 'test_05406_s3_use_idempotency_id.csv', format = CSV, structure = 'a UInt64', use_idempotency_id = 0)
SELECT number FROM numbers(3) SETTINGS s3_truncate_on_insert = 1;

SELECT sum(a) FROM s3(s3_conn, filename = 'test_05406_s3_use_idempotency_id.csv', format = CSV, structure = 'a UInt64', use_idempotency_id = 0);
