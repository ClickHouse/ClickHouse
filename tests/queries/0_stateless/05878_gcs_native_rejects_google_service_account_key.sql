-- Tags: no-fasttest
-- Tag no-fasttest: the `gcs` table function needs the S3 support, which the fast-test build does not have.

-- The S3-compatibility path's `google_service_account_key` has no counterpart on the native SQL surface, so it is refused
-- rather than ignored: ignoring it would authenticate with another credential than the one the query supplied.

SELECT * FROM gcs('https://storage.googleapis.com/bucket/data.csv',
    google_service_account_key = '{"type":"service_account","client_email":"a@b.iam.gserviceaccount.com","private_key":"x"}',
    format = 'CSV', structure = 'x UInt8')
SETTINGS use_native_gcs = 1; -- { serverError BAD_ARGUMENTS, SUPPORT_IS_DISABLED }
