-- Tags: no-fasttest, no-msan
-- Tag no-fasttest: `Lance` requires the Rust build.
-- Tag no-msan: `Lance` is disabled in MSan builds, because the `ring` crate does not build with MSan.

-- The `Lance` object store is a separate client, which cannot send custom request headers, so they
-- must be rejected instead of being silently dropped from the requests for the dataset.

SET allow_experimental_lance = 1;

SELECT * FROM lanceS3('http://localhost:11111/test/05457.lance', NOSIGN, headers('x-amz-request-payer' = 'requester')); -- { serverError NOT_IMPLEMENTED }
