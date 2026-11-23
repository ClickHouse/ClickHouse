-- Tags: no-fasttest
-- Tag no-fasttest: Depends on S3

DESC s3('http://localhost:11111/test/a.tsv', 'TSV') SETTINGS schema_inference_use_cache_for_s3 = 1 FORMAT Null;
SELECT DISTINCT source FROM system.schema_inference_cache WHERE storage = 'S3' AND startsWith(source, 'localhost') AND endsWith(source, '/test/a.tsv');
