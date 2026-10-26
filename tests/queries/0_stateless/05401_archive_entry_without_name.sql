-- Tags: no-fasttest
-- Tag no-fasttest: depends on libarchive and S3

-- A 7z archive with one file `hello` and no file names: libarchive returns a NULL pathname for the entry.
INSERT INTO FUNCTION file(currentDatabase() || '_05401.7z', 'RawBLOB', 'x String', 'none') SELECT unhex('377abcaf271c00048375cced0500000000000000190000000000000023f9bd4168656c6c6f0104060001090500070b01000101000c050008000005010000') SETTINGS engine_file_truncate_on_insert = 1;
SELECT 'file member', * FROM file(currentDatabase() || '_05401.7z :: a', 'RawBLOB');
SELECT 'file glob', _file, * FROM file(currentDatabase() || '_05401.7z :: *', 'RawBLOB');

-- The same bytes with a `.tar` extension are detected as 7z.
INSERT INTO FUNCTION file(currentDatabase() || '_05401.tar', 'RawBLOB', 'x String', 'none') SELECT unhex('377abcaf271c00048375cced0500000000000000190000000000000023f9bd4168656c6c6f0104060001090500070b01000101000c050008000005010000') SETTINGS engine_file_truncate_on_insert = 1;
SELECT 'tar member', * FROM file(currentDatabase() || '_05401.tar :: a', 'RawBLOB');

INSERT INTO FUNCTION s3('http://localhost:11111/test/05401_' || currentDatabase() || '.7z', 'test', 'testtest', 'RawBLOB', 'x String', 'none') SELECT unhex('377abcaf271c00048375cced0500000000000000190000000000000023f9bd4168656c6c6f0104060001090500070b01000101000c050008000005010000') SETTINGS s3_truncate_on_insert = 1;
SELECT 's3 member', * FROM s3('http://localhost:11111/test/05401_' || currentDatabase() || '.7z :: a', 'test', 'testtest', 'RawBLOB');
SELECT 's3 glob', _file, * FROM s3('http://localhost:11111/test/05401_' || currentDatabase() || '.7z :: *', 'test', 'testtest', 'RawBLOB');
SELECT 's3Cluster glob', _file, * FROM s3Cluster('test_cluster_two_shards', 'http://localhost:11111/test/05401_' || currentDatabase() || '.7z :: *', 'test', 'testtest', 'RawBLOB');

-- Two entries without names, as written by two `7z a -si` calls into one archive.
INSERT INTO FUNCTION file(currentDatabase() || '_05401_two.7z', 'RawBLOB', 'x String', 'none') SELECT unhex('377abcaf271c0004c2c232965b000000000000002000000000000000397dfff301000468656c6c6f00010004776f726c64000000813307ae31aaa06a181c0798bad9444b5f317633e70c2ba70e00ca63d3cceb32c37efc0937327e300780ac2b5d5db86836bc54b74083708e4cc7c55e5842c08ed61f094400000017061201094900070b01000123030101055d001000000c4e0a01a66640950000') SETTINGS engine_file_truncate_on_insert = 1;
SELECT 'file two', _file, * FROM file(currentDatabase() || '_05401_two.7z :: *', 'RawBLOB') ORDER BY ALL;
INSERT INTO FUNCTION s3('http://localhost:11111/test/05401_' || currentDatabase() || '_two.7z', 'test', 'testtest', 'RawBLOB', 'x String', 'none') SELECT unhex('377abcaf271c0004c2c232965b000000000000002000000000000000397dfff301000468656c6c6f00010004776f726c64000000813307ae31aaa06a181c0798bad9444b5f317633e70c2ba70e00ca63d3cceb32c37efc0937327e300780ac2b5d5db86836bc54b74083708e4cc7c55e5842c08ed61f094400000017061201094900070b01000123030101055d001000000c4e0a01a66640950000') SETTINGS s3_truncate_on_insert = 1;
SELECT 's3 two', _file, * FROM s3('http://localhost:11111/test/05401_' || currentDatabase() || '_two.7z :: *', 'test', 'testtest', 'RawBLOB') ORDER BY ALL;
SELECT 's3Cluster two', _file, * FROM s3Cluster('test_cluster_two_shards', 'http://localhost:11111/test/05401_' || currentDatabase() || '_two.7z :: *', 'test', 'testtest', 'RawBLOB') ORDER BY ALL;

-- Two entries without names with 1 and 3 rows and different columns: the row count and schema caches must not mix them up.
INSERT INTO FUNCTION file(currentDatabase() || '_05401_cnt.7z', 'RawBLOB', 'x String', 'none') SELECT unhex('377abcaf271c00047e8933bf6f0000000000000020000000000000004c5aa15b0100077b2261223a317d0a00e0001700125d003d8888423728aa0bc6a3af2824a2dfdda000000000813307ae31990b3c6c1b31efcd6b8fe328c0475538da6d3d60be4d214459ddc54a7b67883e82101b2f0579866a288db779af7c0ddf144207f8aac5424e15c43bb966971000000017062601094900070b01000123030101055d001000000c4e0a0141ba6d330000') SETTINGS engine_file_truncate_on_insert = 1;
-- A cache entry registered in the same second as the archive modification is ignored.
SELECT sleep(1) FORMAT Null;
SELECT 'file count', count() FROM file(currentDatabase() || '_05401_cnt.7z :: *', 'JSONEachRow', 'a Nullable(Int64), b Nullable(Int64)') SETTINGS optimize_count_from_files = 1, use_cache_for_count_from_files = 1;
SELECT 'file rows', * FROM file(currentDatabase() || '_05401_cnt.7z :: *', 'JSONEachRow', 'a Nullable(Int64), b Nullable(Int64)') ORDER BY ALL;
SELECT 'file count', count() FROM file(currentDatabase() || '_05401_cnt.7z :: *', 'JSONEachRow', 'a Nullable(Int64), b Nullable(Int64)') SETTINGS optimize_count_from_files = 1, use_cache_for_count_from_files = 1;
DESC file(currentDatabase() || '_05401_cnt.7z :: *', 'JSONEachRow') SETTINGS schema_inference_mode = 'union', schema_inference_use_cache_for_file = 1;
