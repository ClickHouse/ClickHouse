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
