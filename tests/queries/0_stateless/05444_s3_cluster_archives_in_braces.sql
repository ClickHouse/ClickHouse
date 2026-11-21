-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

-- `s3Cluster` with archives listed in braces skips the metadata request for them, and the archive used to be opened
-- with the placeholder size of 0: zip failed with CANNOT_UNPACK_ARCHIVE and 7z with ARGUMENT_OUT_OF_BOUND. The archives
-- are the ones of 03036_reading_s3_cluster_archives and 04040_reading_s3_7z_archives.

SELECT 'zip, glob inside the archives';
SELECT id, data, _size, _file, _path FROM s3Cluster('test_cluster_two_shards', s3_conn, filename='{03036_archive1,03036_archive2}.zip :: example*.csv') ORDER BY (id, _file, _path);

SELECT 'zip, one file inside the archives';
SELECT id, data, _file, _path FROM s3Cluster('test_cluster_two_shards', s3_conn, filename='{03036_archive1,03036_archive2}.zip :: example2.csv') ORDER BY (id, _file, _path);

SELECT '7z, glob inside the archives';
SELECT id, data, _file, _path FROM s3Cluster('test_cluster_two_shards', s3_conn, filename='{04040_archive1,04040_archive2}.7z :: example*', format='CSV', structure='id UInt32, data String') ORDER BY (id, _file, _path);
