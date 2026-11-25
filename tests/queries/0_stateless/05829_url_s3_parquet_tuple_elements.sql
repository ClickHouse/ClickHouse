-- Tags: no-fasttest, no-parallel-replicas
-- Tag no-fasttest: depends on MinIO.
-- Tag no-parallel-replicas: url() and s3() then read on the replicas, and the checks read this query's ProfileEvents.

-- Tuple elements read through url() and s3() decode only the leaves they need, as through file().

-- Random settings limits: use_columns_cache=(0, 0)
SET s3_truncate_on_insert = 1;

INSERT INTO FUNCTION s3(s3_conn, filename = '05829_url_s3/' || currentDatabase() || '.parquet', format = Parquet)
SELECT
    number AS id,
    tuple(('{"a": ' || toString(number) || '}')::JSON, tuple(number, toString(number)), repeat('h', 100000))
        ::Tuple(j JSON, x Tuple(x UInt64, s String), heavy String) AS t,
    [tuple(tuple(number * 100 + 10, repeat('h', 100000)), number * 100 + 20)]::Array(Tuple(x Tuple(x Int64, heavy String), y Int64)) AS a
FROM numbers(3)
SETTINGS output_format_parquet_compression_method = 'zstd';

SELECT id, t.x.x, t.j.a, a.x.x FROM url('http://localhost:11111/test/05829_url_s3/' || currentDatabase() || '.parquet', Parquet)
ORDER BY id SETTINGS log_comment = '05829_read_url';
SELECT id, t.x.x, t.j.a, a.x.x FROM s3(s3_conn, filename = '05829_url_s3/' || currentDatabase() || '.parquet', format = Parquet)
ORDER BY id SETTINGS log_comment = '05829_read_s3';
SELECT sum(length(t.heavy)) FROM url('http://localhost:11111/test/05829_url_s3/' || currentDatabase() || '.parquet', Parquet)
SETTINGS log_comment = '05829_read_url_heavy';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, if(log_comment LIKE '%heavy', bytes >= 100000, bytes BETWEEN 1 AND 99999)
FROM
(
    SELECT log_comment, ProfileEvents['ParquetDecompressedBytes'] AS bytes FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
        AND type = 'QueryFinish' AND log_comment LIKE '05829\_read\_%'
)
ORDER BY log_comment;
