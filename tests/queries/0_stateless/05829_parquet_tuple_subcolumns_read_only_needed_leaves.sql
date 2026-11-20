-- Subcolumns of tuples read from Parquet: a JSON path and a Dynamic type under a tuple element, an element of an
-- unnamed tuple, and elements of tuples inside an array. Each read decodes only the leaves it needs, so the
-- 100000-byte `heavy` strings beside them are not decompressed.

SET engine_file_truncate_on_insert = 1;
-- A column served from the columns cache is not decompressed, see the checks at the end.
-- Random settings limits: use_columns_cache=(0, 0)

INSERT INTO FUNCTION file(currentDatabase() || '_05829.parquet')
SELECT
    number AS id,
    tuple(('{"a": ' || toString(number) || ', "b": "s' || toString(number) || '"}')::JSON,
          if(number % 2 = 0, toInt64(number)::Dynamic, toString(number)::Dynamic),
          tuple(number, toString(number)),
          repeat('h', 100000))::Tuple(j JSON, d Dynamic, x Tuple(x UInt64, s String), heavy String) AS t,
    tuple(number * 10, repeat('h', 100000))::Tuple(UInt64, String) AS u,
    [tuple(tuple(number * 100 + 10, toString(number * 100 + 11), repeat('h', 100000)), number * 100 + 20)]
        ::Array(Tuple(x Tuple(x Int64, y String, heavy String), y Int64)) AS a
FROM numbers(3)
SETTINGS output_format_parquet_compression_method = 'zstd';

SELECT id, t.j.a, t.j.b FROM file(currentDatabase() || '_05829.parquet') ORDER BY id SETTINGS log_comment = '05829_read_json_path';
SELECT id, t.d, t.d.Int64, t.d.String FROM file(currentDatabase() || '_05829.parquet') ORDER BY id SETTINGS log_comment = '05829_read_dynamic_type';
SELECT id, t.x.x, t.x FROM file(currentDatabase() || '_05829.parquet') ORDER BY id SETTINGS log_comment = '05829_read_tuple_and_element';
SELECT id, u.`1` FROM file(currentDatabase() || '_05829.parquet') ORDER BY id SETTINGS log_comment = '05829_read_unnamed_element';
SELECT id, u.`1` FROM file(currentDatabase() || '_05829.parquet', Parquet, 'id UInt64, u Tuple(UInt64, String)') ORDER BY id
    SETTINGS log_comment = '05829_read_unnamed_element_hint';
SELECT id, a.x.x, a.x.y, a.y FROM file(currentDatabase() || '_05829.parquet') ORDER BY id SETTINGS log_comment = '05829_read_tuple_in_array';

-- Reading a `heavy` leaf does decompress it.
SELECT sum(length(t.heavy)) FROM file(currentDatabase() || '_05829.parquet') SETTINGS log_comment = '05829_read_heavy_tuple';
SELECT sum(length(u.`2`)) FROM file(currentDatabase() || '_05829.parquet') SETTINGS log_comment = '05829_read_heavy_unnamed';
SELECT sum(length(arrayJoin(a.x.heavy))) FROM file(currentDatabase() || '_05829.parquet') SETTINGS log_comment = '05829_read_heavy_array';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, if(log_comment LIKE '05829\_read\_heavy%', bytes >= 100000, bytes BETWEEN 1 AND 99999)
FROM
(
    SELECT log_comment, ProfileEvents['ParquetDecompressedBytes'] AS bytes FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
        AND type = 'QueryFinish' AND log_comment LIKE '05829\_read\_%'
)
ORDER BY log_comment;
