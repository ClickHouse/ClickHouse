-- Tags: no-fasttest
-- Tag no-fasttest: the `ParquetMetadata` format needs the Arrow library, which the Fast test build does not include.

-- Arrays whose values span more than 2 Parquet data pages. Without the page index the writer cuts a page inside
-- a row, here after every 1000 values, so a page can hold no row start at all. Both encodings, read whole,
-- with rows skipped by PREWHERE, and one row per block.

SET engine_file_truncate_on_insert = 1;
-- A column served from the columns cache reads no pages, see the page count check.
-- Random settings limits: use_columns_cache=(0, 0)

DROP TABLE IF EXISTS src;
CREATE TABLE src (id UInt64, a Array(UInt32), s Array(Nullable(String)), n Array(Array(UInt32)), t Array(Tuple(x UInt32, y String)))
ENGINE = Memory;
INSERT INTO src SELECT number, arrayMap(i -> toUInt32(number * 100000 + i), range([2, 6000, 3, 4000][number + 1]) AS r),
    arrayMap(i -> if(i % 7 = 0, NULL, toString(number * 100000 + i)), r), arrayMap(i -> range(i % 3), r), arrayMap(i -> (i, toString(i)), r)
FROM numbers(4);

INSERT INTO FUNCTION file(currentDatabase() || '_05829_dict.parquet') SELECT * FROM src
SETTINGS output_format_parquet_write_page_index = 0, output_format_parquet_data_page_size = 100, output_format_parquet_batch_size = 1000,
    output_format_parquet_max_dictionary_size = 1048576;
INSERT INTO FUNCTION file(currentDatabase() || '_05829_plain.parquet') SELECT * FROM src
SETTINGS output_format_parquet_write_page_index = 0, output_format_parquet_data_page_size = 100, output_format_parquet_batch_size = 1000,
    output_format_parquet_max_dictionary_size = 0;

-- The dictionary file must keep every column dictionary-encoded and the PLAIN file none, otherwise one encoding is tested twice.
SELECT arrayMap(c -> has(tupleElement(c, 'encodings'), 'RLE_DICTIONARY'), columns)
FROM file(currentDatabase() || '_05829_dict.parquet', ParquetMetadata);
SELECT arrayMap(c -> has(tupleElement(c, 'encodings'), 'RLE_DICTIONARY'), columns)
FROM file(currentDatabase() || '_05829_plain.parquet', ParquetMetadata);

-- Each check reads both files and compares each with the rows it was written from.
SELECT _file LIKE '%plain%' AS plain, arraySort(groupArray((id, a, s, n, t))) = (SELECT arraySort(groupArray((id, a, s, n, t))) FROM src)
FROM file(currentDatabase() || '_05829_{dict,plain}.parquet') GROUP BY plain ORDER BY plain;
SELECT _file LIKE '%plain%' AS plain, arraySort(groupArray((id, a, s, n, t))) = (SELECT arraySort(groupArray((id, a, s, n, t))) FROM src WHERE id IN (0, 3))
FROM file(currentDatabase() || '_05829_{dict,plain}.parquet') PREWHERE id IN (0, 3) GROUP BY plain ORDER BY plain;
SELECT _file LIKE '%plain%' AS plain, arraySort(groupArray((id, a, s, n, t))) = (SELECT arraySort(groupArray((id, a, s, n, t))) FROM src WHERE id = 2)
FROM file(currentDatabase() || '_05829_{dict,plain}.parquet') PREWHERE id = 2 GROUP BY plain ORDER BY plain;
SELECT _file LIKE '%plain%' AS plain, arraySort(groupArray((id, a, s, n, t))) = (SELECT arraySort(groupArray((id, a, s, n, t))) FROM src WHERE id = 1)
FROM file(currentDatabase() || '_05829_{dict,plain}.parquet') PREWHERE id = 1 GROUP BY plain ORDER BY plain;
SELECT _file LIKE '%plain%' AS plain, arraySort(groupArray((id, a, s, n, t))) = (SELECT arraySort(groupArray((id, a, s, n, t))) FROM src)
FROM file(currentDatabase() || '_05829_{dict,plain}.parquet') GROUP BY plain ORDER BY plain SETTINGS input_format_parquet_max_block_size = 1;
SELECT _file LIKE '%plain%' AS plain, arraySort(groupArray((id, t.x, t.y))) = (SELECT arraySort(groupArray((id, t.x, t.y))) FROM src WHERE id IN (1, 3))
FROM file(currentDatabase() || '_05829_{dict,plain}.parquet') PREWHERE id IN (1, 3) GROUP BY plain ORDER BY plain;

SELECT id, length(a), a[-1], arrayCount(x -> x IS NULL, s), s[-1], length(arrayFlatten(n)), t.y[-1]
FROM file(currentDatabase() || '_05829_dict.parquet') ORDER BY id;

-- More pages than twice the 4 rows: some row's values cross 2 page boundaries.
SELECT a FROM file(currentDatabase() || '_05829_dict.parquet') FORMAT Null SETTINGS log_comment = '05829_pages_dict_a';
SELECT s FROM file(currentDatabase() || '_05829_dict.parquet') FORMAT Null SETTINGS log_comment = '05829_pages_dict_s';
SELECT n FROM file(currentDatabase() || '_05829_dict.parquet') FORMAT Null SETTINGS log_comment = '05829_pages_dict_n';
SELECT t.x FROM file(currentDatabase() || '_05829_dict.parquet') FORMAT Null SETTINGS log_comment = '05829_pages_dict_t_x';
SELECT t.y FROM file(currentDatabase() || '_05829_dict.parquet') FORMAT Null SETTINGS log_comment = '05829_pages_dict_t_y';
SELECT a FROM file(currentDatabase() || '_05829_plain.parquet') FORMAT Null SETTINGS log_comment = '05829_pages_plain_a';
SELECT s FROM file(currentDatabase() || '_05829_plain.parquet') FORMAT Null SETTINGS log_comment = '05829_pages_plain_s';
SELECT n FROM file(currentDatabase() || '_05829_plain.parquet') FORMAT Null SETTINGS log_comment = '05829_pages_plain_n';
SELECT t.x FROM file(currentDatabase() || '_05829_plain.parquet') FORMAT Null SETTINGS log_comment = '05829_pages_plain_t_x';
SELECT t.y FROM file(currentDatabase() || '_05829_plain.parquet') FORMAT Null SETTINGS log_comment = '05829_pages_plain_t_y';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['ParquetReadPages'] > 2 * 4 FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
    AND type = 'QueryFinish' AND log_comment LIKE '05829\_pages\_%'
ORDER BY log_comment;

DROP TABLE src;
