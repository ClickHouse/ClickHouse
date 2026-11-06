-- `/dev/null` is the only path outside of the directory of the user files that `file` can write into, and
-- the numbered names of the next files of a split insert - `/dev/null.1`, ... - are not allowed. Splitting
-- into anything that is not a regular file is refused before anything is probed, opened or deleted.

SET max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0;

INSERT INTO FUNCTION file('/dev/null', TSV, 'x UInt64') SELECT number FROM numbers(1000);
INSERT INTO FUNCTION file('/dev/null', TSV, 'x UInt64') SELECT number FROM numbers(1000) SETTINGS engine_file_split_on_write_by_size_bytes = 100; -- { serverError BAD_ARGUMENTS }
INSERT INTO FUNCTION file('/dev/null', TSV, 'x UInt64') SELECT number FROM numbers(1000) SETTINGS engine_file_split_on_write_by_size_bytes = 100, engine_file_truncate_on_insert = 1; -- { serverError BAD_ARGUMENTS }
SELECT 'OK';
