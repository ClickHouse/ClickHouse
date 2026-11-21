-- Time and Time64 values split by a read buffer boundary.

INSERT INTO FUNCTION file(currentDatabase() || '_time_split_read_buffer.jsonl', 'RawBLOB')
SELECT '{"t":"12:34:56"}\n{"t":"-01:02:03"}\n{"t":"123:45:06"}\n{"t":"5:06:07"}\n{"t":"12:34"}\n{"t":"45"}\n{"t":"7"}\n'
SETTINGS engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_time64_split_read_buffer.jsonl', 'RawBLOB')
SELECT '{"t":"12:34:56.789"}\n{"t":"-01:02:03.5"}\n{"t":"123:45:06"}\n{"t":"5:06:07.25"}\n'
SETTINGS engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_time_split_read_buffer.tsv', 'RawBLOB')
SELECT '12:34:56\n-01:02:03\n123:45:06\n5:06:07\n12:34\n'
SETTINGS engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_time_split_read_buffer.csv', 'RawBLOB')
SELECT '"12:34:56"\n"-01:02:03"\n"123:45:06"\n"5:06:07"\n"12:34"\n'
SETTINGS engine_file_truncate_on_insert = 1;

SELECT t FROM file(currentDatabase() || '_time_split_read_buffer.jsonl', 'JSONEachRow', 't Time')
SETTINGS max_read_buffer_size = 8, storage_file_read_method = 'pread', input_format_parallel_parsing = 0;

SELECT t FROM file(currentDatabase() || '_time_split_read_buffer.jsonl', 'JSONEachRow', 't Time')
SETTINGS max_read_buffer_size = 5, storage_file_read_method = 'pread', input_format_parallel_parsing = 0;

SELECT t FROM file(currentDatabase() || '_time_split_read_buffer.jsonl', 'JSONEachRow', 't Time64(0)')
SETTINGS max_read_buffer_size = 8, storage_file_read_method = 'pread', input_format_parallel_parsing = 0;

SELECT t FROM file(currentDatabase() || '_time64_split_read_buffer.jsonl', 'JSONEachRow', 't Time64(3)')
SETTINGS max_read_buffer_size = 8, storage_file_read_method = 'pread', input_format_parallel_parsing = 0;

SELECT t FROM file(currentDatabase() || '_time_split_read_buffer.tsv', 'TSV', 't Time')
SETTINGS max_read_buffer_size = 4, storage_file_read_method = 'pread', input_format_parallel_parsing = 0;

SELECT t FROM file(currentDatabase() || '_time_split_read_buffer.csv', 'CSV', 't Time')
SETTINGS max_read_buffer_size = 4, storage_file_read_method = 'pread', input_format_parallel_parsing = 0;
