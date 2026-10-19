-- Tags: no-fasttest
-- Tag no-fasttest: depends on brotli

INSERT INTO FUNCTION file(currentDatabase() || '_05408.csv.br', CSV, 'id UInt64') SELECT number FROM numbers(3) SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05408.csv.br', CSV, 'id UInt64') SELECT number + 10 FROM numbers(3);
INSERT INTO FUNCTION file(currentDatabase() || '_05408.csv.br', CSV, 'id UInt64') SELECT number FROM numbers(0);
INSERT INTO FUNCTION file(currentDatabase() || '_05408.csv.br', CSV, 'id UInt64') SELECT number + 20 FROM numbers(3);
SELECT count(), sum(id) FROM file(currentDatabase() || '_05408.csv.br', CSV, 'id UInt64');
SELECT count(), sum(id) FROM file(currentDatabase() || '_05408.csv.br', CSV, 'id UInt64') SETTINGS max_read_buffer_size = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_05408.csv.br', RawBLOB, 's String', 'none') SELECT 'garbage';
SELECT count(), sum(id) FROM file(currentDatabase() || '_05408.csv.br', CSV, 'id UInt64'); -- { serverError BROTLI_READ_FAILED }
