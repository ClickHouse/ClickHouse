-- Tags: no-fasttest
-- Tag no-fasttest: depends on brotli

INSERT INTO FUNCTION file(currentDatabase() || '_05408.csv.br', CSV, 'id UInt64') SELECT number FROM numbers(3) SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05408.csv.br', CSV, 'id UInt64') SELECT number + 10 FROM numbers(3);
INSERT INTO FUNCTION file(currentDatabase() || '_05408.csv.br', CSV, 'id UInt64') SELECT number FROM numbers(0);
INSERT INTO FUNCTION file(currentDatabase() || '_05408.csv.br', CSV, 'id UInt64') SELECT number + 20 FROM numbers(3);
SELECT count(), sum(id) FROM file(currentDatabase() || '_05408.csv.br', CSV, 'id UInt64');
SELECT count(), sum(id) FROM file(currentDatabase() || '_05408.csv.br', CSV, 'id UInt64') SETTINGS max_read_buffer_size = 1;

-- 300000 one-byte empty streams, then a data stream: a reader that recursed once per empty stream would overflow an 8 MiB stack.
INSERT INTO FUNCTION file(currentDatabase() || '_05408.csv.br', RawBLOB, 's String', 'none') SELECT repeat(unhex('06'), 300000);
INSERT INTO FUNCTION file(currentDatabase() || '_05408.csv.br', CSV, 'id UInt64') SELECT number + 30 FROM numbers(3);
SELECT count(), sum(id) FROM file(currentDatabase() || '_05408.csv.br', CSV, 'id UInt64');

INSERT INTO FUNCTION file(currentDatabase() || '_05408_garbage.csv.br', CSV, 'id UInt64') SELECT number FROM numbers(3) SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05408_garbage.csv.br', RawBLOB, 's String', 'none') SELECT 'garbage';
SELECT count(), sum(id) FROM file(currentDatabase() || '_05408_garbage.csv.br', CSV, 'id UInt64'); -- { serverError BROTLI_READ_FAILED }
