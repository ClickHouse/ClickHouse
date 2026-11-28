SET optimize_count_from_files = 0;
SET engine_file_truncate_on_insert = 1;

-- The extension selects compression. `RawBLOB` with 'none' reads/writes compressed bytes unchanged.
-- Prefix filenames with the database name to isolate concurrent test runs.

-- zstd: create a complete frame containing CSV rows 0 through 99999.
INSERT INTO FUNCTION file(currentDatabase() || '_05871.zst', CSV, 'id UInt64')
SELECT number FROM numbers(100000);
SELECT 'zstd: complete frame', count(), sum(id)
FROM file(currentDatabase() || '_05871.zst', CSV, 'id UInt64');

-- Keep only two bytes of the frame header.
INSERT INTO FUNCTION file(currentDatabase() || '_05871_header.zst', RawBLOB, 's String', 'none')
SELECT substring(s, 1, 2)
FROM file(currentDatabase() || '_05871.zst', RawBLOB, 's String', 'none');
SELECT count(), sum(id)
FROM file(currentDatabase() || '_05871_header.zst', CSV, 'id UInt64'); -- { serverError ZSTD_DECODER_FAILED }

-- Keep only the first half of the frame.
INSERT INTO FUNCTION file(currentDatabase() || '_05871_half.zst', RawBLOB, 's String', 'none')
SELECT substring(s, 1, intDiv(length(s), 2))
FROM file(currentDatabase() || '_05871.zst', RawBLOB, 's String', 'none');
SELECT count(), sum(id)
FROM file(currentDatabase() || '_05871_half.zst', CSV, 'id UInt64'); -- { serverError ZSTD_DECODER_FAILED }

-- Remove the final byte, even though the payload may already be decodable.
INSERT INTO FUNCTION file(currentDatabase() || '_05871_tail.zst', RawBLOB, 's String', 'none')
SELECT substring(s, 1, length(s) - 1)
FROM file(currentDatabase() || '_05871.zst', RawBLOB, 's String', 'none');
SELECT count(), sum(id)
FROM file(currentDatabase() || '_05871_tail.zst', CSV, 'id UInt64'); -- { serverError ZSTD_DECODER_FAILED }

-- A complete frame must not hide an incomplete following frame.
INSERT INTO FUNCTION file(currentDatabase() || '_05871_concatenated_truncated.zst', RawBLOB, 's String', 'none')
SELECT concat(s, substring(s, 1, intDiv(length(s), 2)))
FROM file(currentDatabase() || '_05871.zst', RawBLOB, 's String', 'none');
SELECT count(), sum(id)
FROM file(currentDatabase() || '_05871_concatenated_truncated.zst', CSV, 'id UInt64'); -- { serverError ZSTD_DECODER_FAILED }

-- An empty file is valid.
INSERT INTO FUNCTION file(currentDatabase() || '_05871_empty.zst', RawBLOB, 's String', 'none')
SELECT '';
SELECT 'zstd: empty input', count(), sum(id)
FROM file(currentDatabase() || '_05871_empty.zst', CSV, 'id UInt64');

-- A valid empty frame produced by the reference compressor.
INSERT INTO FUNCTION file(currentDatabase() || '_05871_empty_frame.zst', RawBLOB, 's String', 'none')
SELECT unhex('28b52ffd240001000099e9d851');
SELECT 'zstd: empty frame', count(), sum(id)
FROM file(currentDatabase() || '_05871_empty_frame.zst', CSV, 'id UInt64');

-- Append three frames: rows 0-2, an empty frame, then rows 3-5.
INSERT INTO FUNCTION file(currentDatabase() || '_05871_empty_frame.zst', CSV, 'id UInt64')
SELECT number FROM numbers(3)
SETTINGS engine_file_truncate_on_insert = 0;
INSERT INTO FUNCTION file(currentDatabase() || '_05871_empty_frame.zst', RawBLOB, 's String', 'none')
SELECT unhex('28b52ffd240001000099e9d851')
SETTINGS engine_file_truncate_on_insert = 0;
INSERT INTO FUNCTION file(currentDatabase() || '_05871_empty_frame.zst', CSV, 'id UInt64')
SELECT number + 3 FROM numbers(3)
SETTINGS engine_file_truncate_on_insert = 0;
SELECT 'zstd: concatenated frames', count(), sum(id)
FROM file(currentDatabase() || '_05871_empty_frame.zst', CSV, 'id UInt64')
SETTINGS max_read_buffer_size = 1;

-- Read more than one decompression buffer of highly compressible output.
INSERT INTO FUNCTION file(currentDatabase() || '_05871_large.zst', RawBLOB, 's String')
SELECT repeat('xxxx', 800000);
SELECT 'zstd: large output', length(s), s = repeat('xxxx', 800000)
FROM file(currentDatabase() || '_05871_large.zst', RawBLOB, 's String');

-- lz4: create a complete frame containing CSV rows 0 through 99999.
INSERT INTO FUNCTION file(currentDatabase() || '_05871.lz4', CSV, 'id UInt64')
SELECT number FROM numbers(100000);
SELECT 'lz4: complete frame', count(), sum(id)
FROM file(currentDatabase() || '_05871.lz4', CSV, 'id UInt64');

-- Keep only two bytes of the frame header.
INSERT INTO FUNCTION file(currentDatabase() || '_05871_header.lz4', RawBLOB, 's String', 'none')
SELECT substring(s, 1, 2)
FROM file(currentDatabase() || '_05871.lz4', RawBLOB, 's String', 'none');
SELECT count(), sum(id)
FROM file(currentDatabase() || '_05871_header.lz4', CSV, 'id UInt64'); -- { serverError LZ4_DECODER_FAILED }

-- Keep only the first half of the frame.
INSERT INTO FUNCTION file(currentDatabase() || '_05871_half.lz4', RawBLOB, 's String', 'none')
SELECT substring(s, 1, intDiv(length(s), 2))
FROM file(currentDatabase() || '_05871.lz4', RawBLOB, 's String', 'none');
SELECT count(), sum(id)
FROM file(currentDatabase() || '_05871_half.lz4', CSV, 'id UInt64'); -- { serverError LZ4_DECODER_FAILED }

-- Remove the final byte, even though the payload may already be decodable.
INSERT INTO FUNCTION file(currentDatabase() || '_05871_tail.lz4', RawBLOB, 's String', 'none')
SELECT substring(s, 1, length(s) - 1)
FROM file(currentDatabase() || '_05871.lz4', RawBLOB, 's String', 'none');
SELECT count(), sum(id)
FROM file(currentDatabase() || '_05871_tail.lz4', CSV, 'id UInt64'); -- { serverError LZ4_DECODER_FAILED }

-- A complete frame must not hide an incomplete following frame.
INSERT INTO FUNCTION file(currentDatabase() || '_05871_concatenated_truncated.lz4', RawBLOB, 's String', 'none')
SELECT concat(s, substring(s, 1, intDiv(length(s), 2)))
FROM file(currentDatabase() || '_05871.lz4', RawBLOB, 's String', 'none');
SELECT count(), sum(id)
FROM file(currentDatabase() || '_05871_concatenated_truncated.lz4', CSV, 'id UInt64'); -- { serverError LZ4_DECODER_FAILED }

-- An empty file is valid.
INSERT INTO FUNCTION file(currentDatabase() || '_05871_empty.lz4', RawBLOB, 's String', 'none')
SELECT '';
SELECT 'lz4: empty input', count(), sum(id)
FROM file(currentDatabase() || '_05871_empty.lz4', CSV, 'id UInt64');

-- A valid empty frame produced by the reference compressor.
INSERT INTO FUNCTION file(currentDatabase() || '_05871_empty_frame.lz4', RawBLOB, 's String', 'none')
SELECT unhex('04224d186440a700000000055dcc02');
SELECT 'lz4: empty frame', count(), sum(id)
FROM file(currentDatabase() || '_05871_empty_frame.lz4', CSV, 'id UInt64');

-- Append three frames: rows 0-2, an empty frame, then rows 3-5.
INSERT INTO FUNCTION file(currentDatabase() || '_05871_empty_frame.lz4', CSV, 'id UInt64')
SELECT number FROM numbers(3)
SETTINGS engine_file_truncate_on_insert = 0;
INSERT INTO FUNCTION file(currentDatabase() || '_05871_empty_frame.lz4', RawBLOB, 's String', 'none')
SELECT unhex('04224d186440a700000000055dcc02')
SETTINGS engine_file_truncate_on_insert = 0;
INSERT INTO FUNCTION file(currentDatabase() || '_05871_empty_frame.lz4', CSV, 'id UInt64')
SELECT number + 3 FROM numbers(3)
SETTINGS engine_file_truncate_on_insert = 0;
SELECT 'lz4: concatenated frames', count(), sum(id)
FROM file(currentDatabase() || '_05871_empty_frame.lz4', CSV, 'id UInt64')
SETTINGS max_read_buffer_size = 1;

-- Read more than one decompression buffer of highly compressible output.
INSERT INTO FUNCTION file(currentDatabase() || '_05871_large.lz4', RawBLOB, 's String')
SELECT repeat('xxxx', 800000);
SELECT 'lz4: large output', length(s), s = repeat('xxxx', 800000)
FROM file(currentDatabase() || '_05871_large.lz4', RawBLOB, 's String');
