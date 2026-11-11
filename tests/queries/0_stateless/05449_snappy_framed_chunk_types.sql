-- Tags: no-fasttest
-- Tag no-fasttest: the fasttest build is compiled without the Snappy library.

-- `snappy_mode = 'framed'` (documented on DECLARE(SnappyMode, snappy_mode) in
-- src/Core/Settings.cpp) makes file() read the snappy framing format with
-- SnappyFramedReadBuffer. 04201 and 04266 only read back framed streams that
-- ClickHouse itself wrote, which never contain a compressed chunk, a second
-- stream identifier, a skippable chunk or a malformed chunk header. Write such
-- streams byte by byte with unhex() and check what a SELECT over them returns.
-- Framing format: https://github.com/google/snappy/blob/main/framing_format.txt

SET engine_file_truncate_on_insert = 1;

-- An empty input is a valid, empty framed stream: no stream identifier, no rows.
INSERT INTO FUNCTION file(currentDatabase() || '.snp', 'RawBLOB') SELECT unhex('');
SELECT s FROM file(currentDatabase() || '.snp', 'TSV', 's String', 'snappy') ORDER BY s SETTINGS snappy_mode = 'framed';

-- A compressed chunk, a second stream identifier mid-stream, a zero-length
-- compressed chunk and a skippable chunk (type 0x80), all in one stream.
INSERT INTO FUNCTION file(currentDatabase() || '.snp', 'RawBLOB') SELECT unhex('ff060000734e6150705900190000048e2f2313483120636f6d70726573736564206368756e6b0aff060000734e61507059002400002bbf06761e7432206166746572206d69642d73747265616d206964656e7469666965720a00050000d8ea82a2008003000078797a001e00001200ca22185c3320616674657220736b69707061626c65206368756e6b0a');
SELECT s FROM file(currentDatabase() || '.snp', 'TSV', 's String', 'snappy') ORDER BY s SETTINGS snappy_mode = 'framed';

-- Stream identifier chunk whose declared size is not 6.
INSERT INTO FUNCTION file(currentDatabase() || '.snp', 'RawBLOB') SELECT unhex('ff060000734e61507059ff050000');
SELECT s FROM file(currentDatabase() || '.snp', 'TSV', 's String', 'snappy') ORDER BY s SETTINGS snappy_mode = 'framed'; -- { serverError SNAPPY_UNCOMPRESS_FAILED }

-- Stream identifier chunk whose payload is not `sNaPpY`.
INSERT INTO FUNCTION file(currentDatabase() || '.snp', 'RawBLOB') SELECT unhex('ff060000734e61507059ff060000734e61507058');
SELECT s FROM file(currentDatabase() || '.snp', 'TSV', 's String', 'snappy') ORDER BY s SETTINGS snappy_mode = 'framed'; -- { serverError SNAPPY_UNCOMPRESS_FAILED }

-- Compressed chunk shorter than its 4-byte CRC field.
INSERT INTO FUNCTION file(currentDatabase() || '.snp', 'RawBLOB') SELECT unhex('ff060000734e6150705900030000000000');
SELECT s FROM file(currentDatabase() || '.snp', 'TSV', 's String', 'snappy') ORDER BY s SETTINGS snappy_mode = 'framed'; -- { serverError SNAPPY_UNCOMPRESS_FAILED }

-- Compressed chunk whose snappy length prefix is a truncated varint.
INSERT INTO FUNCTION file(currentDatabase() || '.snp', 'RawBLOB') SELECT unhex('ff060000734e615070590005000000000000ff');
SELECT s FROM file(currentDatabase() || '.snp', 'TSV', 's String', 'snappy') ORDER BY s SETTINGS snappy_mode = 'framed'; -- { serverError SNAPPY_UNCOMPRESS_FAILED }

-- A complete compressed chunk that decodes to exactly the 65536-byte limit: one
-- literal 'a' followed by copies with a 2-byte offset of 1, plus a correct CRC-32C.
INSERT INTO FUNCTION file(currentDatabase() || '.snp', 'RawBLOB') SELECT unhex('ff060000734e61507059' || '00090c00' || '0388017d' || '808004' || '0061' || repeat('fe0100', 1023) || 'fa0100');
SELECT length(s), s = repeat('a', 65536) FROM file(currentDatabase() || '.snp', 'TSV', 's String', 'snappy') SETTINGS snappy_mode = 'framed';

-- The same stream with one copy more: a complete body with a correct CRC-32C that
-- decodes to 65537 bytes, one over the limit. Only the uncompressed-length check can
-- reject it; without that check the chunk decodes and the SELECT returns its row.
INSERT INTO FUNCTION file(currentDatabase() || '.snp', 'RawBLOB') SELECT unhex('ff060000734e61507059' || '00090c00' || 'b54914e9' || '818004' || '0061' || repeat('fe0100', 1024));
SELECT s FROM file(currentDatabase() || '.snp', 'TSV', 's String', 'snappy') SETTINGS snappy_mode = 'framed'; -- { serverError SNAPPY_UNCOMPRESS_FAILED }

-- Compressed chunk whose body decodes to fewer bytes than its length prefix.
INSERT INTO FUNCTION file(currentDatabase() || '.snp', 'RawBLOB') SELECT unhex('ff060000734e61507059000a0000000000000a0c41424344');
SELECT s FROM file(currentDatabase() || '.snp', 'TSV', 's String', 'snappy') ORDER BY s SETTINGS snappy_mode = 'framed'; -- { serverError SNAPPY_UNCOMPRESS_FAILED }

-- Compressed chunk that decodes cleanly but carries a wrong CRC-32C.
INSERT INTO FUNCTION file(currentDatabase() || '.snp', 'RawBLOB') SELECT unhex('ff060000734e61507059000900000000000003087a7a0a');
SELECT s FROM file(currentDatabase() || '.snp', 'TSV', 's String', 'snappy') ORDER BY s SETTINGS snappy_mode = 'framed'; -- { serverError SNAPPY_UNCOMPRESS_FAILED }

-- Uncompressed chunk shorter than its 4-byte CRC field.
INSERT INTO FUNCTION file(currentDatabase() || '.snp', 'RawBLOB') SELECT unhex('ff060000734e61507059010200000000');
SELECT s FROM file(currentDatabase() || '.snp', 'TSV', 's String', 'snappy') ORDER BY s SETTINGS snappy_mode = 'framed'; -- { serverError SNAPPY_UNCOMPRESS_FAILED }

-- An uncompressed chunk whose payload is exactly the 65536-byte limit, fully present
-- and correctly checksummed.
INSERT INTO FUNCTION file(currentDatabase() || '.snp', 'RawBLOB') SELECT unhex('ff060000734e61507059' || '01040001' || '0388017d' || repeat('61', 65536));
SELECT length(s), s = repeat('a', 65536) FROM file(currentDatabase() || '.snp', 'TSV', 's String', 'snappy') SETTINGS snappy_mode = 'framed';

-- One byte more: the whole 65537-byte payload is present with a correct CRC-32C, so
-- only the payload-size check can reject it; without it the SELECT returns its row.
INSERT INTO FUNCTION file(currentDatabase() || '.snp', 'RawBLOB') SELECT unhex('ff060000734e61507059' || '01050001' || 'b54914e9' || repeat('61', 65537));
SELECT s FROM file(currentDatabase() || '.snp', 'TSV', 's String', 'snappy') SETTINGS snappy_mode = 'framed'; -- { serverError SNAPPY_UNCOMPRESS_FAILED }

-- Uncompressed chunk with a wrong CRC-32C.
INSERT INTO FUNCTION file(currentDatabase() || '.snp', 'RawBLOB') SELECT unhex('ff060000734e615070590107000000000000616263');
SELECT s FROM file(currentDatabase() || '.snp', 'TSV', 's String', 'snappy') ORDER BY s SETTINGS snappy_mode = 'framed'; -- { serverError SNAPPY_UNCOMPRESS_FAILED }

-- Skippable chunk whose declared payload is missing from the stream.
INSERT INTO FUNCTION file(currentDatabase() || '.snp', 'RawBLOB') SELECT unhex('ff060000734e6150705980100000');
SELECT s FROM file(currentDatabase() || '.snp', 'TSV', 's String', 'snappy') ORDER BY s SETTINGS snappy_mode = 'framed'; -- { serverError SNAPPY_UNCOMPRESS_FAILED }

-- Reserved unskippable chunk type 0x02 must be rejected.
INSERT INTO FUNCTION file(currentDatabase() || '.snp', 'RawBLOB') SELECT unhex('ff060000734e6150705902000000');
SELECT s FROM file(currentDatabase() || '.snp', 'TSV', 's String', 'snappy') ORDER BY s SETTINGS snappy_mode = 'framed'; -- { serverError SNAPPY_UNCOMPRESS_FAILED }
