-- Regression for a latent bug in input_format_max_block_size_bytes (added by PR #79495): the size-based
-- block accounting in IRowInputFormat::read must not read a row that a format skipped without appending.
-- Regexp with format_regexp_skip_unmatched returns true on an unmatched line without appending, so
-- byteSizeAt(size() - 1) on the unchanged (possibly empty) column underflows to SIZE_MAX, which trips the
-- chassert in ColumnString::byteSizeAt on debug/sanitizer builds. The setting is enabled explicitly here
-- because its default is 0 (off) on master.

-- Leading unmatched line: the first row is skipped, leaving the column empty at the accounting point.
SELECT s FROM format(Regexp, 's String', 'ignored\nok: a\nok: b\n')
SETTINGS format_regexp = 'ok: (.*)', format_regexp_skip_unmatched = 1, input_format_max_block_size_bytes = 1;

-- Unmatched line right after a block boundary (a 1-byte limit emits a block after each row, so the
-- skipped line starts a fresh block with an empty column).
SELECT s FROM format(Regexp, 's String', 'ok: a\nignored\nok: b\n')
SETTINGS format_regexp = 'ok: (.*)', format_regexp_skip_unmatched = 1, input_format_max_block_size_bytes = 1;
