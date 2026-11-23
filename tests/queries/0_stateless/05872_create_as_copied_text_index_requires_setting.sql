-- `CREATE TABLE ... AS` copying a text index requires `enable_full_text_index`, like a text index written in the query.

SET enable_full_text_index = 1;
CREATE TABLE src (s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple();

SET enable_full_text_index = 0;
CREATE TABLE dst AS src; -- { serverError SUPPORT_IS_DISABLED }

SET enable_full_text_index = 1;
CREATE TABLE dst AS src;
SELECT name, type FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'dst';
