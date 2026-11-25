-- A stored table with a text index is loaded by a short ATTACH without `enable_full_text_index`.

SET enable_full_text_index = 1;
CREATE TABLE t (s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t VALUES ('hello world');
DETACH TABLE t;

SET enable_full_text_index = 0;
ATTACH TABLE t;
SELECT count() FROM t;
DROP TABLE t;
