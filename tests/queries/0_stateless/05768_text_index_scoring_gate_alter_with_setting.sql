-- Tags: no-replicated-database
-- Tag no-replicated-database: Replicated databases reject an `ALTER` that mixes `ADD INDEX` and `MODIFY SETTING`.

-- A new text index with `scoring = 'bm25'` is checked against the settings the table has after the statement.

DROP TABLE IF EXISTS tab_scoring_gate;

CREATE TABLE tab_scoring_gate (id UInt32, body String, title String)
ENGINE = MergeTree
ORDER BY id;

ALTER TABLE tab_scoring_gate
    ADD INDEX idx_body(body) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', scoring = 'bm25'),
    MODIFY SETTING allow_experimental_text_index_scoring = 1;

ALTER TABLE tab_scoring_gate
    ADD INDEX idx_title(title) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', scoring = 'bm25'),
    MODIFY SETTING allow_experimental_text_index_scoring = 0; -- { serverError SUPPORT_IS_DISABLED }

SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab_scoring_gate' ORDER BY name;

DROP TABLE tab_scoring_gate;
