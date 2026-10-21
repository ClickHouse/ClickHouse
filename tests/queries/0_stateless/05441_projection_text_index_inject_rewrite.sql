-- Text search functions over a column with a projection text index use the index's preprocessor,
-- also when skip-index analysis does not produce a direct-read entry for them:
-- the function is in the SELECT list, or `use_skip_indexes = 0`.
-- The results must match those of a table with an equivalent secondary text index.

SET allow_experimental_projection_text_index = 1;
SET enable_full_text_index = 1;

DROP TABLE IF EXISTS tab_projection;
DROP TABLE IF EXISTS tab_secondary;

CREATE TABLE tab_projection
(
    key UInt64,
    val String,
    PROJECTION idx INDEX val TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(val))
)
ENGINE = MergeTree
ORDER BY key;

CREATE TABLE tab_secondary
(
    key UInt64,
    val String,
    INDEX idx (val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(val))
)
ENGINE = MergeTree
ORDER BY key;

INSERT INTO tab_projection VALUES (1, 'foo'), (2, 'BAR'), (3, 'Baz qux');
INSERT INTO tab_secondary VALUES (1, 'foo'), (2, 'BAR'), (3, 'Baz qux');

SELECT '-- SELECT list';
SELECT key, hasAllTokens(val, 'bar'), hasAnyTokens(val, 'BAZ none') FROM tab_projection ORDER BY key;
SELECT key, hasAllTokens(val, 'bar'), hasAnyTokens(val, 'BAZ none') FROM tab_secondary ORDER BY key;

SELECT '-- use_skip_indexes = 0';
SELECT key FROM tab_projection WHERE hasAllTokens(val, 'QUX') ORDER BY key SETTINGS use_skip_indexes = 0;
SELECT key FROM tab_secondary WHERE hasAllTokens(val, 'QUX') ORDER BY key SETTINGS use_skip_indexes = 0;

DROP TABLE tab_projection;
DROP TABLE tab_secondary;
