-- Concurrent shard queries build the text index of one table; the preprocessor of an Array index must not rewrite the table's index expression.
SET enable_full_text_index = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS tab_expr;

CREATE TABLE tab
(
    s String,
    INDEX idx arrayMap(x -> concat(x, 'x'), splitByChar(' ', s))
        TYPE text(tokenizer = 'array', preprocessor = lower(arrayMap(x -> concat(x, 'x'), splitByChar(' ', s))))
)
ENGINE = MergeTree ORDER BY s;

INSERT INTO tab VALUES ('Hello World'), ('FOO bar'), ('');

SELECT count() FROM remote('127.0.0.{1,2,3,4}', currentDatabase(), tab)
WHERE hasAnyTokens(arrayMap(x -> concat(x, 'x'), splitByChar(' ', s)), ['hellox'])
SETTINGS prefer_localhost_replica = 0;

DROP TABLE tab;

-- Analysis folds the constant `if`, so `expr` shows whether building the index rewrote the stored expression.
-- The preprocessor refers to the index expression by its analyzed name.
CREATE TABLE tab_expr
(
    arr Array(String),
    INDEX idx arrayMap(x -> if(1, concat(x, 'x'), x), arr)
        TYPE text(tokenizer = 'array', preprocessor = lower(arrayMap(x -> concat(x, 'x'), arr)))
)
ENGINE = MergeTree ORDER BY tuple();

SELECT expr FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab_expr';
INSERT INTO tab_expr VALUES (['Hello', 'World']);
SELECT count() FROM tab_expr WHERE hasAnyTokens(arrayMap(x -> concat(x, 'x'), arr), ['hellox']);
SELECT expr FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab_expr';

DROP TABLE tab_expr;
