-- A text index preprocessor over an `Array` column is packed into a lambda, and when the preprocessor has an `IN`
-- with a constant set the folded lambda captures that set. Rebuilding the predicate for the parts where the index is
-- not materialized must keep the set instead of turning it into `NULL`, so both kinds of parts agree.

SET enable_full_text_index = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab_05399;
CREATE TABLE tab_05399 (id UInt64, a Array(String)) ENGINE = MergeTree ORDER BY id;

SYSTEM STOP MERGES tab_05399;

INSERT INTO tab_05399 VALUES (1, ['Foo']), (2, ['foo']), (3, ['Bar']);
ALTER TABLE tab_05399 ADD INDEX idx a TYPE text(tokenizer = array, preprocessor = if(a IN ('Foo', 'Bar'), lower(a), a));
INSERT INTO tab_05399 VALUES (4, ['Foo']), (5, ['BAR']), (6, ['bar']);

SELECT 'direct read';
SELECT groupArray(id) FROM (SELECT id FROM tab_05399 WHERE hasAnyTokens(a, ['foo']) ORDER BY id) SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT groupArray(id) FROM (SELECT id FROM tab_05399 WHERE hasAllTokens(a, ['bar']) ORDER BY id) SETTINGS query_plan_direct_read_from_text_index = 1;

SELECT 'without index';
SELECT groupArray(id) FROM (SELECT id FROM tab_05399 WHERE hasAnyTokens(a, ['foo']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_05399 WHERE hasAllTokens(a, ['bar']) ORDER BY id) SETTINGS use_skip_indexes = 0;

DROP TABLE tab_05399;
