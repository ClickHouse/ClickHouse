-- The `regexpMasked` tokenizer: tokens of a base tokenizer, with every match of a mask (an IP address, a number, ...) merged into one token.

SET enable_full_text_index = 1;
SET use_skip_indexes = 1;
SET explain_query_plan_default = 'legacy';
SET use_query_condition_cache = 0;

SELECT 'tokens';

SELECT tokens('took 1.5GB from 10.0.0.1:9000', 'regexpMasked');
SELECT tokens('took 1.5GB from 10.0.0.1:9000', 'regexpMasked(masks = [])');
SELECT tokens('took 1.5GB from 10.0.0.1:9000', 'regexpMasked(masks = [''ip''])');
SELECT tokens('at 2026-11-05 10:15:30.123+00:00 GET http://host:8123/?q=1 id 97a133f8-4d41-4e4f-b869-db7c48b6da4c', 'regexpMasked');
SELECT tokens('[04/Nov/2026:10:15:30 +0000] read /var/lib/clickhouse/x.bin, 0x7f3a, deadbeef, read/write x=-5', 'regexpMasked');
SELECT tokens('user=alice session=sess_9f2c1a7b3d4e5f60 took 1.5GB', 'regexpMasked(custom_masks = [''sess_[0-9a-f]{16}''])');
SELECT tokens('a, 10.0.0.1, b c', 'regexpMasked(splitByString(['', '', '' '']))');
SELECT tokens('', 'regexpMasked');
-- A long needle whose every token starts a would-be path: tokenizing it must stay linear.
SELECT hasAllTokens('a', repeat('a.', 100000), 'regexpMasked'), hasAnyTokens('b', repeat('a.', 100000) || '/b/', 'regexpMasked');
-- Separators inside a run of hexadecimal digits: every token starts a `hex` candidate that is rejected for lack of a digit.
SELECT length(tokens(repeat('bbbbbbbba', 32000), 'regexpMasked(splitByString([''a'']))'));

SELECT 'Rejected definitions';

SELECT tokens('a', 'regexpMasked(masks = [''foo''])'); -- { serverError BAD_ARGUMENTS }
SELECT tokens('a', 'regexpMasked(ngrams)'); -- { serverError BAD_ARGUMENTS }
SELECT tokens('a', 'regexpMasked(custom_masks = [''a*''])'); -- { serverError BAD_ARGUMENTS }
SELECT tokens('a', 'regexpMasked(custom_masks = [''^a''])'); -- { serverError BAD_ARGUMENTS }
SELECT tokens('a', 'regexpMasked(custom_masks = [''(''])'); -- { serverError BAD_ARGUMENTS }
SELECT tokens('a', 'regexpMasked(foo = [''ip''])'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab_bad (s String, INDEX idx s TYPE text(tokenizer = regexpMasked(masks = ['foo']))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab_bad (s String, INDEX idx s TYPE text(tokenizer = regexpMasked(splitByRegexp('a')))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab_bad (s String, INDEX idx s TYPE text(tokenizer = regexpMasked(custom_masks = ['\\bx']))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS tab_ref;

CREATE TABLE tab_ref (id UInt64, message String) ENGINE = MergeTree ORDER BY id;

INSERT INTO tab_ref VALUES
    (1, 'Connection to 10.0.0.1:9000 timed out after 30000 ms'),
    (2, 'Connection to 10.0.0.2:9000 refused'),
    (3, 'user=alice session=sess_9f2c1a7b3d4e5f60 took 1.5GB'),
    (4, 'user=bob session=sess_0000000000000001 took 20ms'),
    (5, 'read /var/lib/clickhouse/store/abc.bin, 0x7f3a'),
    (6, 'GET http://host:8123/?q=1 HTTP/1.1 api/v1/users'),
    (7, 'users list'),
    (8, 'at 2026-11-05 10:15:30 merged 10 parts'),
    (9, 'Took 1.5gb From 10.0.0.1');

SELECT 'Index with all built-in masks';

CREATE TABLE tab
(
    id UInt64,
    message String,
    INDEX idx message TYPE text(tokenizer = regexpMasked) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi';

INSERT INTO tab SELECT * FROM tab_ref;

SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, '10.0.0.1') SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['10.0.0.1', '9000']) SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['10.0.0.2', '1.5GB']) SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, 'From 10.0.0.1') SETTINGS force_data_skipping_indices = 'idx';
-- A path is one token, so `users` matches only the row where it is a word.
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, 'users') SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, 'api/v1/users') SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, '2026-11-05 10:15:30') SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, '10.0.0.1', 'regexpMasked') SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, '10.0.0.1', 'regexpMasked(splitByNonAlpha, masks = [''url'', ''path'', ''number'', ''hex'', ''uuid'', ''ip'', ''timestamp''])') SETTINGS force_data_skipping_indices = 'idx';
-- A different tokenizer: the index is not used, the result is still correct.
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, '10.0.0.1', 'regexpMasked(masks = [])');

SELECT 'The index prunes granules';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE hasAllTokens(message, '10.0.0.1')
    SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

SELECT 'Index results equal a brute force over tokens';
SELECT (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAllTokens(message, ['10.0.0.1']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'))
    = (SELECT groupArray(id) FROM (SELECT id FROM tab_ref WHERE has(tokens(message, 'regexpMasked'), '10.0.0.1') ORDER BY id));
SELECT (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAllTokens(message, ['10.0.0.2']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'))
    = (SELECT groupArray(id) FROM (SELECT id FROM tab_ref WHERE has(tokens(message, 'regexpMasked'), '10.0.0.2') ORDER BY id));
SELECT (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAllTokens(message, ['9000']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'))
    = (SELECT groupArray(id) FROM (SELECT id FROM tab_ref WHERE has(tokens(message, 'regexpMasked'), '9000') ORDER BY id));
SELECT (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAllTokens(message, ['30000']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'))
    = (SELECT groupArray(id) FROM (SELECT id FROM tab_ref WHERE has(tokens(message, 'regexpMasked'), '30000') ORDER BY id));
SELECT (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAllTokens(message, ['sess']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'))
    = (SELECT groupArray(id) FROM (SELECT id FROM tab_ref WHERE has(tokens(message, 'regexpMasked'), 'sess') ORDER BY id));
SELECT (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAllTokens(message, ['9f2c1a7b3d4e5f60']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'))
    = (SELECT groupArray(id) FROM (SELECT id FROM tab_ref WHERE has(tokens(message, 'regexpMasked'), '9f2c1a7b3d4e5f60') ORDER BY id));
SELECT (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAllTokens(message, ['0000000000000001']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'))
    = (SELECT groupArray(id) FROM (SELECT id FROM tab_ref WHERE has(tokens(message, 'regexpMasked'), '0000000000000001') ORDER BY id));
SELECT (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAllTokens(message, ['1.5GB']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'))
    = (SELECT groupArray(id) FROM (SELECT id FROM tab_ref WHERE has(tokens(message, 'regexpMasked'), '1.5GB') ORDER BY id));
SELECT (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAllTokens(message, ['20ms']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'))
    = (SELECT groupArray(id) FROM (SELECT id FROM tab_ref WHERE has(tokens(message, 'regexpMasked'), '20ms') ORDER BY id));
SELECT (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAllTokens(message, ['var/lib/clickhouse/store/abc.bin']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'))
    = (SELECT groupArray(id) FROM (SELECT id FROM tab_ref WHERE has(tokens(message, 'regexpMasked'), 'var/lib/clickhouse/store/abc.bin') ORDER BY id));
SELECT (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAllTokens(message, ['0x7f3a']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'))
    = (SELECT groupArray(id) FROM (SELECT id FROM tab_ref WHERE has(tokens(message, 'regexpMasked'), '0x7f3a') ORDER BY id));
SELECT (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAllTokens(message, ['http://host:8123/?q=1']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'))
    = (SELECT groupArray(id) FROM (SELECT id FROM tab_ref WHERE has(tokens(message, 'regexpMasked'), 'http://host:8123/?q=1') ORDER BY id));
SELECT (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAllTokens(message, ['1.1']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'))
    = (SELECT groupArray(id) FROM (SELECT id FROM tab_ref WHERE has(tokens(message, 'regexpMasked'), '1.1') ORDER BY id));
SELECT (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAllTokens(message, ['users']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'))
    = (SELECT groupArray(id) FROM (SELECT id FROM tab_ref WHERE has(tokens(message, 'regexpMasked'), 'users') ORDER BY id));
SELECT (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAllTokens(message, ['api/v1/users']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'))
    = (SELECT groupArray(id) FROM (SELECT id FROM tab_ref WHERE has(tokens(message, 'regexpMasked'), 'api/v1/users') ORDER BY id));
SELECT (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAllTokens(message, ['2026-11-05 10:15:30']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'))
    = (SELECT groupArray(id) FROM (SELECT id FROM tab_ref WHERE has(tokens(message, 'regexpMasked'), '2026-11-05 10:15:30') ORDER BY id));
SELECT (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAllTokens(message, ['10']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'))
    = (SELECT groupArray(id) FROM (SELECT id FROM tab_ref WHERE has(tokens(message, 'regexpMasked'), '10') ORDER BY id));
SELECT (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAllTokens(message, ['nothing']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'))
    = (SELECT groupArray(id) FROM (SELECT id FROM tab_ref WHERE has(tokens(message, 'regexpMasked'), 'nothing') ORDER BY id));

DETACH TABLE tab;
ATTACH TABLE tab;
SELECT 'After ATTACH';
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, '10.0.0.1') SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE tab;

SELECT 'Selected and custom masks';

CREATE TABLE tab
(
    id UInt64,
    message String,
    INDEX idx message TYPE text(tokenizer = regexpMasked(splitByNonAlpha, masks = ['ip'], custom_masks = ['sess_[0-9a-f]{16}', 'took \\d+']))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab SELECT * FROM tab_ref;

SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, 'sess_9f2c1a7b3d4e5f60') SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['sess']) SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['took 1', 'took 20ms']) SETTINGS force_data_skipping_indices = 'idx';
-- Without the `number` mask, the digits are base tokens.
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['30000']) SETTINGS force_data_skipping_indices = 'idx';
-- The same custom masks in a different order are the same tokenizer.
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, '10.0.0.1', 'regexpMasked(masks = [''ip''], custom_masks = [''took \\\\d+'', ''sess_[0-9a-f]{16}'', ''took \\\\d+''])') SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE tab;

SELECT 'splitByString base';

CREATE TABLE tab
(
    id UInt64,
    message String,
    INDEX idx message TYPE text(tokenizer = regexpMasked(splitByString([' ', ', ']), masks = ['ip', 'number']))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab SELECT * FROM tab_ref;

SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['user=alice']) SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['10.0.0.1:9000']) SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['/var/lib/clickhouse/store/abc.bin']) SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE tab;

SELECT 'Preprocessor';

CREATE TABLE tab
(
    id UInt64,
    message String,
    INDEX idx message TYPE text(tokenizer = regexpMasked, preprocessor = lower(message))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab SELECT * FROM tab_ref;

SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['took', '1.5gb', 'from', '10.0.0.1']) SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE tab;

SELECT 'Phrase search';

CREATE TABLE tab
(
    id UInt64,
    message String,
    INDEX idx message TYPE text(tokenizer = regexpMasked, support_phrase_search = 1)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;

INSERT INTO tab SELECT * FROM tab_ref;

SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasPhrase(message, 'Connection to 10.0.0.1') ORDER BY id SETTINGS force_data_skipping_indices = 'idx');
SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasPhrase(message, 'took 1.5GB') ORDER BY id SETTINGS force_data_skipping_indices = 'idx');
SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasPhrase(message, '10.0.0.1 9000') ORDER BY id SETTINGS force_data_skipping_indices = 'idx');
SELECT groupArray(id) FROM (SELECT id FROM tab_ref WHERE hasPhrase(message, 'took 1.5GB', 'regexpMasked') ORDER BY id);

DROP TABLE tab;
DROP TABLE tab_ref;
