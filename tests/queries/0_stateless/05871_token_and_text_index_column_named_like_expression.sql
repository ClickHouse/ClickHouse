-- A `tokenbf_v1`, `ngrambf_v1` or `text` index on a column literally named like an expression is not used for that expression.

DROP TABLE IF EXISTS t_token;
DROP TABLE IF EXISTS t_text;
DROP TABLE IF EXISTS t_reverse;
DROP TABLE IF EXISTS t_genuine;
DROP TABLE IF EXISTS t_normalized;
DROP TABLE IF EXISTS t_shadow;

CREATE TABLE t_token
(
    s String,
    `lower(s)` String,
    m Map(String, String),
    `mapKeys(m)` Array(String),
    `mapValues(m)` Array(String),
    j JSON,
    `JSONAllPaths(j)` Array(String),
    INDEX idx_lower `lower(s)` TYPE tokenbf_v1(512, 3, 0),
    INDEX idx_ngram `lower(s)` TYPE ngrambf_v1(3, 512, 3, 0),
    INDEX idx_upper upper(`lower(s)`) TYPE tokenbf_v1(512, 3, 0),
    INDEX idx_keys `mapKeys(m)` TYPE tokenbf_v1(512, 3, 0),
    INDEX idx_values `mapValues(m)` TYPE tokenbf_v1(512, 3, 0),
    INDEX idx_paths `JSONAllPaths(j)` TYPE tokenbf_v1(512, 3, 0)
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_token VALUES ('HELLO', 'zzz', {'k': 'v'}, ['zzz'], ['zzz'], '{"a": 1}', ['zzz']);

SELECT count() FROM t_token WHERE lower(s) = 'hello';
SELECT count() FROM t_token WHERE lower(s) IN ('hello');
SELECT count() FROM t_token WHERE hasToken(lower(s), 'hello');
SELECT count() FROM t_token WHERE hasTokenCaseInsensitive(s, 'HELLO');
SELECT count() FROM t_token WHERE lower(s) LIKE '%hell%';
SELECT count() FROM t_token WHERE upper(lower(s)) = 'HELLO';
SELECT count() FROM t_token WHERE m['k'] = 'v';
SELECT count() FROM t_token WHERE mapContains(m, 'k') SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_token WHERE has(mapKeys(m), 'k') SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_token WHERE mapContainsValue(m, 'v') SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_token WHERE isNotNull(j.a);
SELECT count() FROM t_token WHERE j.a = 1;

-- Still used for the column itself.
SELECT count() FROM t_token WHERE `lower(s)` = 'zzz' SETTINGS force_data_skipping_indices = 'idx_lower';
SELECT count() FROM t_token WHERE has(`mapKeys(m)`, 'zzz') SETTINGS force_data_skipping_indices = 'idx_keys';
SELECT count() FROM t_token WHERE upper(`lower(s)`) = 'ZZZ' SETTINGS force_data_skipping_indices = 'idx_upper';

CREATE TABLE t_text
(
    id UInt64,
    s String,
    `lower(s)` String,
    m Map(String, String),
    `mapKeys(m)` Array(String),
    `mapValues(m)` Array(String),
    j JSON,
    `JSONAllValues(j)` Array(String),
    `JSONAllPaths(j)` Array(String),
    `splitByChar(' ', s)` Array(String),
    INDEX idx_lower `lower(s)` TYPE text(tokenizer = 'splitByNonAlpha'),
    INDEX idx_upper upper(`lower(s)`) TYPE text(tokenizer = 'splitByNonAlpha'),
    INDEX idx_keys `mapKeys(m)` TYPE text(tokenizer = 'splitByNonAlpha'),
    INDEX idx_values `mapValues(m)` TYPE text(tokenizer = 'splitByNonAlpha'),
    INDEX idx_json_values `JSONAllValues(j)` TYPE text(tokenizer = 'splitByNonAlpha'),
    INDEX idx_json_paths `JSONAllPaths(j)` TYPE text(tokenizer = 'splitByNonAlpha'),
    INDEX idx_split `splitByChar(' ', s)` TYPE text(tokenizer = 'splitByNonAlpha'),
    INDEX idx_filter arrayFilter(x -> x != '', `splitByChar(' ', s)`) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_text VALUES (1, 'HELLO world', 'zzz', {'k': 'v'}, ['zzz'], ['zzz'], '{"a": "hello"}', ['zzz'], ['zzz'], ['zzz']);

SELECT count() FROM t_text WHERE hasToken(lower(s), 'hello');
SELECT groupArray(id) FROM (SELECT id FROM t_text WHERE hasToken(lower(s), 'hello'));
SELECT count() FROM t_text WHERE lower(s) = 'hello world';
SELECT count() FROM t_text WHERE lower(s) IN ('hello world');
SELECT count() FROM t_text WHERE (id, lower(s)) IN ((1, 'hello world'));
SELECT count() FROM t_text WHERE lower(s) LIKE '%hello%';
SELECT count() FROM t_text WHERE hasToken(upper(lower(s)), 'HELLO');
SELECT count() FROM t_text WHERE m['k'] = 'v';
SELECT count() FROM t_text WHERE m['k'] IN ('v');
SELECT count() FROM t_text WHERE startsWith(m['k'], 'v');
SELECT count() FROM t_text WHERE mapContains(m, 'k') SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_text WHERE mapContainsValue(m, 'v') SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_text WHERE j.a = 'hello';
SELECT count() FROM t_text WHERE isNotNull(j.a);
SELECT count() FROM t_text WHERE has(splitByChar(' ', s), 'world');
SELECT count() FROM t_text WHERE arrayJoin(splitByChar(' ', s)) = 'world';
SELECT count() FROM t_text WHERE has(arrayFilter(x -> x != '', splitByChar(' ', s)), 'world');

-- Still used for the column itself.
SELECT count() FROM t_text WHERE hasToken(`lower(s)`, 'zzz') SETTINGS force_data_skipping_indices = 'idx_lower';
SELECT count() FROM t_text WHERE has(`splitByChar(' ', s)`, 'zzz') SETTINGS force_data_skipping_indices = 'idx_split';
SELECT count() FROM t_text WHERE hasToken(upper(`lower(s)`), 'ZZZ') SETTINGS force_data_skipping_indices = 'idx_upper';

-- The index computes the expression, the query reads the column named like it.
CREATE TABLE t_reverse
(
    s String,
    `lower(s)` String,
    `splitByChar(' ', s)` Array(String),
    INDEX idx_token upper(lower(s)) TYPE tokenbf_v1(512, 3, 0),
    INDEX idx_text upper(lower(s)) TYPE text(tokenizer = 'splitByNonAlpha'),
    INDEX idx_filter arrayFilter(x -> x != '', splitByChar(' ', s)) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_reverse VALUES ('HELLO world', 'zzz', ['zzz']);

SELECT count() FROM t_reverse WHERE upper(`lower(s)`) = 'ZZZ';
SELECT count() FROM t_reverse WHERE hasToken(upper(`lower(s)`), 'ZZZ');
SELECT count() FROM t_reverse WHERE has(arrayFilter(x -> x != '', `splitByChar(' ', s)`), 'zzz');

SELECT count() FROM t_reverse WHERE upper(lower(s)) = 'HELLO WORLD' SETTINGS force_data_skipping_indices = 'idx_token';
SELECT count() FROM t_reverse WHERE hasToken(upper(lower(s)), 'HELLO') SETTINGS force_data_skipping_indices = 'idx_text';
SELECT count() FROM t_reverse WHERE has(arrayFilter(x -> x != '', splitByChar(' ', s)), 'world') SETTINGS force_data_skipping_indices = 'idx_filter';

-- Indexes on the expressions themselves are still used.
CREATE TABLE t_genuine
(
    s String,
    m Map(String, String),
    j JSON,
    arr Array(String),
    `'k'` String,
    INDEX idx_lower lower(s) TYPE tokenbf_v1(512, 3, 0),
    INDEX idx_keys mapKeys(m) TYPE tokenbf_v1(512, 3, 0),
    INDEX idx_paths JSONAllPaths(j) TYPE tokenbf_v1(512, 3, 0),
    INDEX idx_lambda arrayMap(x -> lower(x), arr) TYPE tokenbf_v1(512, 3, 0),
    INDEX idx_concat concat(`'k'`, 'k') TYPE tokenbf_v1(512, 3, 0)
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_genuine VALUES ('HELLO', {'k': 'v'}, '{"a": 1}', ['HELLO'], 'x');

SELECT count() FROM t_genuine WHERE concat('k', `'k'`) = 'kx';

SELECT count() FROM t_genuine WHERE lower(s) = 'hello' SETTINGS force_data_skipping_indices = 'idx_lower';
SELECT count() FROM t_genuine WHERE hasTokenCaseInsensitive(s, 'HELLO') SETTINGS force_data_skipping_indices = 'idx_lower';
SELECT count() FROM t_genuine WHERE m['k'] = 'v' SETTINGS force_data_skipping_indices = 'idx_keys';
SELECT count() FROM t_genuine WHERE mapContains(m, 'k') SETTINGS force_data_skipping_indices = 'idx_keys', optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_genuine WHERE isNotNull(j.a) SETTINGS force_data_skipping_indices = 'idx_paths';
SELECT count() FROM t_genuine WHERE has(arrayMap(x -> lower(x), arr), 'hello') SETTINGS force_data_skipping_indices = 'idx_lambda';
SELECT count() FROM t_genuine WHERE concat(`'k'`, 'k') = 'xk' SETTINGS force_data_skipping_indices = 'idx_concat';

-- The text index also matches its expression after the rewrite of `optimize_empty_string_comparisons`.
CREATE TABLE t_normalized
(
    arr Array(String),
    `2` UInt64,
    INDEX idx_col arrayFilter(x -> substring(x, `2`) != '', arr) TYPE text(tokenizer = 'splitByNonAlpha'),
    INDEX idx_const arrayFilter(x -> substring(x, 3) != '', arr) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_normalized VALUES (['hello'], 10);

SELECT count() FROM t_normalized WHERE has(arrayFilter(x -> substring(x, 2) != '', arr), 'hello');

SELECT count() FROM t_normalized WHERE has(arrayFilter(x -> substring(x, 3) != '', arr), 'hello') SETTINGS force_data_skipping_indices = 'idx_const';
SELECT count() FROM t_normalized WHERE has(arrayFilter(x -> '' != substring(x, 3), arr), 'hello') SETTINGS force_data_skipping_indices = 'idx_const', optimize_empty_string_comparisons = 1;
SELECT count() FROM t_normalized WHERE has(arrayFilter(x -> notEmpty(substring(x, 3)), arr), 'hello') SETTINGS force_data_skipping_indices = 'idx_const';
SELECT count() FROM t_normalized WHERE has(arrayFilter(x -> substring(x, 3) != '', arr), 'hello') SETTINGS force_data_skipping_indices = 'idx_const', optimize_empty_string_comparisons = 0;
SELECT count() FROM t_normalized WHERE has(arrayFilter(x -> substring(x, `2`) != '', arr), 'hello') SETTINGS force_data_skipping_indices = 'idx_col';

-- A column named like a `JSON` subcolumn is not that subcolumn.
CREATE TABLE t_shadow
(
    j JSON,
    `j.a` Nullable(String),
    INDEX idx_token JSONAllPaths(j) TYPE tokenbf_v1(512, 3, 0),
    INDEX idx_text JSONAllValues(j) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_shadow VALUES ('{"b": "y"}', 'x');

SELECT count() FROM t_shadow WHERE `j.a` = 'x';
SELECT count() FROM t_shadow WHERE hasToken(`j.a`, 'x');
SELECT count() FROM t_shadow WHERE isNotNull(`j.a`) SETTINGS optimize_functions_to_subcolumns = 0;

SELECT count() FROM t_shadow WHERE j.b = 'y' SETTINGS force_data_skipping_indices = 'idx_text';

DROP TABLE t_token;
DROP TABLE t_text;
DROP TABLE t_reverse;
DROP TABLE t_genuine;
DROP TABLE t_normalized;
DROP TABLE t_shadow;
