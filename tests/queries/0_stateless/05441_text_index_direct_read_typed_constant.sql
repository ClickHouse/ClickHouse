-- Direct read from a text index computes the predicate from its preprocessor on parts without the index.
-- A constant in it was written out as the bare value of its `Field`, which loses the type: a `Date` became its day
-- number, a `DateTime64` a `Decimal`, an `INTERVAL` a number of seconds, a `Float32` a `Float64`, and the values of
-- a `Map(String, Date)` numbers too. So `toDate('2020-01-01') + length(x)` added two integers, and rows were missing.

SET enable_full_text_index = 1;
SET use_query_condition_cache = 0;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;

DROP TABLE IF EXISTS tab;

-- The preprocessor of an `Array(String)` column is applied in a lambda over the elements.
CREATE TABLE tab
(
    no_lambda String,
    lambda Array(String),
    nested Array(String),
    datetime64 Array(String),
    interval Array(String),
    float32 Array(String),
    map Array(String)
)
ENGINE = MergeTree ORDER BY tuple();

-- A merge would build the index for the first part.
SYSTEM STOP MERGES tab;

INSERT INTO tab SELECT 'alpha zebra', ['alpha', 'zebra'], ['alpha', 'zebra'], ['alpha', 'zebra'], ['alpha', 'zebra'], ['alpha', 'zebra'], ['alpha', 'zebra'];

ALTER TABLE tab ADD INDEX idx_no_lambda no_lambda TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = replaceAll(toString(toDate('2020-01-01') + length(no_lambda)), '-', ''));
ALTER TABLE tab ADD INDEX idx_lambda lambda TYPE text(tokenizer = 'array', preprocessor = arrayStringConcat(arrayMap(x -> replaceAll(toString(toDate('2020-01-01') + length(x)), '-', ''), [lambda])));
ALTER TABLE tab ADD INDEX idx_nested nested TYPE text(tokenizer = 'array', preprocessor = arrayStringConcat(arrayMap(x -> arrayStringConcat(arrayMap(y -> concat(replaceAll(toString(toDate('2020-01-01') + length(x)), '-', ''), y), ['q'])), [nested])));
ALTER TABLE tab ADD INDEX idx_datetime64 datetime64 TYPE text(tokenizer = 'array', preprocessor = replaceRegexpAll(toString(toDateTime64('2020-01-01 00:00:00.123', 3, 'UTC') + length(datetime64)), '[^0-9]', ''));
ALTER TABLE tab ADD INDEX idx_interval interval TYPE text(tokenizer = 'array', preprocessor = toString(toUnixTimestamp(toDateTime(length(interval), 'UTC') + INTERVAL 1 DAY)));
ALTER TABLE tab ADD INDEX idx_float32 float32 TYPE text(tokenizer = 'array', preprocessor = concat('f', replaceAll(toString(if(length(float32) = 5, toFloat32(0.1), toFloat32(0.2))), '.', '')));
ALTER TABLE tab ADD INDEX idx_map map TYPE text(tokenizer = 'array', preprocessor = concat(map, replaceAll(toString(map('zebra', toDate('2020-01-01'))[map]), '-', '')));

INSERT INTO tab SELECT 'beta zebra', ['beta', 'zebra'], ['beta', 'zebra'], ['beta', 'zebra'], ['beta', 'zebra'], ['beta', 'zebra'], ['beta', 'zebra'];

SELECT 'no lambda', count() FROM tab WHERE hasAnyTokens(no_lambda, ['20200112', '20200111']);
SELECT 'lambda', count() FROM tab WHERE hasAllTokens(lambda, ['20200106']);
-- The inner lambda captures the constant from the scope of the outer one.
SELECT 'nested lambdas', count() FROM tab WHERE hasAllTokens(nested, ['20200106q']);
-- The expected result, without direct read.
SELECT 'nested lambdas, no direct read', count() FROM tab WHERE hasAllTokens(nested, ['20200106q']) SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'DateTime64', count() FROM tab WHERE hasAllTokens(datetime64, ['20200101000005123']);
SELECT 'INTERVAL', count() FROM tab WHERE hasAllTokens(interval, ['86405']);
SELECT 'Float32', count() FROM tab WHERE hasAllTokens(float32, ['f01']);
SELECT 'Map', count() FROM tab WHERE hasAllTokens(map, ['zebra20200101']);
-- The needles must stay a constant, and a cast is not folded when its result takes 1 MiB or more.
SELECT 'needles of 1 MiB', count() FROM tab WHERE hasAnyTokens(no_lambda, (SELECT arrayConcat(groupArray(concat('tok', toString(number))), ['20200112', '20200111']) FROM numbers(100000)));

DROP TABLE tab;
