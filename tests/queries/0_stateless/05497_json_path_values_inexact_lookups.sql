-- `jsonPathValues` must not answer predicates whose semantics its tokens do not preserve.
-- Each query runs without the index and with direct reads from the index; the results must match.

SET enable_json_type = 1;
SET allow_experimental_variant_type = 1;
SET allow_suspicious_low_cardinality_types = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET cast_string_to_variant_use_inference = 0;

DROP TABLE IF EXISTS json_path_values_inexact;
CREATE TABLE json_path_values_inexact
(
    id UInt64,
    data JSON(s String, n UInt64, u LowCardinality(UInt8), v Variant(UInt64, String), vs Array(Variant(UInt64, String))),
    INDEX idx data TYPE text(tokenizer = jsonPathValues) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, allow_experimental_text_index_json_path_values = 1;

INSERT INTO json_path_values_inexact VALUES
    (1, '{"s":"Äpfel","n":42,"u":2,"v":42,"vs":[42]}'),
    (2, '{"s":"x","n":1,"u":1,"v":7,"vs":[""]}'),
    (3, '{"s":"y","n":0,"u":0,"v":0,"vs":[7]}');

-- `ilike` folds UTF-8 case, the dictionary scan folds ASCII only.
SELECT 'ilike substring', arraySort(groupArray(id)) FROM json_path_values_inexact WHERE data.s ILIKE '%äpf%' SETTINGS use_skip_indexes = 0;
SELECT 'ilike substring', arraySort(groupArray(id)) FROM json_path_values_inexact WHERE data.s ILIKE '%äpf%';
SELECT 'ilike prefix', arraySort(groupArray(id)) FROM json_path_values_inexact WHERE data.s ILIKE 'äpf%' SETTINGS use_skip_indexes = 0;
SELECT 'ilike prefix', arraySort(groupArray(id)) FROM json_path_values_inexact WHERE data.s ILIKE 'äpf%';

-- `CAST` to `Bool` changes the value even though `UInt8` equals `Bool`.
SELECT 'cast to Bool', arraySort(groupArray(id)) FROM json_path_values_inexact WHERE CAST(data.u, 'Bool') = true SETTINGS use_skip_indexes = 0;
SELECT 'cast to Bool', arraySort(groupArray(id)) FROM json_path_values_inexact WHERE CAST(data.u, 'Bool') = true;

-- A `Variant` constant cannot be rebuilt from its `Field` for the fallback expression.
SELECT 'variant constant', arraySort(groupArray(id)) FROM json_path_values_inexact WHERE data.n = CAST(toUInt64(42), 'Variant(String, UInt64)') SETTINGS use_skip_indexes = 0;
SELECT 'variant constant', arraySort(groupArray(id)) FROM json_path_values_inexact WHERE data.n = CAST(toUInt64(42), 'Variant(String, UInt64)');
SELECT 'variant equality', arraySort(groupArray(id)) FROM json_path_values_inexact WHERE data.v = CAST(toUInt64(42), 'Variant(UInt64, String)') SETTINGS use_skip_indexes = 0;
SELECT 'variant equality', arraySort(groupArray(id)) FROM json_path_values_inexact WHERE data.v = CAST(toUInt64(42), 'Variant(UInt64, String)');

-- The token of a `Variant` element does not identify its alternative.
SELECT 'variant has string', arraySort(groupArray(id)) FROM json_path_values_inexact WHERE has(data.vs, CAST('42', 'Variant(UInt64, String)')) SETTINGS use_skip_indexes = 0;
SELECT 'variant has string', arraySort(groupArray(id)) FROM json_path_values_inexact WHERE has(data.vs, CAST('42', 'Variant(UInt64, String)'));
SELECT 'variant has number', arraySort(groupArray(id)) FROM json_path_values_inexact WHERE has(data.vs, CAST(toUInt64(42), 'Variant(UInt64, String)')) SETTINGS use_skip_indexes = 0;
SELECT 'variant has number', arraySort(groupArray(id)) FROM json_path_values_inexact WHERE has(data.vs, CAST(toUInt64(42), 'Variant(UInt64, String)'));
SELECT 'variant has empty', arraySort(groupArray(id)) FROM json_path_values_inexact WHERE has(data.vs, CAST('', 'Variant(UInt64, String)')) SETTINGS use_skip_indexes = 0;
SELECT 'variant has empty', arraySort(groupArray(id)) FROM json_path_values_inexact WHERE has(data.vs, CAST('', 'Variant(UInt64, String)'));

DROP TABLE json_path_values_inexact;
