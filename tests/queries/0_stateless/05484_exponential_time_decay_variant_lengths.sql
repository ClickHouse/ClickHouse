SET allow_experimental_time_decay_aggregate_functions = 1;

-- A mismatched first alternative must not prevent selecting the matching decay length.
SELECT variantType(JSONExtract(
    '{"value":2,"timestamp":0,"decay_length":20}',
    'Variant(ExponentialTimeDecaying64(10), ExponentialTimeDecaying64(20))'));

SELECT variantType(JSONExtract(
    '[2,0,20]',
    'Variant(ExponentialTimeDecaying128(10), ExponentialTimeDecaying128(20))'));

-- Preserve direct metadata through JSON output and extraction for both alternatives.
WITH
    JSONExtract(json, 'Variant(ExponentialTimeDecaying64(10, direct), ExponentialTimeDecaying64(20, direct))') AS v,
    JSONExtract(toJSONString(v), 'Variant(ExponentialTimeDecaying64(10, direct), ExponentialTimeDecaying64(20, direct))') AS restored
SELECT variantType(v), variantType(restored), toJSONString(v) = toJSONString(restored)
FROM
(
    SELECT arrayJoin(['[3,0,10]', '[2,0,20]']) AS json
)
ORDER BY variantType(v);

-- Text deserialization probes alternatives through the same nonthrowing interface.
SELECT variantType(v)
FROM format(TSV,
    'v Variant(ExponentialTimeDecaying64(10, direct), ExponentialTimeDecaying64(20, direct))',
    '(2,0,20)\n(3,0,10)')
ORDER BY variantType(v);
