-- A COMPLEX_KEY_DIRECT dictionary sends the requested keys to the source as text, and the source can answer
-- with a key that is not byte-identical to the requested one: `0` for `-0.0`, or the canonical NaN for a NaN
-- with another bit pattern. `dictHas` and the `direct` JOIN must report such keys as missing.
-- A batch that also requests `0.` or the canonical NaN finds the answered key among the requested ones,
-- so the batches below request only the keys that are not byte-identical.

DROP DICTIONARY IF EXISTS dict;
DROP TABLE IF EXISTS src;

CREATE TABLE src (a Float64, v UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO src VALUES (0, 1), (nan, 2);

CREATE DICTIONARY dict (a Float64, v UInt8) PRIMARY KEY a SOURCE(CLICKHOUSE(TABLE 'src')) LAYOUT(COMPLEX_KEY_DIRECT());

SELECT 'dictHas';
SELECT dictHas('dict', tuple(0.));
SELECT dictHas('dict', tuple(-0.));
SELECT dictHas('dict', tuple(nan));
SELECT dictHas('dict', tuple(reinterpretAsFloat64(toUInt64(9221120237041090561))));
SELECT dictHas('dict', tuple(k)) FROM (SELECT arrayJoin([-0., reinterpretAsFloat64(toUInt64(9221120237041090561))]) AS k);

SELECT 'dictGetOrDefault';
SELECT dictGetOrDefault('dict', 'v', tuple(-0.), 42);
SELECT dictGetOrDefault('dict', 'v', tuple(reinterpretAsFloat64(toUInt64(9221120237041090561))), 42);

SELECT 'direct JOIN';
SELECT l.n, d.v
FROM (SELECT number AS n, [-0., reinterpretAsFloat64(toUInt64(9221120237041090561))][number + 1] AS k FROM numbers(2)) AS l
LEFT JOIN dict AS d ON l.k = d.a
ORDER BY l.n
SETTINGS join_algorithm = 'direct', join_use_nulls = 0;

DROP DICTIONARY dict;
DROP TABLE src;
