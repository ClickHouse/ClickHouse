-- A keep set above the bloom filter cutoff: the index must hold exactly the kept tokens,
-- including when bloom false positives fall back to the token map lookup.

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (
  id UInt32,
  s String,
  INDEX idx(s) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = if(s IN (
    'k0', 'k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7', 'k8', 'k9', 'k10', 'k11', 'k12', 'k13', 'k14', 'k15', 'k16', 'k17', 'k18', 'k19',
    'k20', 'k21', 'k22', 'k23', 'k24', 'k25', 'k26', 'k27', 'k28', 'k29', 'k30', 'k31', 'k32', 'k33', 'k34', 'k35', 'k36', 'k37', 'k38', 'k39',
    'k40', 'k41', 'k42', 'k43', 'k44', 'k45', 'k46', 'k47', 'k48', 'k49', 'k50', 'k51', 'k52', 'k53', 'k54', 'k55', 'k56', 'k57', 'k58', 'k59',
    'k60', 'k61', 'k62', 'k63', 'k64', 'k65', 'k66', 'k67', 'k68', 'k69', 'k70', 'k71', 'k72', 'k73', 'k74', 'k75', 'k76', 'k77', 'k78', 'k79',
    'k80', 'k81', 'k82', 'k83', 'k84', 'k85', 'k86', 'k87', 'k88', 'k89', 'k90', 'k91', 'k92', 'k93', 'k94', 'k95', 'k96', 'k97', 'k98', 'k99'), s, ''))
)
ENGINE = MergeTree
ORDER BY id;

-- Kept tokens k0..k99, rejected tokens k100..k159 and 5000 distinct z-tokens.
INSERT INTO tab SELECT number, format('k{} z{} k{} z{}', toString(number % 160), toString(number), toString(number * 7 % 160), toString(number + 2500)) FROM numbers(2500);

SELECT count(), sum(cardinality) FROM mergeTreeTextIndex(currentDatabase(), tab, idx);

SELECT
    (SELECT groupArraySorted(1000)((token, cardinality)) FROM mergeTreeTextIndex(currentDatabase(), tab, idx))
    = (SELECT groupArraySorted(1000)((t, c)) FROM (
        SELECT t, uniqExact(id) AS c FROM tab ARRAY JOIN tokens(s, 'splitByNonAlpha') AS t
        WHERE t IN (SELECT concat('k', toString(number)) FROM numbers(100))
        GROUP BY t));

SELECT count() FROM tab WHERE hasToken(s, 'k42');
SELECT count() FROM tab WHERE hasToken(s, 'k42') SETTINGS use_skip_indexes = 0;

DROP TABLE tab;
