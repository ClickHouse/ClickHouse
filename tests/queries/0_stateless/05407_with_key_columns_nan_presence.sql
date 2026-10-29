-- `exists_` on an already-materialized Map, and the `with_key_columns` presence stream,
-- must agree with `m[k]` for `NaN` keys and for a repeated key.

SET allow_experimental_map_key_columns = 1;

SELECT 'in_memory';
SELECT m.exists_nan, m.exists_1 FROM (SELECT map(toFloat64('nan'), 1) AS m);
SELECT m.exists_nan, m.exists_1 FROM (SELECT map(toFloat64(1), 1) AS m);

DROP TABLE IF EXISTS t_wkc_nan_presence;

CREATE TABLE t_wkc_nan_presence
(
    id UInt64,
    m Map(Float64, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    serialization_info_version = 'with_types';

INSERT INTO t_wkc_nan_presence VALUES
    (1, map(toFloat64('nan'), 7)),
    (2, map(toFloat64(1), 8)),
    (3, map(toFloat64(1), 10, toFloat64(1), 20, toFloat64('nan'), 3));

SELECT 'part';
SELECT id, m.exists_nan, m.exists_1, m[toFloat64('nan')], m[toFloat64(1)], length(m)
FROM t_wkc_nan_presence
ORDER BY id;

DROP TABLE t_wkc_nan_presence;
