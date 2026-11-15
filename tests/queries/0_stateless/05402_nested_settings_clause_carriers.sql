-- A nested `SETTINGS` clause is applied one change at a time, clamped to the settings constraints.
-- That must keep an explicit assignment that `compatibility` in the same clause would otherwise override,
-- as a top-level clause does, and a reset must clear an explicit assignment under `compatibility`.
SELECT * FROM
(
    SELECT getSetting('use_variant_as_common_type')
    SETTINGS use_variant_as_common_type = 1, compatibility = '25.12'
);

SELECT * FROM
(
    SELECT getSetting('use_variant_as_common_type')
    SETTINGS compatibility = '25.12'
);

SET compatibility = '25.12', use_variant_as_common_type = 1;
SELECT * FROM
(
    SELECT getSetting('use_variant_as_common_type')
    SETTINGS use_variant_as_common_type = DEFAULT
);
SET compatibility = '', use_variant_as_common_type = DEFAULT;

-- `compatibility = DEFAULT` changes the values of the settings like an assignment of `compatibility` does.
SET compatibility = '25.12';
SELECT * FROM
(
    SELECT getSetting('use_variant_as_common_type')
    SETTINGS use_variant_as_common_type = 0, compatibility = DEFAULT
);
SET compatibility = '';

-- A trailing query-level `SETTINGS` clause of a set operation applies to every operand, also when
-- `INTERSECT` / `EXCEPT` nests the last operand that carries it.
SET obfuscate_markov_order = 0;
SELECT count() FROM
(
    (SELECT * FROM obfuscate(SELECT 'x' AS s FROM numbers(1)) LIMIT 1)
    UNION ALL
    SELECT 'x'
    INTERSECT ALL
    SELECT 'x'
    SETTINGS obfuscate_markov_order = DEFAULT
);
SET obfuscate_markov_order = DEFAULT;

-- A parenthesized set operation keeps its trailing clause to itself.
SET transform_null_in = 0;
SELECT n FROM
(
    SELECT NULL IN (NULL) AS n
    UNION ALL
    (SELECT NULL IN (NULL) AS n INTERSECT ALL SELECT NULL IN (NULL) AS n SETTINGS transform_null_in = 1)
)
ORDER BY n NULLS FIRST;
SELECT n FROM
(
    SELECT NULL IN (NULL) AS n
    UNION ALL
    (SELECT NULL IN (NULL) AS n EXCEPT ALL SELECT 0 AS n SETTINGS transform_null_in = 1)
)
ORDER BY n NULLS FIRST;
SELECT n FROM
(
    SELECT NULL IN (NULL) AS n
    UNION ALL
    (SELECT NULL IN (NULL) AS n UNION ALL SELECT NULL IN (NULL) AS n SETTINGS transform_null_in = 1)
)
ORDER BY n NULLS FIRST;
SELECT n FROM
(
    SELECT NULL IN (NULL) AS n
    UNION DISTINCT
    (SELECT NULL IN (NULL) AS n INTERSECT ALL SELECT NULL IN (NULL) AS n SETTINGS transform_null_in = 1)
)
ORDER BY n NULLS FIRST;
SELECT n FROM
(
    SELECT NULL IN (NULL) AS n
    UNION DISTINCT
    (SELECT NULL IN (NULL) AS n EXCEPT ALL SELECT 0 AS n SETTINGS transform_null_in = 1)
)
ORDER BY n NULLS FIRST;
SELECT n FROM
(
    SELECT NULL IN (NULL) AS n
    UNION DISTINCT
    (SELECT NULL IN (NULL) AS n UNION ALL SELECT NULL IN (NULL) AS n SETTINGS transform_null_in = 1)
)
ORDER BY n NULLS FIRST;
-- So does a parenthesized single `SELECT`, also when `INTERSECT` / `EXCEPT` nests it as the last operand.
SELECT * FROM
(
    SELECT getSetting('transform_null_in') AS x
    UNION ALL
    SELECT false
    INTERSECT ALL
    (SELECT true SETTINGS transform_null_in = 1)
);
SELECT x FROM
(
    SELECT getSetting('transform_null_in') AS x
    UNION ALL
    (SELECT getSetting('transform_null_in') AS x SETTINGS transform_null_in = 1)
)
ORDER BY x;
SELECT getSetting('transform_null_in') AS x INTERSECT ALL (SELECT false AS x SETTINGS transform_null_in = 1);
-- Without the parentheses, the trailing clause applies to the whole union.
SELECT x FROM
(
    SELECT getSetting('transform_null_in') AS x
    UNION ALL
    SELECT getSetting('transform_null_in') AS x SETTINGS transform_null_in = 1
)
ORDER BY x;
-- Both scopes are kept in the query sent to a remote server.
SELECT x FROM remote('127.0.0.1', view(SELECT getSetting('transform_null_in') AS x UNION ALL (SELECT getSetting('transform_null_in') AS x SETTINGS transform_null_in = 1))) ORDER BY x;
SELECT x FROM remote('127.0.0.1', view(SELECT getSetting('transform_null_in') AS x UNION ALL SELECT getSetting('transform_null_in') AS x SETTINGS transform_null_in = 1)) ORDER BY x;
SET transform_null_in = DEFAULT;

-- Both query-level clauses of a union bind query parameters for every arm, in the order they are applied.
SELECT {n:UInt8} AS x UNION ALL SELECT {n:UInt8} AS x SETTINGS param_n = '7' FORMAT TSV SETTINGS param_n = '9';
SELECT {n:UInt8} AS x UNION ALL SELECT {n:UInt8} AS x SETTINGS param_n = '7' FORMAT TSV SETTINGS max_threads = 1;

-- The query cache check replays the whole nested clause: `readonly = 1` makes the constraints drop the
-- seed assignment that follows it, so `obfuscate` runs with a random seed and the result must not be cached.
SELECT * FROM
(
    SELECT number FROM obfuscate(SELECT number FROM numbers(4)) LIMIT 4
    SETTINGS readonly = 1, obfuscate_seed = 'stable'
)
SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
