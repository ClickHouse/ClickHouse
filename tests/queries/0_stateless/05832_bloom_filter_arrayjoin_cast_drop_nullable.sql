DROP TABLE IF EXISTS t_bf_aj_nullable_cast;

SET cast_keep_nullable = 0;
SET query_plan_enable_optimizations = 1;
SET query_plan_lower_array_join_function = 1;
SET query_plan_fuse_filter_into_array_join = 1;
SET use_skip_indexes_on_data_read = 0;

CREATE TABLE t_bf_aj_nullable_cast
(
    id UInt64,
    tags Array(String),
    INDEX idx_tags tags TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0;

INSERT INTO t_bf_aj_nullable_cast VALUES (1, ['target']), (2, ['other']);

-- Both cast positions are valid when no stored array generates a NULL.
SELECT id FROM t_bf_aj_nullable_cast
WHERE CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))) AS String) IN ('target')
SETTINGS use_skip_indexes = 0;
SELECT id FROM t_bf_aj_nullable_cast
WHERE CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))) AS String) IN ('target')
SETTINGS use_skip_indexes = 1;
SELECT id FROM t_bf_aj_nullable_cast
WHERE arrayJoin(CAST(emptyArrayToSingle(CAST(tags AS Array(Nullable(String)))) AS Array(String))) IN ('target')
SETTINGS use_skip_indexes = 0;
SELECT id FROM t_bf_aj_nullable_cast
WHERE arrayJoin(CAST(emptyArrayToSingle(CAST(tags AS Array(Nullable(String)))) AS Array(String))) IN ('target')
SETTINGS use_skip_indexes = 1;

-- A real empty array still generates a NULL that cannot be cast to String.
INSERT INTO t_bf_aj_nullable_cast VALUES (3, []);

SELECT count() FROM t_bf_aj_nullable_cast
WHERE CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))) AS String) IN ('target')
SETTINGS use_skip_indexes = 0; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT count() FROM t_bf_aj_nullable_cast
WHERE CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))) AS String) IN ('target')
SETTINGS use_skip_indexes = 1; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT count() FROM t_bf_aj_nullable_cast
WHERE arrayJoin(CAST(emptyArrayToSingle(CAST(tags AS Array(Nullable(String)))) AS Array(String))) IN ('target')
SETTINGS use_skip_indexes = 0; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT count() FROM t_bf_aj_nullable_cast
WHERE arrayJoin(CAST(emptyArrayToSingle(CAST(tags AS Array(Nullable(String)))) AS Array(String))) IN ('target')
SETTINGS use_skip_indexes = 1; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }

DROP TABLE t_bf_aj_nullable_cast;
