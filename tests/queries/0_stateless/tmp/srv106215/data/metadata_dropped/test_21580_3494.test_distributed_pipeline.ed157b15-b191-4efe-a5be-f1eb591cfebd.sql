ATTACH TABLE _ UUID 'ed157b15-b191-4efe-a5be-f1eb591cfebd'
(
    `a` UInt64,
    `b` UInt64,
    `c` UInt64
)
ENGINE = MergeTree
ORDER BY c
SETTINGS index_granularity = 8192
