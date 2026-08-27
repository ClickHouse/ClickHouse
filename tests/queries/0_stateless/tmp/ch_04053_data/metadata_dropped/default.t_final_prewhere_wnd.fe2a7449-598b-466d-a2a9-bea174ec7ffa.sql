ATTACH TABLE _ UUID 'fe2a7449-598b-466d-a2a9-bea174ec7ffa'
(
    `id` UInt64,
    `update_ts` DateTime,
    `value` UInt32
)
ENGINE = ReplacingMergeTree(update_ts)
PARTITION BY 0 * id
ORDER BY (update_ts, id)
SETTINGS index_granularity = 8192
