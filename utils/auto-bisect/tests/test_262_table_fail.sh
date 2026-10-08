#!/bin/bash
set -e

echo "$PWD"
CH_PATH=${CH_PATH:=clickhouse}


$CH_PATH client -mn -q "
SELECT version();

set enable_positional_arguments_for_projections=1;

drop table if exists experimentation_102; CREATE TABLE experimentation_102
(
    logged_at DateTime COMMENT 'Time at which the event was logged.',
    occurred_at DateTime COMMENT 'Time at which the event was occurred.',
    user_id UInt64,
    experiment String,
    variant String,
    recorded DateTime COMMENT 'Time at which the row was last recorded.',
    PROJECTION prj_exp_health
    (
        SELECT max(occurred_at)
        GROUP BY toYYYYMMDD(occurred_at)
    ),
    PROJECTION prj_query_optimization
    (
        SELECT
            CAST(user_id, 'String') AS user_id,
            experiment,
            variant,
            1000 * toUnixTimestamp(min(occurred_at)) AS first_occurred_at,
            1000 * toUnixTimestamp(max(occurred_at)) AS last_occurred_at
        GROUP BY
            user_id,
            experiment,
            variant
    )
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(occurred_at)
PRIMARY KEY (user_id, toYYYYMMDD(occurred_at))
ORDER BY (user_id, toYYYYMMDD(occurred_at))
TTL occurred_at + toIntervalDay(30)
SETTINGS index_granularity = 8192;

"
