#!/bin/bash
set -e

WORK_TREE="$1"
(
  cd $WORK_TREE || exit 1

  echo "$PWD"

  cat << EOF > /tmp/query.sql
SELECT version();

DROP TABLE IF EXISTS t;
CREATE TABLE test_map_contains
(
    ResourceAttributes Map(LowCardinality(String), String),
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
)
ORDER BY tuple();

INSERT INTO test_map_contains VALUES
(
    {
        'rum.sessionId': '5f449ff91029b8041c73af2e884ea1bc',
    },
);

SELECT
throwIf(count()==0)
FROM test_map_contains
WHERE mapContains(ResourceAttributes, 'rum.sessionId')
EOF

  # CH_PATH is the path to downloaded binary
  $CH_PATH client --queries-file /tmp/query.sql
)
