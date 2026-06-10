#!/bin/bash
set -e

CH_PATH=${CH_PATH:=clickhouse}

WORK_TREE="$1"

(
  if [ -n "$WORK_TREE" ]; then
    cd "$WORK_TREE" || exit 129
  fi
  $CH_PATH client --user admin -mn -q "

select version();
DROP TABLE IF EXISTS test_map_regression;

-- Create table matching your schema structure
CREATE TABLE test_map_regression
(
    Timestamp DateTime64(9),
    SpanAttributes Map(LowCardinality(String), String),
    INDEX idx_span_attr_value mapValues(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1
    -- INDEX idx_span_attr_key mapKeys(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY toDate(Timestamp)
ORDER BY Timestamp;
-- SETTINGS index_granularity = 10;

SET max_insert_block_size = 1;
-- Generate test data (adjust row count to match your data volume)
INSERT INTO test_map_regression
SELECT
    -- 1000 partitions/parts
    now() - toIntervalDay(number % 1000) as Timestamp,
    map(
        'project.id',
        if(number % 1000000 = 0, 'dffe5d60-3faa-421a-acf5-0755a1fb0f80', randomPrintableASCII(36))
    ) as SpanAttributes
FROM numbers(1000000);
;" || exit 129

  START=$(date +%s)
  for i in {1..4}; do
      $CH_PATH client -q "
SYSTEM DROP MARK CACHE;
SYSTEM DROP UNCOMPRESSED CACHE;
SYSTEM DROP FILESYSTEM CACHE;
SYSTEM DROP QUERY CACHE;

SET max_threads=1, compatibility='';

SELECT count() AS total_runs
FROM test_map_regression
WHERE
(Timestamp <= (now()))
 AND (SpanAttributes['project.id'] = 'dffe5d60-3faa-421a-acf5-0755a1fb0f80')
SETTINGS log_comment = 'repro_${i}';
SYSTEM FLUSH LOGS;"
  done
  END=$(date +%s)
  echo "Total time: $((END - START)) seconds"


# check logs for entry
# PushingToViews: Pushing from default.input (b0b95661-0bd7-4d42-a288-b6d7efb1bf4f) to default.mv_statistics (a230f10f-92a0-4b7b-9e3c-18acab44ff28) took 1430 ms

# log is missing in 25.5
grep 'PushingToViews: Pushing from' $SCRIPT_DIR/data/clickhouse.log \
  | grep -oE '[0-9]+ ms\.' \
  | grep -oE '[0-9]+' \
  | $CH_PATH local --structure 'ms UInt32' --input-format TSV --query "SELECT medianExact(ms), round(avg(ms)), count() FROM table"

median=$(grep 'PushingToViews: Pushing from' $SCRIPT_DIR/data/clickhouse.log \
  | grep -oE '[0-9]+ ms\.' \
  | grep -oE '[0-9]+' \
  | $CH_PATH local --structure 'ms UInt32' --input-format TSV --query "SELECT medianExact(ms) FROM table")


#if (( median > 2000 )); then
#    exit 1
#fi

)
