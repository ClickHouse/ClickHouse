#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg is not in fast test

# An Iceberg table written by Apache Iceberg Java, see `data_iceberg/icebird/README.md`. Its Parquet files store the
# column `Breed Name` as `Breed_x20Name` with field id 1, also in the equality delete file (`equality_ids = [1]`).
# The expected results are those of icebird's `test/read.java.test.js`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cd "$CUR_DIR/data_iceberg/icebird" || exit 1

$CLICKHOUSE_LOCAL -q "
-- Position deletes only.
SELECT count() FROM icebergLocal('java/bunnies', SETTINGS iceberg_metadata_file_path = 'metadata/v4.metadata.json');
SELECT \`Breed Name\`, \`Popularity Rank\`, __happy__ FROM icebergLocal('java/bunnies', SETTINGS iceberg_metadata_file_path = 'metadata/v4.metadata.json') WHERE \`Breed Name\` = 'New Zealand';
-- The current snapshot deletes that row by equality and adds an updated one with the same sequence number.
SELECT count() FROM icebergLocal('java/bunnies');
SELECT \`Breed Name\`, \`Popularity Rank\`, __happy__ FROM icebergLocal('java/bunnies') WHERE \`Breed Name\` = 'New Zealand';
"
