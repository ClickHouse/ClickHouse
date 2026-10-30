#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test build has no Parquet support
# parquet-cpp before Arrow 3.0 wrote compressed `DataPageV2` pages with `is_compressed` set to false (ARROW-10353).
# `ARROW-17100.parquet` is such a file from https://github.com/apache/arrow-testing (`data/parquet`, created by
# `parquet-cpp version 1.5.1-SNAPSHOT`, Snappy-compressed). Its pages are decompressed anyway, as Arrow C++ does.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} --query "
    DESC file('${CUR_DIR}/data_parquet/ARROW-17100.parquet', Parquet);
    SELECT * FROM file('${CUR_DIR}/data_parquet/ARROW-17100.parquet', Parquet) ORDER BY col1;"
