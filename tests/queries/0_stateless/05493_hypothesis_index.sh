#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# rows read after the hypothesis index skipped granules
rows_read()
{
    $CLICKHOUSE_CLIENT --merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability 0 --query "SELECT count() FROM $1 FORMAT JSON" | grep "rows_read"
}

$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_hypothesis (i UInt64, a UInt64, b UInt64, c Float64, INDEX t (a < b AND b <= c) TYPE hypothesis GRANULARITY 1)
ENGINE = MergeTree ORDER BY i SETTINGS index_granularity = 1;
INSERT INTO t_hypothesis VALUES (0, 1, 2, 2), (1, 2, 1, 2), (2, 2, 2, 1), (3, 1, 2, 3);
"

echo "-- a < b AND b <= c holds in granules 0 and 3"
rows_read "t_hypothesis WHERE b > a"  # 4
rows_read "t_hypothesis WHERE b <= a" # 2
rows_read "t_hypothesis WHERE b >= a" # 4
rows_read "t_hypothesis WHERE b = a"  # 2
rows_read "t_hypothesis WHERE c < a"  # 2, through a < b <= c
rows_read "t_hypothesis WHERE c = a"  # 2
rows_read "t_hypothesis WHERE c > a"  # 4
rows_read "t_hypothesis WHERE NOT (b > a)"       # 2
rows_read "t_hypothesis WHERE b <= a OR i = 3"   # 4
rows_read "t_hypothesis WHERE b <= a AND i >= 0" # 2
rows_read "t_hypothesis PREWHERE b <= a"         # 2

$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_hypothesis_not_equals (i UInt64, a UInt64, b UInt64, INDEX t (a != b) TYPE hypothesis GRANULARITY 1)
ENGINE = MergeTree ORDER BY i SETTINGS index_granularity = 1;
INSERT INTO t_hypothesis_not_equals VALUES (0, 1, 2), (1, 2, 1), (2, 2, 2), (3, 1, 0);
"

echo "-- a != b holds in granules 0, 1 and 3"
rows_read "t_hypothesis_not_equals WHERE a < b"  # 4
rows_read "t_hypothesis_not_equals WHERE a <= b" # 4
rows_read "t_hypothesis_not_equals WHERE a = b"  # 1
rows_read "t_hypothesis_not_equals WHERE a != b" # 4

$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_hypothesis_equals (i UInt64, a UInt64, b UInt64, INDEX t (a = b) TYPE hypothesis GRANULARITY 1)
ENGINE = MergeTree ORDER BY i SETTINGS index_granularity = 1;
INSERT INTO t_hypothesis_equals VALUES (0, 1, 2), (1, 2, 1), (2, 2, 2), (3, 1, 0);
"

echo "-- a = b holds in granule 2"
rows_read "t_hypothesis_equals WHERE a < b"  # 3
rows_read "t_hypothesis_equals WHERE a <= b" # 4
rows_read "t_hypothesis_equals WHERE a = b"  # 4
rows_read "t_hypothesis_equals WHERE a != b" # 3

$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_hypothesis_constraint (i UInt64, a UInt64, b UInt64, c UInt64, CONSTRAINT c1 ASSUME b <= c, INDEX t (a < b) TYPE hypothesis GRANULARITY 1)
ENGINE = MergeTree ORDER BY i SETTINGS index_granularity = 1;
INSERT INTO t_hypothesis_constraint VALUES (0, 1, 2, 2), (1, 2, 1, 2), (2, 2, 2, 2), (3, 1, 2, 3);
"

echo "-- a < b holds in granules 0 and 3, the constraint adds b <= c"
rows_read "t_hypothesis_constraint WHERE c < a"  # 2
rows_read "t_hypothesis_constraint WHERE c >= a" # 4

$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_hypothesis_constant (i UInt64, a UInt64, INDEX t (a < 10) TYPE hypothesis GRANULARITY 1)
ENGINE = MergeTree ORDER BY i SETTINGS index_granularity = 1;
INSERT INTO t_hypothesis_constant VALUES (0, 1), (1, 20), (2, 5), (3, 30);
"

echo "-- a < 10 holds in granules 0 and 2"
rows_read "t_hypothesis_constant WHERE a > 15" # 2
rows_read "t_hypothesis_constant WHERE a = 10" # 2
rows_read "t_hypothesis_constant WHERE a < 3"  # 4

$CLICKHOUSE_CLIENT --query "
DROP TABLE t_hypothesis;
DROP TABLE t_hypothesis_not_equals;
DROP TABLE t_hypothesis_equals;
DROP TABLE t_hypothesis_constraint;
DROP TABLE t_hypothesis_constant;
"
