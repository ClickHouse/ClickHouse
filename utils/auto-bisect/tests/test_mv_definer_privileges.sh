#!/bin/bash
set -e

CH_PATH=${CH_PATH:=clickhouse}

WORK_TREE="$1"

(
  if [ -n "$WORK_TREE" ]; then
    cd "$WORK_TREE" || exit 129
  fi
  $CH_PATH client --user admin -mn -q "
  drop table if exists source_table;
  drop table if exists target_table;
drop user if exists definer_user;
drop user if exists user;


CREATE TABLE source_table (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE target_table (x UInt32) ENGINE = MergeTree ORDER BY x;


CREATE USER definer_user;
GRANT SELECT ON source_table TO definer_user;
GRANT INSERT ON source_table TO definer_user;
GRANT SELECT ON target_table TO definer_user;
GRANT NONE ON target_table TO definer_user;

CREATE MATERIALIZED VIEW mv TO target_table DEFINER = definer_user SQL SECURITY DEFINER AS SELECT * FROM source_table;
CREATE USER user;

GRANT SELECT ON mv TO user;
GRANT INSERT ON mv TO user;
GRANT SELECT ON source_table TO user;
GRANT INSERT ON source_table TO user;
GRANT SELECT ON target_table TO user;
GRANT INSERT ON target_table TO user;
"

if $CH_PATH client --user user -mn -q "INSERT INTO mv VALUES (10);"; then
    exit 1
else
    exit 0
fi


)