#!/bin/bash
set -e

CH_PATH=${CH_PATH:=clickhouse}

(
  $CH_PATH client -mn -q "
DROP TABLE IF EXISTS ttl_dict;
DROP DICTIONARY IF EXISTS always_alive_ids_dict;
DROP TABLE IF EXISTS always_alive_ids;

CREATE TABLE always_alive_ids (id UInt64) engine=Memory();
INSERT INTO always_alive_ids VALUES (-1);

CREATE DICTIONARY always_alive_ids_dict (id UInt64) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'always_alive_ids')) LAYOUT(HASHED()) LIFETIME(0);
CREATE TABLE ttl_dict (id UInt64, event_date Date) ENGINE = MergeTree ORDER BY (id) TTL event_date + INTERVAL 1 MONTH WHERE NOT dictHas('always_alive_ids_dict', id);

INSERT INTO ttl_dict SETTINGS log_comment='x' VALUES (1, today()-60)(2, today()-60)(3, today()) ;

system flush logs;
"
result=$($CH_PATH client -mn -q "
SELECT has(used_privileges, 'dictGet ON default.always_alive_ids_dict')
FROM system.query_log
WHERE log_comment = 'x' AND type != 'QueryStart';")

echo $result

if [ "$result" = "1" ]; then
  exit 1
else
  exit 0
fi

)