#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A MySQL dump with `ENUM` without the list of values, or with non-string values, must be rejected with an exception.

function dump()
{
    printf 'CREATE TABLE `t1` (`x` %s NOT NULL) ENGINE=InnoDB;\n/*!40101 SET character_set_client = @saved_cs_client */;\nINSERT INTO `t1` VALUES (%s);\n' "$1" "$2"
}

for type in 'ENUM' 'ENUM(UInt8)' 'ENUM(1, 2)'; do
    echo "$type"
    dump "$type" 1 | $CLICKHOUSE_LOCAL --input-format MySQLDump --query "SELECT * FROM table" 2>&1 | grep -o -m1 -F 'INCORRECT_DATA'
done

dump "ENUM('a', 'b')" "'b'" | $CLICKHOUSE_LOCAL --input-format MySQLDump --query "SELECT x, toTypeName(x) FROM table"
