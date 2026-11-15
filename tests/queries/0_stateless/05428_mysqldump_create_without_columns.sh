#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `CREATE TABLE` in a MySQL dump without column definitions (`CREATE TABLE ... LIKE`, or only indexes or constraints)
# gives no structure, so the structure is inferred from the data of the `INSERT`.

function dump()
{
    printf 'CREATE TABLE `t1` %s;\n/*!40101 SET character_set_client = @saved_cs_client */;\nINSERT INTO `t1` VALUES (1,'"'"'a'"'"'),(2,'"'"'b'"'"');\n' "$1"
}

for create in 'LIKE `t0`' '(KEY `k` (`x`)) ENGINE=InnoDB' '(CONSTRAINT `c` CHECK (`x` > 0)) ENGINE=InnoDB'; do
    echo "$create"
    dump "$create" | $CLICKHOUSE_LOCAL --input-format MySQLDump --query "DESCRIBE TABLE table"
    dump "$create" | $CLICKHOUSE_LOCAL --input-format MySQLDump --query "SELECT * FROM table"
done

# The `INSERT` immediately following the `CREATE TABLE` must not be skipped.

function dump_single_insert()
{
    printf 'CREATE TABLE `t1` %s;\nINSERT INTO `t1` VALUES (1,'"'"'a'"'"'),(2,'"'"'b'"'"');\n' "$1"
}

function dump_multiple_inserts()
{
    printf 'CREATE TABLE `t1` %s;\nINSERT INTO `t1` VALUES (1,'"'"'a'"'"');\nINSERT INTO `t1` VALUES (2,'"'"'b'"'"');\nINSERT INTO `t1` VALUES (3,'"'"'c'"'"');\n' "$1"
}

for create in 'LIKE `t0`' '(KEY `k` (`x`)) ENGINE=InnoDB' '(CONSTRAINT `c` CHECK (`x` > 0)) ENGINE=InnoDB' '(`x` int, `y` varchar(10)) ENGINE=InnoDB'; do
    echo "$create"
    dump_single_insert "$create" | $CLICKHOUSE_LOCAL --input-format MySQLDump --query "SELECT * FROM table"
    dump_multiple_inserts "$create" | $CLICKHOUSE_LOCAL --input-format MySQLDump --query "SELECT * FROM table"
    dump_multiple_inserts "$create" | $CLICKHOUSE_LOCAL --input-format MySQLDump --structure 'x UInt32, y String' --query "SELECT * FROM table"
done
