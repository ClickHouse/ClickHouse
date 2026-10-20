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
