#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: named collections are stored in SQL, which the fast test does not set up.
# no-replicated-database: SQL named collections are local to a server, so the replicas of the test database cannot resolve the collection when they execute the replicated `CREATE DICTIONARY`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A dictionary created with DDL registers its dependency on a named collection only when it is loaded.
# `DETACH DICTIONARY` (and `DETACH DATABASE`) of a dictionary that was never loaded must still keep the
# collection protected: otherwise the dictionary fails to load with `NAMED_COLLECTION_DOESNT_EXIST`
# after it is attached back.

NC="nc_${CLICKHOUSE_DATABASE}"
DB2="${CLICKHOUSE_DATABASE}_2"

${CLICKHOUSE_CLIENT} -m -q "
CREATE TABLE src (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
INSERT INTO src VALUES (1, 'one');

CREATE NAMED COLLECTION ${NC} AS
    host = 'localhost',
    port = ${CLICKHOUSE_PORT_TCP},
    user = 'default',
    password = '',
    db = '${CLICKHOUSE_DATABASE}',
    table = 'src';

CREATE DICTIONARY d (id UInt64, value String)
PRIMARY KEY id
SOURCE(CLICKHOUSE(NAME ${NC}))
LAYOUT(FLAT())
LIFETIME(0)
SETTINGS(dictionary_lazy_load = 1);

SELECT status FROM system.dictionaries WHERE database = currentDatabase() AND name = 'd';
DETACH DICTIONARY d;
"

echo "detached dictionary"
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION ${NC}" 2>&1 | grep -o -m1 "NAMED_COLLECTION_IS_USED"

${CLICKHOUSE_CLIENT} -m -q "
ATTACH DICTIONARY d;
SELECT dictGet('${CLICKHOUSE_DATABASE}.d', 'value', toUInt64(1));
DROP DICTIONARY d;

CREATE DATABASE ${DB2} ENGINE = Atomic;
CREATE DICTIONARY ${DB2}.d (id UInt64, value String)
PRIMARY KEY id
SOURCE(CLICKHOUSE(NAME ${NC}))
LAYOUT(FLAT())
LIFETIME(0)
SETTINGS(dictionary_lazy_load = 1);
DETACH DATABASE ${DB2};
"

echo "detached database"
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION ${NC}" 2>&1 | grep -o -m1 "NAMED_COLLECTION_IS_USED"

${CLICKHOUSE_CLIENT} -m -q "
ATTACH DATABASE ${DB2};
SELECT dictGet('${DB2}.d', 'value', toUInt64(1));
DROP DATABASE ${DB2};
DROP NAMED COLLECTION ${NC};
SELECT count() FROM system.named_collections WHERE name = '${NC}';
"
