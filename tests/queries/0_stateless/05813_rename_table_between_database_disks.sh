#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree
# Tag no-fasttest: depends on OpenSSL
# Tag no-replicated-database: Replicated databases refuse cross-database moves, custom disk
# Tag no-shared-merge-tree: custom disk

# A cross-database RENAME/EXCHANGE between Atomic databases on different metadata disks is refused.
# ENC_A and ENC_B have no `path`, so all four databases keep their metadata in the directory of BASE.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB=${CLICKHOUSE_DATABASE}
BASE="disk(type = local, path = '${CLICKHOUSE_DISKS_FILES}/${DB}_rename_disks/')"
ENC_A="disk(type = encrypted, disk = ${BASE}, algorithm = 'AES_128_CTR', key_hex = '00112233445566778899aabbccddeeff')"
ENC_B="disk(type = encrypted, disk = ${BASE}, algorithm = 'AES_256_CTR', key_hex = '00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff')"

${CLICKHOUSE_CLIENT} --multiline -q "
DROP DATABASE IF EXISTS ${DB}_plain SYNC;
DROP DATABASE IF EXISTS ${DB}_plain_2 SYNC;
DROP DATABASE IF EXISTS ${DB}_enc_a SYNC;
DROP DATABASE IF EXISTS ${DB}_enc_b SYNC;

CREATE DATABASE ${DB}_plain SETTINGS disk = ${BASE};
CREATE DATABASE ${DB}_plain_2 SETTINGS disk = ${BASE};
CREATE DATABASE ${DB}_enc_a SETTINGS disk = ${ENC_A};
CREATE DATABASE ${DB}_enc_b SETTINGS disk = ${ENC_B};

CREATE TABLE ${DB}_plain.t (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE ${DB}_enc_a.t (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE ${DB}_enc_b.t (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO ${DB}_plain.t VALUES (1);
INSERT INTO ${DB}_enc_a.t VALUES (2);
INSERT INTO ${DB}_enc_b.t VALUES (3);
CREATE DICTIONARY ${DB}_enc_a.d (x UInt32) PRIMARY KEY x SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0);
CREATE DICTIONARY ${DB}_plain.d (x UInt32) PRIMARY KEY x SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0);

RENAME TABLE ${DB}_plain.t TO ${DB}_enc_a.t2; -- { serverError NOT_IMPLEMENTED }
RENAME TABLE ${DB}_enc_a.t TO ${DB}_enc_b.t2; -- { serverError NOT_IMPLEMENTED }
EXCHANGE TABLES ${DB}_plain.t AND ${DB}_enc_a.t; -- { serverError NOT_IMPLEMENTED }
RENAME DICTIONARY ${DB}_enc_a.d TO ${DB}_enc_b.d2; -- { serverError NOT_IMPLEMENTED }
EXCHANGE DICTIONARIES ${DB}_plain.d AND ${DB}_enc_a.d; -- { serverError NOT_IMPLEMENTED }

-- ATTACH re-reads the metadata file through the database's disk.
DETACH TABLE ${DB}_plain.t;
ATTACH TABLE ${DB}_plain.t;
DETACH TABLE ${DB}_enc_a.t;
ATTACH TABLE ${DB}_enc_a.t;
DETACH TABLE ${DB}_enc_b.t;
ATTACH TABLE ${DB}_enc_b.t;
SELECT 'plain', x FROM ${DB}_plain.t;
SELECT 'enc_a', x FROM ${DB}_enc_a.t;
SELECT 'enc_b', x FROM ${DB}_enc_b.t;

-- Databases on the same disk still move tables.
RENAME TABLE ${DB}_plain.t TO ${DB}_plain_2.t;
DETACH TABLE ${DB}_plain_2.t;
ATTACH TABLE ${DB}_plain_2.t;
SELECT 'plain_2', x FROM ${DB}_plain_2.t;

DROP DATABASE ${DB}_plain SYNC;
DROP DATABASE ${DB}_plain_2 SYNC;
DROP DATABASE ${DB}_enc_a SYNC;
DROP DATABASE ${DB}_enc_b SYNC;
"
