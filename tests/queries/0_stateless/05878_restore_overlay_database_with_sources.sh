#!/usr/bin/env bash

# An `Overlay` database restored together with its source is created after it; without its source it is refused,
# as by `CREATE`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

src="${CLICKHOUSE_DATABASE}_src"
overlay="${CLICKHOUSE_DATABASE}_overlay"
backup_both="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_both')"
backup_overlay="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_overlay')"
backup_identifier="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_identifier')"

${CLICKHOUSE_CLIENT} -m -q "
CREATE DATABASE ${src};
CREATE TABLE ${src}.t (x Int32) ENGINE = MergeTree ORDER BY x;
INSERT INTO ${src}.t VALUES (1);
CREATE DATABASE ${overlay} ENGINE = Overlay('${src}');
BACKUP DATABASE ${src}, DATABASE ${overlay} TO ${backup_both} FORMAT Null;
BACKUP DATABASE ${overlay} TO ${backup_overlay} FORMAT Null;
DROP DATABASE ${overlay};
DROP DATABASE ${src} SYNC;

RESTORE DATABASE ${overlay} FROM ${backup_overlay} FORMAT Null; -- { serverError UNKNOWN_DATABASE }

RESTORE DATABASE ${src}, DATABASE ${overlay} FROM ${backup_both} FORMAT Null;
SELECT x FROM ${overlay}.t;

-- A renamed source is renamed in the arguments of the overlay too, whether written as a literal or an identifier.
RESTORE DATABASE ${src} AS ${src}_renamed, DATABASE ${overlay} AS ${overlay}_renamed FROM ${backup_both} FORMAT Null;
SELECT x FROM ${overlay}_renamed.t;
CREATE DATABASE ${overlay}_identifier ENGINE = Overlay(${src});
BACKUP DATABASE ${src}, DATABASE ${overlay}_identifier TO ${backup_identifier} FORMAT Null;
RESTORE DATABASE ${src} AS ${src}_identifier_renamed, DATABASE ${overlay}_identifier AS ${overlay}_identifier_renamed
    FROM ${backup_identifier} FORMAT Null;
SELECT x FROM ${overlay}_identifier_renamed.t;

DROP DATABASE ${overlay}_identifier_renamed;
DROP DATABASE ${src}_identifier_renamed;
DROP DATABASE ${overlay}_identifier;
DROP DATABASE ${overlay}_renamed;
DROP DATABASE ${src}_renamed;
DROP DATABASE ${overlay};
DROP DATABASE ${src};
"
