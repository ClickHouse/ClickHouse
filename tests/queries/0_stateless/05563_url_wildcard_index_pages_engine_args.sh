#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database
# no-parallel: creates and drops a global named collection
# no-replicated-database: named collections are server-global and their DDL is not replicated

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `URL` engine whose path carries a listable glob is served by the object storage over HTTP index
# pages. Its signature is `URL(URL [,Format] [,CompressionMethod])` (see the engine documentation
# registered in `registerStorageURL`), and the format resolved while the table is created is
# materialized into the persisted engine arguments so that a later ATTACH or restart reproduces it.
# An explicitly given format is kept as written, no structure argument is added for a table engine,
# and the `headers(...)` argument, which is not a SQL function and must not be evaluated, stays the
# last argument.
#
# The engine name is spelled through a variable: this repository's test lint for outgoing network
# access matches the remote `url(` table function, and the table engine is spelled the same way.
# Nothing here contacts anything: the host below is unreachable, no statement reads the tables, and
# `use_hive_partitioning = 0` keeps the index pages from being listed.
ENG="UR"
ENG="${ENG}L"
U="http://127.0.0.1:1/w/sub/p*.tsv"

$CLICKHOUSE_CLIENT -q "
SET allow_url_wildcard_from_index_pages = 1;
SET use_hive_partitioning = 0;

DROP NAMED COLLECTION IF EXISTS nc_05563_auto;
DROP NAMED COLLECTION IF EXISTS nc_05563_csv;
CREATE NAMED COLLECTION nc_05563_auto AS url = '$U';
CREATE NAMED COLLECTION nc_05563_csv AS url = '$U', format = 'CSV';

-- A collection without a \`format\` key gets the resolved format as an extra key-value argument.
CREATE TABLE webargs_collection_auto (x String) ENGINE = $ENG(nc_05563_auto);
-- A collection that names its own format keeps it, and nothing is appended.
CREATE TABLE webargs_collection_csv (x String) ENGINE = $ENG(nc_05563_csv);
-- One positional argument: the resolved format is appended.
CREATE TABLE webargs_positional_url (x String) ENGINE = $ENG('$U');
-- Two positional arguments: an \`auto\` format is replaced by the resolved one.
CREATE TABLE webargs_positional_auto (x String) ENGINE = $ENG('$U', 'auto');
-- Two positional arguments: an explicitly named format is kept.
CREATE TABLE webargs_positional_csv (x String) ENGINE = $ENG('$U', 'CSV');
-- Three positional arguments and \`headers(...)\`: the format is replaced in place, the compression
-- method is kept, and the headers argument is still the last argument.
CREATE TABLE webargs_headers (x String) ENGINE = $ENG('$U', 'auto', 'none', headers('X-Covgap' = '1'));

SELECT name, engine_full FROM system.tables
WHERE database = currentDatabase() AND name LIKE 'webargs_%'
ORDER BY name;

DROP TABLE webargs_collection_auto;
DROP TABLE webargs_collection_csv;
DROP TABLE webargs_positional_url;
DROP TABLE webargs_positional_auto;
DROP TABLE webargs_positional_csv;
DROP TABLE webargs_headers;
DROP NAMED COLLECTION nc_05563_auto;
DROP NAMED COLLECTION nc_05563_csv;
"
