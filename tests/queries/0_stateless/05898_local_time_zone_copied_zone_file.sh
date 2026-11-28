#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# POSIX lets a host name its time zone by a path (`TZ=:<file>`), and an image can copy that file
# instead of linking it into the time zone database. The path of a copy says nothing about the
# zone, so the zone is identified by searching the database for a file with the same contents.
# The alternate copies a database keeps - `posix/` (plain copies of the top-level zones) and
# `right/` (the same zones counting leap seconds) - and its own copy of the selected zone
# (`localtime`) are all skipped, so the name that search finds is the canonical zone name.
#
# `serverTimezone()` reports the zone the server selected - the value of the `timezone` server
# setting, see docs/reference/settings/server-settings/settings/other.mdx - and a `DateTime`
# without an explicit zone is decoded in it.
#
# The zone itself is loaded from the database linked into the binary once its name is known, so
# the files of the private database below only have to be distinguishable from each other.

TZ_ROOT="${CLICKHOUSE_TMP:?}/zoneinfo_${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
rm -rf "${TZ_ROOT}"
mkdir -p "${TZ_ROOT}/db/Europe" "${TZ_ROOT}/db/posix/Europe" "${TZ_ROOT}/db/right/Europe"

echo "zone-of-the-host" > "${TZ_ROOT}/db/Europe/Berlin"
cp "${TZ_ROOT}/db/Europe/Berlin" "${TZ_ROOT}/db/posix/Europe/Berlin"
cp "${TZ_ROOT}/db/Europe/Berlin" "${TZ_ROOT}/db/right/Europe/Berlin"
cp "${TZ_ROOT}/db/Europe/Berlin" "${TZ_ROOT}/db/localtime"
# What the host points `TZ` at: a copy that sits outside the database.
cp "${TZ_ROOT}/db/Europe/Berlin" "${TZ_ROOT}/copy_of_the_zone"
# As long as the zone above, with different contents, so no file of the database holds it.
echo "zone-not-in-tzdb" > "${TZ_ROOT}/unknown_zone"

# A zone that cannot be named is fatal: the server refuses to start rather than guess one. The
# diagnostic it writes names the path it was given, so only the outcome is asserted here.
detect()
{
    env TZDIR="${TZ_ROOT}/db" TZ=":$1" ${CLICKHOUSE_LOCAL} --query "
        SELECT serverTimezone(),
               toDateTime('2026-01-15 12:00:00', serverTimezone()) - toDateTime('2026-01-15 12:00:00', 'UTC')
    " 2>/dev/null || echo "no time zone"
}

# The copy is named after the file of the database that has the same contents, and the zone that
# name selects in the built-in database puts Berlin one hour ahead of UTC in January.
detect "${TZ_ROOT}/copy_of_the_zone"

# A copy whose contents no file of the database holds cannot be named,
detect "${TZ_ROOT}/unknown_zone"

# and neither can a path that does not exist.
detect "${TZ_ROOT}/no_such_file"

rm -rf "${TZ_ROOT}"
