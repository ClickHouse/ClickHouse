#!/usr/bin/env bash
set -e -x

source default-config

[[ -d "${WORKSPACE}/sources" ]] || die "Run get-sources.sh first"

latest_tzdb_version=$(curl -s https://data.iana.org/time-zones/data/version);
tzdb_version_in_repo=$(cat "${WORKSPACE}/sources/contrib/cctz/testdata/version");

if [ "$tzdb_version_in_repo" = "$latest_tzdb_version" ];
then
    echo "No update for TZDB needed";
    exit 0;
else
    echo "TZDB update required! Version in repo is ${tzdb_version_in_repo}, latest version is ${latest_tzdb_version}";
    exit 1
fi;
