#!/usr/bin/env bash
set -e -x

source default-config

if [[ -d "${WORKSPACE}/sources" ]]; then
    tar -c -z -f "${WORKSPACE}/sources.tar.gz" --directory "${WORKSPACE}/sources" .
else
    die "Run get-sources first"
fi
