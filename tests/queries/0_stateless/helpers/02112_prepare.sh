#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
FILE=${CURDIR}/../file_02112
echo "drop table if exists t;create table t(i Int32) engine=Memory; insert into t select 1" > "$FILE"
