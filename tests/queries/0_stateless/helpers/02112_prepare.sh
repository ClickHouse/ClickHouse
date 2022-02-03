#!/usr/bin/env bash

FILE=${CURDIR}/file_02112
if [ -f $FILE ]; then
    rm $FILE
fi
echo "drop table if exists t;create table t(i Int32) engine=Memory; insert into t select 1" >> $FILE
