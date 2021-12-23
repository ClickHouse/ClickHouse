#!/usr/bin/env bash

FILE=${CURDIR}/file_02112
if [ -f $FILE ]; then
    rm $FILE
fi
