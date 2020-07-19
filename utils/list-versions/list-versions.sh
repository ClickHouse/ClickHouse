#!/bin/bash

git tag --list | grep -P 'v.+-(stable|lts)' | sort -V | xargs git show --format='%ai' --no-patch | awk '/^v/ { version = $1 } /^[0-9]+/ { date = $1 } { if (version && date) { print version "\t" date; version = ""; date = ""; } }' | tac
