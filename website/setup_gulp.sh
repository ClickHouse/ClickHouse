#!/usr/bin/env bash
set -ex
grep require gulpfile.js | awk -F\' '{print $2;}' | xargs npm install
