#!/usr/bin/env bash
set -ex
grep require gulpfile.js | awk -F\' '{print $2;}' | xargs npm install

# npm is assumed to be already installed,
# if it's not you can do so with 'apt-get install npm' for Debian/Ubuntu,
# 'brew install npm' for Mac OS or manual download:
# https://nodejs.org/en/download/
