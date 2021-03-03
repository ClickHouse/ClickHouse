#!/bin/bash

# This script searches for ya.make.in files in repository and generates ya.make files from them.
# ya.make.in is a template with substitutions in form of <? command ?>
# command is interpreted by bash and output is put in place of substitution

ROOT_PATH=$(git rev-parse --show-toplevel)
EXCLUDE_DIRS='build/|integration/|widechar_width/|glibc-compatibility/|memcpy/|consistent-hashing'

find "${ROOT_PATH}" -name 'ya.make.in' | while read path; do
    echo "# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it." > "${path/.in/}"
    (cd $(dirname "${path}") && perl -pne 's/<\?(.+?)\?>/`$1`/e' < "${path}" >> "${path/.in/}")
done
