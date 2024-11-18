#!/usr/bin/env bash

if [[ "$OSTYPE" == "darwin"* ]]
then
    # use GNU versions, their presence is ensured in cmake/tools.cmake
    GREP_CMD=ggrep
    FIND_CMD=gfind
else
    FIND_CMD='find'
    GREP_CMD='grep'
fi

ROOT_PATH="$(git rev-parse --show-toplevel)"
LIBS_PATH="${ROOT_PATH}/contrib"

mapfile -t libs < <(echo "${ROOT_PATH}/base/poco"; find "${LIBS_PATH}" -maxdepth 1 -type d -not -name '*-cmake' -not -name 'rust_vendor' | LC_ALL=C sort)
for LIB in "${libs[@]}"
do
    LIB_NAME=$(basename "$LIB")

    LIB_LICENSE=$(
        LC_ALL=C ${FIND_CMD} "$LIB" -type f -and '(' -iname 'LICENSE*' -or -iname 'COPYING*' -or -iname 'COPYRIGHT*' -or -iname 'NOTICE' ')' -and -not '(' -iname '*.html' -or -iname '*.htm' -or -iname '*.rtf' -or -name '*.cpp' -or -name '*.h' -or -iname '*.json' ')' -printf "%d\t%p\n" |
            LC_ALL=C sort | LC_ALL=C awk '
                BEGIN { IGNORECASE=1; min_depth = 0 }
                /LICENSE/ { if (!min_depth || $1 <= min_depth) { min_depth = $1; license = $2 } }
                /COPY/    { if (!min_depth || $1 <= min_depth) { min_depth = $1; copying = $2 } }
                /NOTICE/  { if (!min_depth || $1 <= min_depth) { min_depth = $1; notice = $2 } }
                END { if (license) { print license } else if (copying) { print copying } else { print notice } }')

    if [ -n "$LIB_LICENSE" ]
    then
        LICENSE_TYPE=$(
        (${GREP_CMD} -q -F 'Apache' "$LIB_LICENSE" &&
         echo "Apache") ||
        (${GREP_CMD} -q -F 'Boost' "$LIB_LICENSE" &&
         echo "Boost") ||
        (${GREP_CMD} -q -i -P 'public\s*domain|CC0 1\.0 Universal' "$LIB_LICENSE" &&
         echo "Public Domain") ||
        (${GREP_CMD} -q -F 'BSD' "$LIB_LICENSE" &&
         echo "BSD") ||
        (${GREP_CMD} -q -F 'Lesser General Public License' "$LIB_LICENSE" &&
         echo "LGPL") ||
        (${GREP_CMD} -q -F 'General Public License' "$LIB_LICENSE" &&
         echo "GPL") ||
        (${GREP_CMD} -q -i -F 'The origin of this software must not be misrepresented' "$LIB_LICENSE" &&
         ${GREP_CMD} -q -i -F 'Altered source versions must be plainly marked as such' "$LIB_LICENSE" &&
         ${GREP_CMD} -q -i -F 'This notice may not be removed or altered' "$LIB_LICENSE" &&
         echo "zLib") ||
        (${GREP_CMD} -q -i -F 'This program, "bzip2", the associated library "libbzip2"' "$LIB_LICENSE" &&
         echo "bzip2") ||
        (${GREP_CMD} -q -i -F 'Permission is hereby granted, free of charge, to any person' "$LIB_LICENSE" &&
         ${GREP_CMD} -q -i -F 'The above copyright notice and this permission notice shall be' "$LIB_LICENSE" &&
         ${GREP_CMD} -q -i -F 'THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND' "$LIB_LICENSE" &&
         echo "MIT") ||
        (${GREP_CMD} -q -F 'PostgreSQL' "$LIB_LICENSE" &&
         echo "PostgreSQL") ||
        (${GREP_CMD} -q -i -F 'Permission to use, copy, modify, and distribute this software for any purpose' "$LIB_LICENSE" &&
         ${GREP_CMD} -q -i -F 'the name of a copyright holder shall not' "$LIB_LICENSE" &&
         ${GREP_CMD} -q -i -F 'THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND' "$LIB_LICENSE" &&
         echo "MIT/curl") ||
        (${GREP_CMD} -q -i -F 'OpenLDAP Public License' "$LIB_LICENSE" &&
         ${GREP_CMD} -q -i -F 'Version 2.8' "$LIB_LICENSE" &&
         echo "OpenLDAP Version 2.8") ||
        (${GREP_CMD} -q -i -F 'Redistributions of source code must retain the above copyright' "$LIB_LICENSE" &&
         ${GREP_CMD} -q -i -F 'Redistributions in binary form must reproduce' "$LIB_LICENSE" &&
         ${GREP_CMD} -q -i -F 'Neither the name' "$LIB_LICENSE" &&
         echo "BSD 3-clause") ||
        (${GREP_CMD} -q -i -F 'Redistributions of source code must retain the above copyright' "$LIB_LICENSE" &&
         ${GREP_CMD} -q -i -F 'Redistributions in binary form must reproduce' "$LIB_LICENSE" &&
         echo "BSD 2-clause") ||
        (${GREP_CMD} -q -i -F 'Permission to use, copy, modify, and distribute this software' "$LIB_LICENSE" &&
         ${GREP_CMD} -q -i -F 'documentation for any purpose and without fee is hereby granted' "$LIB_LICENSE" &&
         ${GREP_CMD} -q -i -F 'the above copyright notice appear in all copies and that both that copyright' "$LIB_LICENSE" &&
         ${GREP_CMD} -q -i -F 'notice and this permission notice appear in supporting documentation' "$LIB_LICENSE" &&
         ${GREP_CMD} -q -i -F 'not be used in advertising or publicity pertaining' "$LIB_LICENSE" &&
         ${GREP_CMD} -q -i -F 'distribution of the software without specific, written prior permission' "$LIB_LICENSE" &&
         ${GREP_CMD} -q -i -F 'makes no representations about the suitability of this software' "$LIB_LICENSE" &&
         echo "HPND") ||
        echo "Unknown")

        if [ "$LICENSE_TYPE" == "GPL" ]
        then
            echo "Fatal error: General Public License found in ${LIB_NAME}." >&2
            exit 1
        fi

        if [ "$LICENSE_TYPE" == "Unknown" ]
        then
            echo "Fatal error: sources with unknown license found in ${LIB_NAME}." >&2
            exit 1
        fi

        RELATIVE_PATH=$(echo "$LIB_LICENSE" | sed -r -e 's!^.+/(contrib|base)/!/\1/!')

        echo -e "$LIB_NAME\t$LICENSE_TYPE\t$RELATIVE_PATH"
    fi
done

# Special care for Rust
find "${LIBS_PATH}/rust_vendor/" -name 'Cargo.toml' | xargs grep 'license = ' | (grep -v -P 'MIT|Apache|MPL' && echo "Fatal error: unrecognized licenses in the Rust code" >&2 && exit 1 || true)
