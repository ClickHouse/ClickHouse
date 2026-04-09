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

ROOT_PATH="${1:-}"
if [[ -z "$ROOT_PATH" ]]; then
    ROOT_PATH="$(git rev-parse --show-toplevel)"
fi

LIBS_PATH="${ROOT_PATH}/contrib"

# Function to process a single C/C++ library
process_library() {
    local LIB="$1"
    local LIB_NAME=$(basename "$LIB")

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
        (${GREP_CMD} -q -i -F 'MIT License' "$LIB_LICENSE" &&
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
}

# Function to process a single Rust crate
process_rust_crate() {
    local dependency="$1"
    local FOLDER=$(dirname "$dependency")

    # Crate names follow `some-crate-name-1.0.0` pattern.
    CRATE=$(basename "$FOLDER")
    NAME=$(echo "$CRATE" | rev | cut -f2- -d- | rev)

    LICENSE_TYPE=$(${GREP_CMD} 'license = "' "$dependency"  | cut -d '"' -f2)
    if echo "${LICENSE_TYPE}" | ${GREP_CMD} -v -P 'MIT|Apache|MPL|ISC|BSD|Unicode|Zlib|CC0-1.0|CDLA-Permissive|BSL-1.0';
    then
        echo "Fatal error: unrecognized licenses ($LICENSE_TYPE) in the Rust code" >&2
        exit 1
    fi

    LICENSE_PATH=""
    declare -a arr=(
      "LICENSE"
      "LICENCE"
      "LICENSE.md"
      "LICENSE.txt"
      "LICENSE.TXT"
      "COPYING"
      "LICENSE_APACHE"
      "LICENSE-APACHE"
      "license-apache-2.0"
      "LICENSES/Apache-2.0.txt"
      "LICENSE-MIT"
      "LICENSE-MIT.txt"
      "LICENSE-MIT.md"
      "LICENSE.MIT"
      "LICENSE_A2"
      "LICENSE_CC0"
      "LICENSE_A2LLVM"
    )
    for possible_path in "${arr[@]}"
    do
        if test -f "${FOLDER}/${possible_path}";
        then
            LICENSE_PATH="${FOLDER}/${possible_path}"
            break
        fi
    done

    if [ -z "${LICENSE_PATH}" ];
    then
        # It's important that we match exactly the license that we want for those projects that don't include a LICENSE file
        if [ "$LICENSE_TYPE" == "Apache-2.0" ] ||
           [ "$LICENSE_TYPE" == "MIT OR Apache-2.0" ] ||
           [ "$LICENSE_TYPE" == "MIT/Apache-2.0" ] ||
           [ "$LICENSE_TYPE" == "MIT OR Apache-2.0 OR LGPL-2.1-or-later" ] ||
           [ "$LICENSE_TYPE" == "Zlib OR Apache-2.0 OR MIT" ] ||
           [ "$LICENSE_TYPE" == "Apache-2.0 OR BSL-1.0 OR MIT" ] ||
           [ "$LICENSE_TYPE" == "Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT" ] ||
           [ "$LICENSE_TYPE" == "Apache-2.0 WITH LLVM-exception" ] ||
           [ "$LICENSE_TYPE" == "Apache-2.0/MIT" ];
        then
            LICENSE_PATH="/utils/list-licenses/Apache-2.0.txt"
        elif [ "$LICENSE_TYPE" == "BSL-1.0" ]
        then
            # NOTE: this should be Boost Software License not Business Software License (BUSL)
            LICENSE_PATH="/utils/list-licenses/BSL-1.0.txt"
        elif [ "$LICENSE_TYPE" == "MIT" ]
        then
            LICENSE_PATH="/utils/list-licenses/MIT.txt"
        elif [ "$LICENSE_TYPE" == "MPL-2.0" ] || [ "$LICENSE_TYPE" == "MPL-2.0+" ];
        then
            LICENSE_PATH="/utils/list-licenses/MPL-2.0.txt"
        elif [ "$LICENSE_TYPE" == "BSD-3-Clause" ] ||
             [ "$LICENSE_TYPE" == "GPL-2.0-only OR BSD-3-Clause" ];
        then
            LICENSE_PATH="/utils/list-licenses/BSD-3-Clause.txt"
        elif [ "$LICENSE_TYPE" == "ISC" ]
        then
            LICENSE_PATH="/utils/list-licenses/ISC.txt"
        else
            echo "Could not find a valid license file for \"${LICENSE_TYPE}\" in $FOLDER" >&2
            ls "$FOLDER" >&2
            exit 1
        fi
    fi

    RELATIVE_PATH=$(echo "$LICENSE_PATH" | sed -r -e 's!^.+/(contrib|base)/!/\1/!')
    echo -e "$NAME\t$LICENSE_TYPE\t$RELATIVE_PATH"
}

# Export functions and variables for parallel execution
export -f process_library process_rust_crate
export GREP_CMD FIND_CMD ROOT_PATH LIBS_PATH

# Process C/C++ libraries in parallel
libs=$(echo "${ROOT_PATH}/base/poco"; (${FIND_CMD} "${LIBS_PATH}" -mindepth 1 -maxdepth 1 -type d -not -name '*-cmake' -not -name 'rust_vendor' | LC_ALL=C sort) )

# Use xargs for parallel processing (fall back to 4 jobs if nproc not available)
JOBS=$(nproc 2>/dev/null || echo 4)

# Process in parallel (output order is non-deterministic)
echo "$libs" | tr ' ' '\n' | xargs -P ${JOBS} -I {} bash -c 'process_library "$@"' _ {}

${FIND_CMD} "${LIBS_PATH}/rust_vendor/" -name 'Cargo.toml' | \
    xargs -P ${JOBS} -I {} bash -c 'process_rust_crate "$@"' _ {}
