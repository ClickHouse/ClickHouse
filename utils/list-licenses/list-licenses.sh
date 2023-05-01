#!/bin/bash

if [[ "$OSTYPE" == "darwin"* ]]; then
    # use GNU versions, their presence is ensured in cmake/tools.cmake
    GREP_CMD=ggrep
    FIND_CMD=gfind
else
    FIND_CMD=find
    GREP_CMD=grep
fi

ROOT_PATH="$(git rev-parse --show-toplevel)"
LIBS_PATH="${ROOT_PATH}/contrib"

ls -1 -d ${LIBS_PATH}/*/ | ${GREP_CMD} -F -v -- '-cmake' | LC_ALL=C sort | while read LIB; do
    LIB_NAME=$(basename $LIB)

    LIB_LICENSE=$(
        LC_ALL=C ${FIND_CMD} "$LIB" -type f -and '(' -iname 'LICENSE*' -or -iname 'COPYING*' -or -iname 'COPYRIGHT*' ')' -and -not '(' -iname '*.html' -or -iname '*.htm' -or -iname '*.rtf' -or -name '*.cpp' -or -name '*.h' -or -iname '*.json' ')' -printf "%d\t%p\n" |
            LC_ALL=C sort | LC_ALL=C awk '
                BEGIN { IGNORECASE=1; min_depth = 0 }
                /LICENSE/ { if (!min_depth || $1 <= min_depth) { min_depth = $1; license = $2 } }
                /COPY/    { if (!min_depth || $1 <= min_depth) { min_depth = $1; copying = $2 } }
                END { if (license) { print license } else { print copying } }')

    if [ -n "$LIB_LICENSE" ]; then

        LICENSE_TYPE=$(
        (${GREP_CMD} -q -F 'Apache' "$LIB_LICENSE" &&
         echo "Apache") ||
        (${GREP_CMD} -q -F 'Boost' "$LIB_LICENSE" &&
         echo "Boost") ||
        (${GREP_CMD} -q -i -P 'public\s*domain' "$LIB_LICENSE" &&
         echo "Public Domain") ||
        (${GREP_CMD} -q -F 'BSD' "$LIB_LICENSE" &&
         echo "BSD") ||
        (${GREP_CMD} -q -F 'Lesser General Public License' "$LIB_LICENSE" &&
         echo "LGPL") ||
        (${GREP_CMD} -q -i -F 'The origin of this software must not be misrepresented' "$LIB_LICENSE" &&
         ${GREP_CMD} -q -i -F 'Altered source versions must be plainly marked as such' "$LIB_LICENSE" &&
         ${GREP_CMD} -q -i -F 'This notice may not be removed or altered' "$LIB_LICENSE" &&
         echo "zLib") ||
        (${GREP_CMD} -q -i -F 'Permission is hereby granted, free of charge, to any person' "$LIB_LICENSE" &&
         ${GREP_CMD} -q -i -F 'The above copyright notice and this permission notice shall be included' "$LIB_LICENSE" &&
         ${GREP_CMD} -q -i -F 'THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND' "$LIB_LICENSE" &&
         echo "MIT") ||
        (${GREP_CMD} -q -i -F 'Permission to use, copy, modify, and distribute this software for any purpose' "$LIB_LICENSE" &&
         ${GREP_CMD} -q -i -F 'the name of a copyright holder shall not' "$LIB_LICENSE" &&
         ${GREP_CMD} -q -i -F 'THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND' "$LIB_LICENSE" &&
         echo "MIT/curl") ||
        (${GREP_CMD} -q -i -F 'Redistributions of source code must retain the above copyright' "$LIB_LICENSE" &&
         ${GREP_CMD} -q -i -F 'Redistributions in binary form must reproduce' "$LIB_LICENSE" &&
         ${GREP_CMD} -q -i -F 'Neither the name' "$LIB_LICENSE" &&
         echo "BSD 3-clause") ||
        (${GREP_CMD} -q -i -F 'Redistributions of source code must retain the above copyright' "$LIB_LICENSE" &&
         ${GREP_CMD} -q -i -F 'Redistributions in binary form must reproduce' "$LIB_LICENSE" &&
         echo "BSD 2-clause") ||
        echo "Unknown")

        RELATIVE_PATH=$(echo "$LIB_LICENSE" | sed -r -e 's!^.+/contrib/!/contrib/!')

        echo -e "$LIB_NAME\t$LICENSE_TYPE\t$RELATIVE_PATH"
    fi
done
