#!/bin/bash

ROOT_PATH="$(git rev-parse --show-toplevel)"
LIBS_PATH="${ROOT_PATH}/contrib"

ls -1 -d ${LIBS_PATH}/*/ | grep -F -v -- '-cmake' | while read LIB; do
    LIB_NAME=$(basename $LIB)

    LIB_LICENSE=$(
        LC_ALL=C find "$LIB" -type f -and '(' -iname 'LICENSE*' -or -iname 'COPYING*' -or -iname 'COPYRIGHT*' ')' -and -not -iname '*.html' -printf "%d\t%p\n" |
            awk '
                BEGIN { IGNORECASE=1; min_depth = 0 }
                /LICENSE/ { if (!min_depth || $1 <= min_depth) { min_depth = $1; license = $2 } }
                /COPY/    { if (!min_depth || $1 <= min_depth) { min_depth = $1; copying = $2 } }
                END { if (license) { print license } else { print copying } }')

    if [ -n "$LIB_LICENSE" ]; then

        LICENSE_TYPE=$(
        (grep -q -F 'Apache' "$LIB_LICENSE" &&
         echo "Apache") ||
        (grep -q -F 'Boost' "$LIB_LICENSE" &&
         echo "Boost") ||
        (grep -q -i -P 'public\s*domain' "$LIB_LICENSE" &&
         echo "Public Domain") ||
        (grep -q -F 'BSD' "$LIB_LICENSE" &&
         echo "BSD") ||
        (grep -q -F 'Lesser General Public License' "$LIB_LICENSE" &&
         echo "LGPL") ||
        (grep -q -i -F 'The origin of this software must not be misrepresented' "$LIB_LICENSE" &&
         grep -q -i -F 'Altered source versions must be plainly marked as such' "$LIB_LICENSE" &&
         grep -q -i -F 'This notice may not be removed or altered' "$LIB_LICENSE" &&
         echo "zLib") ||
        (grep -q -i -F 'Permission is hereby granted, free of charge, to any person' "$LIB_LICENSE" &&
         grep -q -i -F 'The above copyright notice and this permission notice shall be included' "$LIB_LICENSE" &&
         grep -q -i -F 'THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND' "$LIB_LICENSE" &&
         echo "MIT") ||
        (grep -q -i -F 'Permission to use, copy, modify, and distribute this software for any purpose' "$LIB_LICENSE" &&
         grep -q -i -F 'the name of a copyright holder shall not' "$LIB_LICENSE" &&
         grep -q -i -F 'THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND' "$LIB_LICENSE" &&
         echo "MIT/curl") ||
        (grep -q -i -F 'Redistributions of source code must retain the above copyright' "$LIB_LICENSE" &&
         grep -q -i -F 'Redistributions in binary form must reproduce' "$LIB_LICENSE" &&
         grep -q -i -F 'Neither the name' "$LIB_LICENSE" &&
         echo "BSD 3-clause") ||
        (grep -q -i -F 'Redistributions of source code must retain the above copyright' "$LIB_LICENSE" &&
         grep -q -i -F 'Redistributions in binary form must reproduce' "$LIB_LICENSE" &&
         echo "BSD 2-clause") ||
        echo "Unknown")

        RELATIVE_PATH=$(echo "$LIB_LICENSE" | sed -r -e 's!^.+/contrib/!/contrib/!')

        echo -e "$LIB_NAME\t$LICENSE_TYPE\t$RELATIVE_PATH"
    fi
done
