#!/usr/bin/env bash

export LC_ALL=C # The "total" should be printed without localization

# Check that there are no new translation units compiled to an object file larger than a certain size.

TU_EXCLUDES=(
    AggregateFunctionUniq
    Aggregator
)

if find $1 -name '*.o' | xargs wc -c | grep --regexp='\.o$' | sort -rn | awk '{ if ($1 > 50000000) print }' \
    | grep -v -f <(printf "%s\n" "${TU_EXCLUDES[@]}")
then
    echo "^ It's not allowed to have so large translation units."
    echo "  To bypass this check, configure the build with -DCHECK_LARGE_OBJECT_SIZES=0"
    exit 1
fi
