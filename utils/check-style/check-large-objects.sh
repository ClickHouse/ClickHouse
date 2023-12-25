#!/usr/bin/env bash

# Check that there are no new translation units compiled to an object file larger than a certain size.

TU_EXCLUDES=(
    CastOverloadResolver
    AggregateFunctionMax
    AggregateFunctionMin
    AggregateFunctionUniq
    FunctionsConversion

    RangeHashedDictionary

    Aggregator
)

if find $1 -name '*.o' | xargs wc -c | grep -v total | sort -rn | awk '{ if ($1 > 50000000) print }' \
    | grep -v -f <(printf "%s\n" "${TU_EXCLUDES[@]}")
then
    echo "^ It's not allowed to have so large translation units."
    exit 1
fi
