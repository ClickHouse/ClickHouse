#!/usr/bin/env bash

# Check that there are no new translation units compiled to an object file larger than a certain size.

if find $1 -name '*.o' | xargs wc -c | grep -v total | sort -rn | awk '{ if ($1 > 50000000) print }' \
    | grep -v -P 'CastOverloadResolver|AggregateFunctionMax|AggregateFunctionMin|RangeHashedDictionary|Aggregator|AggregateFunctionUniq'
then
    echo "^ It's not allowed to have so large translation units."
    exit 1
fi
