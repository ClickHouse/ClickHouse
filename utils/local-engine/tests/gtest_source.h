//
// Created by saber on 3/17/22.
//

#ifndef CLICKHOUSE_GTEST_SOURCE_H
#define CLICKHOUSE_GTEST_SOURCE_H

typedef unsigned long ulong;

ulong inc(ulong x);

ulong inline inc_inline(ulong x)
{
    return x + 1;
}


#endif //CLICKHOUSE_GTEST_SOURCE_H
