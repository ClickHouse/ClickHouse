#include "JoinHelper.h"
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>

using namespace DB;

namespace local_engine
{

JoinOptimizationInfo parseJoinOptimizationInfo(std::string optimization)
{
    JoinOptimizationInfo info;
    ReadBufferFromString in(optimization);
    assertString("JoinParameters:", in);
    assertString("isBHJ=", in);
    readBoolText(info.is_broadcast, in);
    assertChar('\n', in);
    if (info.is_broadcast)
    {
        assertString("isNullAwareAntiJoin=", in);
        readBoolText(info.is_null_aware_anti_join, in);
        assertChar('\n', in);
        assertString("buildHashTableId=", in);
        readString(info.storage_join_key, in);
        assertChar('\n', in);
    }
    return info;
}
}
