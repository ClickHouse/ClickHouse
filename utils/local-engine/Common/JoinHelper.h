#pragma once
#include <string>

namespace local_engine
{
struct JoinOptimizationInfo
{
    bool is_broadcast;
    bool is_null_aware_anti_join;
    std::string storage_join_key;
};


JoinOptimizationInfo parseJoinOptimizationInfo(std::string optimization);


}


