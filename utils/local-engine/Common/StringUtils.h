#pragma once
#include <string>
#include <vector>

namespace local_engine
{
using PartitionValue = std::pair<std::string, std::string>;
using PartitionValues = std::vector<PartitionValue>;

class StringUtils
{
public:
    static PartitionValues parsePartitionTablePath(std::string file);
    static bool isNullPartitionValue(std::string value);
};
}
