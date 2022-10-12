#include "StringUtils.h"
#include <Poco/StringTokenizer.h>
#include <filesystem>

namespace local_engine
{
PartitionValues StringUtils::parsePartitionTablePath(const std::string & file)
{
    PartitionValues result;
    Poco::StringTokenizer path(file, "/");
    for (const auto & item : path)
    {
        auto position = item.find('=');
        if (position != std::string::npos)
        {
            result.emplace_back(PartitionValue(item.substr(0,position), item.substr(position+1)));
        }
    }
    return result;
}
bool StringUtils::isNullPartitionValue(const std::string & value)
{
    return value == "__HIVE_DEFAULT_PARTITION__";
}
}


