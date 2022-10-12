#pragma once

#include <Poco/Logger.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace local_engine
{
class Logger
{
public:
    static void initConsoleLogger(const std::string & level);
    static void initFileLogger(Poco::Util::AbstractConfiguration & config, const std::string & cmd_name);
};
}


