#include "Logger.h"
#include <Poco/ConsoleChannel.h>
#include <Poco/AutoPtr.h>
#include <Poco/AsyncChannel.h>


using Poco::ConsoleChannel;
using Poco::AutoPtr;
using Poco::AsyncChannel;

void local_engine::Logger::initConsoleLogger()
{
    AutoPtr<ConsoleChannel> pCons(new ConsoleChannel);
    AutoPtr<AsyncChannel> pAsync(new AsyncChannel(pCons));
    Poco::Logger::root().setChannel(pAsync);
    Poco::Logger::root().setLevel("debug");
    Poco::Logger::root().debug("init logger success");
}

