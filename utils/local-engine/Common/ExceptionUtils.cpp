#include "ExceptionUtils.h"

using namespace DB;

namespace local_engine
{
void ExceptionUtils::handleException(const Exception & exception)
{
    LOG_ERROR(&Poco::Logger::get("ExceptionUtils"), "{}\n{}", exception.message(), exception.getStackTraceString());
    exception.rethrow();
}

}
