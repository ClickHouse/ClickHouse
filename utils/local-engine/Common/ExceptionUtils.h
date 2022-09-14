#pragma once
#include <base/logger_useful.h>
#include <Common/Exception.h>

namespace local_engine
{
class ExceptionUtils
{
public:
    static void handleException(const DB::Exception & exception);
};
}
