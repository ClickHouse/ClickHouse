#include "QueryContext.h"
#include <Interpreters/Context.h>
#include <Parser/SerializedPlanParser.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace CurrentMemoryTracker
{
extern thread_local std::function<void(Int64, bool)> before_alloc;
extern thread_local std::function<void(Int64)> before_free;
}

namespace local_engine
{
using namespace DB;
thread_local std::weak_ptr<CurrentThread::QueryScope> query_scope;
thread_local std::weak_ptr<ThreadStatus> thread_status;
std::unordered_map<int64_t, NativeAllocatorContextPtr> allocator_map;
std::mutex allocator_lock;

int64_t initializeQuery(ReservationListenerWrapperPtr listener)
{
    auto query_context = Context::createCopy(SerializedPlanParser::global_context);
    query_context->makeQueryContext();
    auto allocator_context = std::make_shared<NativeAllocatorContext>();
    allocator_context->thread_status = std::make_shared<ThreadStatus>();
    allocator_context->query_scope = std::make_shared<CurrentThread::QueryScope>(query_context);
    allocator_context->query_context = query_context;
    allocator_context->listener = listener;
    thread_status = std::weak_ptr<ThreadStatus>(allocator_context->thread_status);
    query_scope = std::weak_ptr<CurrentThread::QueryScope>(allocator_context->query_scope);
    auto allocator_id = reinterpret_cast<int64_t>(allocator_context.get());
    CurrentMemoryTracker::before_alloc = [listener](Int64 size, bool throw_if_memory_exceed) -> void
    {
        if (throw_if_memory_exceed)
            listener->reserveOrThrow(size);
        else
            listener->reserve(size);
    };
    CurrentMemoryTracker::before_free = [listener](Int64 size) -> void { listener->free(size); };
    {
        std::lock_guard lock{allocator_lock};
        allocator_map.emplace(allocator_id, allocator_context);
    }
    return allocator_id;
}

void releaseAllocator(int64_t allocator_id)
{
    std::lock_guard lock{allocator_lock};
    if (!allocator_map.contains(allocator_id))
    {
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "allocator {} not found", allocator_id);
    }
    auto status = allocator_map.at(allocator_id)->thread_status;
    auto listener = allocator_map.at(allocator_id)->listener;
    if (status->untracked_memory < 0)
        listener->free(-status->untracked_memory);
    allocator_map.erase(allocator_id);
}

NativeAllocatorContextPtr getAllocator(int64_t allocator)
{
    return allocator_map.at(allocator);
}

int64_t allocatorMemoryUsage(int64_t allocator_id)
{
    return allocator_map.at(allocator_id)->thread_status->memory_tracker.get();
}

}
