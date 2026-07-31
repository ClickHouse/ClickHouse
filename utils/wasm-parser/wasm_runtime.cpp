/// Minimal runtime for the standalone WebAssembly build of the SQL parser.
///
/// The real implementations of these live in `Common/Exception.cpp`, `Common/StackTrace.cpp`,
/// `Common/MemoryTracker.cpp` and friends. They are deliberately NOT built here:
///
///   * stack traces need libunwind, DWARF parsing and a patched libc++
///     (`Common/Exception.cpp` static_asserts on `STD_EXCEPTION_HAS_STACK_TRACE`),
///   * memory tracking and thread status need thread-local server bookkeeping,
///   * logging needs Poco's channel/formatter machinery.
///
/// None of that is meaningful in a browser, and all of it dominates the bundle size, so the
/// WebAssembly build substitutes the no-op versions below.

#include <Common/Exception.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/logger_useful.h>
#include <Common/CurrentMemoryTracker.h>
#include <Core/Settings.h>

#include <csetjmp>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <string>
#include <typeinfo>

namespace DB
{

bool terminate_on_any_exception = false;


std::atomic_bool abort_on_logical_error = false;

thread_local bool Exception::enable_job_stack_trace = false;
thread_local bool Exception::can_use_thread_frame_pointers = false;
thread_local Exception::ThreadFramePointers Exception::thread_frame_pointers;
const Exception::ThreadFramePointersBase Exception::dummy_frame_pointers{};
std::function<void(std::string_view, int, bool, const Exception::Trace &)> Exception::callback;

Exception::ThreadFramePointers::ThreadFramePointers() = default;
Exception::ThreadFramePointers::~ThreadFramePointers() = default;

const Exception::ThreadFramePointersBase & Exception::getThreadFramePointers()
{
    return dummy_frame_pointers;
}

void Exception::setThreadFramePointers(ThreadFramePointersBase)
{
}

void Exception::clearThreadFramePointers()
{
}

/// No query masking rules exist in a browser, so the message is passed through unchanged.
Exception::MessageMasked::MessageMasked(const std::string & msg_, std::string format_string_)
    : msg(msg_), format_string(std::move(format_string_))
{
}

Exception::MessageMasked::MessageMasked(std::string && msg_, std::string format_string_)
    : msg(std::move(msg_)), format_string(std::move(format_string_))
{
}

Exception::Exception(const MessageMasked & msg_masked, int code, bool remote_)
    : Poco::Exception(msg_masked.msg, code), remote(remote_), message_format_string(msg_masked.format_string)
{
}

Exception::Exception(MessageMasked && msg_masked, int code, bool remote_)
    : Poco::Exception(std::move(msg_masked.msg), code), remote(remote_), message_format_string(msg_masked.format_string)
{
}

Exception::~Exception() = default;

void Exception::addMessage(const MessageMasked & msg_masked)
{
    extendedMessage(msg_masked.msg);
}

std::string Exception::getStackTraceString() const
{
    return {};
}

Exception::Trace Exception::getStackFramePointers() const
{
    return {};
}

bool Exception::isErrorCodeImportant() const
{
    return false;
}

template Exception::Exception(int, FormatStringHelperImpl<>);

/// `Core/Settings.cpp` is not built: one call (`ParserSetQuery` asking whether a bare `SET x`
/// names a Bool setting) would otherwise pull in the whole settings schema - every
/// `SettingField*Traits` specialization - which dwarfs the parser itself. Answering
/// "yes, it could be a Bool" keeps `SET x` parsing; the server still validates the name.
Field Settings::castValueUtil(std::string_view, const Field & value)
{
    return value;
}

/// Logging is not wired up: nothing consumes it in a browser.
bool currentThreadHasGroup()
{
    return false;
}

LogsLevel currentThreadLogsLevel()
{
    return LogsLevel::none;
}

}

/// Memory accounting is a server concern; in WebAssembly `malloc` is the only budget there is.
AllocationTrace CurrentMemoryTracker::alloc(Int64) { return AllocationTrace(0.0); }
AllocationTrace CurrentMemoryTracker::allocNoThrow(Int64) { return AllocationTrace(0.0); }
AllocationTrace CurrentMemoryTracker::allocThrow(Int64) { return AllocationTrace(0.0); }
AllocationTrace CurrentMemoryTracker::free(Int64) { return AllocationTrace(0.0); }
void CurrentMemoryTracker::check() {}
void CurrentMemoryTracker::injectFault() {}

void AllocationTrace::onAllocImpl(void *, size_t) const {}
void AllocationTrace::onFreeImpl(void *, size_t) const {}

thread_local constinit VariableContext MemoryTrackerBlockerInThread::level = VariableContext::Global;
MemoryTrackerBlockerInThread::MemoryTrackerBlockerInThread(VariableContext) {}
MemoryTrackerBlockerInThread::MemoryTrackerBlockerInThread(MemoryTrackerBlockerInThread &&) noexcept = default;
MemoryTrackerBlockerInThread & MemoryTrackerBlockerInThread::operator=(MemoryTrackerBlockerInThread &&) noexcept = default;
void MemoryTrackerBlockerInThread::reset() {}
MemoryTrackerBlockerInThread::~MemoryTrackerBlockerInThread() {}

namespace ProfileEvents
{
    void incrementForLogMessage(int) {}
    void incrementLoggerElapsedNanoseconds(UInt64) {}
}

/// ---------------------------------------------------------------------------------------------
/// Server-side facilities the parser links against but never meaningfully uses in a browser.
/// ---------------------------------------------------------------------------------------------

#include <Common/CurrentThread.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/StackTrace.h>
#include <Common/filesystemHelpers.h>
#include <IO/ReadHelpers.h>
#include <base/getPageSize.h>

thread_local constinit uint64_t LockMemoryExceptionInThread::counter = 0;
thread_local constinit VariableContext LockMemoryExceptionInThread::level = VariableContext::Global;
thread_local constinit bool LockMemoryExceptionInThread::block_fault_injections = false;
LockMemoryExceptionInThread::LockMemoryExceptionInThread(VariableContext, bool)
    : previous_level(VariableContext::Global), previous_block_fault_injections(false) {}
LockMemoryExceptionInThread::~LockMemoryExceptionInThread() = default;

/// WebAssembly cannot walk its own call stack from user code.
StackTrace::StackTrace() = default;
std::string StackTrace::toString() const { return {}; }

/// The timezone database is generated into the binary by `contrib/cctz-cmake` and is far larger
/// than the parser; the WebAssembly build leaves it out and reports no timezone data.
std::string_view getTimeZone(const char *) { return {}; }

/// `if_nametoindex`/`if_indextoname` are named by Poco::Net::IPAddress for scoped IPv6 addresses.
extern "C" unsigned int if_nametoindex(const char *) { return 0; }
extern "C" char * if_indextoname(unsigned int, char * name) { return name; }

#include <Poco/Process.h>
#include <Poco/Thread.h>

namespace Poco
{
    ProcessImpl::PIDImpl ProcessImpl::idImpl() { return 1; }
    ThreadImpl * ThreadImpl::currentImpl() { return nullptr; }
}

namespace ProfileEvents
{
    void incrementForLogMessage(Poco::Message::Priority) {}
}

namespace FS
{
    bool isSymlink(const std::filesystem::path &) { return false; }
    std::filesystem::path readSymlink(const std::filesystem::path &) { return {}; }
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int LOGICAL_ERROR;
}

void throwReadAfterEOF()
{
    throw Exception(ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF, "Attempt to read after EOF");
}

/// Query masking rules are configured on the server; there is nothing to mask here.
std::string wipeSensitiveDataAndCutToLength(std::string str, size_t max_length, bool)
{
    if (max_length && str.size() > max_length)
        str.resize(max_length);
    return str;
}

void tryLogCurrentException(const char *, const std::string &, LogsLevel) {}
void tryLogCurrentException(Poco::Logger *, const std::string &, LogsLevel) {}
void tryLogCurrentException(LoggerPtr, const std::string &, LogsLevel) {}

bool CurrentThread::isInitialized() { return false; }

}

/// ---------------------------------------------------------------------------------------------
/// The last few server entry points reached from `Access/Common` and `base/Decimal`.
/// ---------------------------------------------------------------------------------------------

#include <Access/AccessControl.h>
#include <Common/ThreadStatus.h>
#include <Interpreters/Context.h>
#include <base/throwError.h>

void throwError(const char * err)
{
    throw DB::Exception::createDeprecated(err, DB::ErrorCodes::LOGICAL_ERROR);
}

namespace DB
{

/// There is no server, therefore no access control: `AccessRightsElement` formatting falls back
/// to its defaults, which is what a client-side formatter wants anyway.
bool AccessControl::isEnabledUserNameAccessType() const { return false; }
bool AccessControl::isEnabledReadWriteGrants() const { return false; }

ThreadStatus & CurrentThread::get()
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no current thread status in WebAssembly");
}

ContextPtr ThreadStatus::tryGetQueryContext() const { return nullptr; }

const AccessControl & Context::getAccessControl() const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no access control in WebAssembly");
}

/// No timezone database is linked in (see `getTimeZone` above), so nothing validates.
void SettingFieldTimezone::validateTimezone(const std::string &) {}

/// `DateLUT` reads the session timezone out of the settings; without a server context it never
/// gets that far, but the reference still has to resolve.
namespace Setting
{
    extern const SettingsTimezone session_timezone;
    const SettingsTimezone session_timezone{};
}

const SettingFieldTimezone & Settings::operator[](SettingsTimezone) const
{
    static const SettingFieldTimezone empty{};
    return empty;
}

}

/// ---------------------------------------------------------------------------------------------
/// Exceptions.
///
/// The build uses `-fignore-exceptions`: `throw`, `try` and `catch` still compile, but no landing
/// pads or unwind tables are emitted, so nothing can be caught, and defining `__cxa_throw` here
/// keeps libc++abi's exception machinery out of the bundle entirely.
///
/// A syntax error does not come this way - `tryParseQuery` returns null and fills in the message,
/// and `src/Parsers` contains no `catch` at all, which a style check enforces. But a handful of
/// checks in the parser still report an invalid query by throwing (`Frame start cannot be
/// UNBOUNDED FOLLOWING`, for one), and stopping the module on an ordinary user mistake is not
/// acceptable. So `ch_check` and `ch_format` arm a `setjmp` boundary and a throw returns to it,
/// with the message, as a parse failure.
///
/// The unwinding this replaces would have run destructors; `longjmp` does not, so a throw leaks
/// whatever the parser allocated below the boundary. The alternative is `-fwasm-exceptions`, which
/// costs 262 KB and an engine implementing the exception-handling proposal - too much to pay for
/// tidiness on a path that a browser hits only on an invalid query.
///
/// Recovery is for `DB::Exception` and nothing else. The object arriving at `__cxa_throw` is
/// untyped, so its dynamic type has to be established from the `type_info` argument before it can
/// be read as anything; a `std::bad_alloc` from `operator new` or a `Poco` exception is an
/// unrelated object, and reading it through a `Poco::Exception` pointer would be undefined. For
/// those only the type name can be reported, and the module stops. The comparison is exact rather
/// than a `dynamic_cast` - there is no RTTI hierarchy walk available here - so a hypothetical class
/// derived from `DB::Exception` also lands in the second case, which is the safe direction.
/// ---------------------------------------------------------------------------------------------

namespace
{

jmp_buf recovery_point;
bool recovery_armed = false;

/// Not a `std::string`: filling this in must not allocate, or a throw from the allocation would
/// re-enter `__cxa_throw`.
char recovery_message[1024];

}

extern "C"
{

jmp_buf * chParserRecoveryPoint()
{
    return &recovery_point;
}

void chParserArmRecovery(bool armed)
{
    recovery_armed = armed;
}

const char * chParserRecoveryMessage()
{
    return recovery_message;
}

void * __cxa_allocate_exception(size_t size) noexcept
{
    /// The exception object is constructed in place here, so the storage has to be aligned for any
    /// type that can be thrown, not just for `char`.
    alignas(std::max_align_t) static char buffer[512];
    return size <= sizeof(buffer) ? static_cast<void *>(buffer) : nullptr;
}

void __cxa_free_exception(void *) noexcept
{
}

[[noreturn]] void __cxa_throw(void * thrown, void * type_info, void (*)(void *))
{
    const auto * thrown_type = static_cast<const std::type_info *>(type_info);

    if (thrown_type && *thrown_type == typeid(DB::Exception))
    {
        /// `DB::Exception` derives from `Poco::Exception`, whose `what()` is the message.
        const char * message = static_cast<const DB::Exception *>(thrown)->what();

        if (recovery_armed)
        {
            /// Disarm first: this is the only path that can be re-entered.
            recovery_armed = false;

            size_t length = std::strlen(message);
            if (length > sizeof(recovery_message) - 1)
                length = sizeof(recovery_message) - 1;
            std::memcpy(recovery_message, message, length);
            recovery_message[length] = 0;

            longjmp(recovery_point, 1);
        }

        std::fprintf(stderr, "ClickHouse parser: unrecoverable error: %s\n", message);
        std::abort();
    }

    /// The object cannot be read, so the type name is all there is to report.
    std::fprintf(stderr, "ClickHouse parser: unrecoverable error of type %s\n", thrown_type ? thrown_type->name() : "unknown");
    std::abort();
}

}
