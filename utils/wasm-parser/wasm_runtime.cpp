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

#include <cstddef>
#include <cstdint>
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
/// pads or unwind tables are emitted, so nothing can be caught. Defining `__cxa_throw` here keeps
/// libc++abi's exception machinery out of the bundle entirely; the exception object is constructed
/// and then we stop.
///
/// A syntax error is not an exception - `tryParseQuery` reports it by returning null - but an
/// exception is not confined to bugs either: `IParser::Pos` throws `TOO_DEEP_RECURSION` and
/// `TOO_SLOW` when a query exceeds the depth or backtracking limit, and a few parsers throw on
/// input they have already committed to, so ordinary input can reach this. There is nothing to
/// unwind to, so the module traps - but it records the message first, and a trap leaves linear
/// memory intact, so the embedder can read `ch_error_data`/`ch_error_size` and then instantiate
/// the module again. See `utils/wasm-parser/README.md`.
/// ---------------------------------------------------------------------------------------------

namespace
{

/// Written from `__cxa_throw`, which must not allocate - the throw may be `std::bad_alloc` - and
/// must not assume the dynamic type of the thrown object.
char last_error[1024];
uint32_t last_error_size = 0;

}

extern "C"
{

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

const char * ch_error_data()
{
    return last_error;
}

uint32_t ch_error_size()
{
    return last_error_size;
}

[[noreturn]] void __cxa_throw(void * thrown, void * type_info, void (*)(void *))
{
    /// Only `DB::Exception` may be read as such: the thrown object is untyped here, and everything
    /// else - `std::bad_alloc` from `operator new`, a `Poco` exception from a `Poco::Net` address
    /// parser - would be a reinterpretation of an unrelated object. For those, the type name is all
    /// that can be reported safely. It is not a `dynamic_cast`, so a hypothetical class derived
    /// from `DB::Exception` also falls into the second case rather than into undefined behavior.
    const auto * thrown_type = static_cast<const std::type_info *>(type_info);
    const char * message = thrown_type && *thrown_type == typeid(DB::Exception)
        ? static_cast<const DB::Exception *>(thrown)->what()
        : (thrown_type ? thrown_type->name() : "unknown exception");

    size_t length = std::strlen(message);
    if (length > sizeof(last_error) - 1)
        length = sizeof(last_error) - 1;
    std::memcpy(last_error, message, length);
    last_error[length] = 0;
    last_error_size = static_cast<uint32_t>(length);

    std::fprintf(stderr, "ClickHouse parser: unrecoverable error: %s\n", message);

    /// `__builtin_trap` rather than `std::abort`: a trap returns control to the embedder with the
    /// instance's memory still readable, while `abort` may end the WASI process instead.
    __builtin_trap();
}

}
