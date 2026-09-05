/// Minimal runtime for the standalone WebAssembly build of the SQL parser.
///
/// The real implementations of these live in `Common/Exception.cpp`, `Common/StackTrace.cpp`,
/// `Common/MemoryTracker.cpp` and friends. They are deliberately NOT built here:
///
///   * stack traces need libunwind, DWARF parsing and a patched libc++
///     (`STD_EXCEPTION_HAS_STACK_TRACE`, which is 0 here),
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

#include <wasm_sjlj.h>

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

constinit FiberLocal<VariableContext, FiberLocalSlot::MEMORY_TRACKER_BLOCKER_LEVEL, /* default_value = */ VariableContext::Max> MemoryTrackerBlockerInThread::level;
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

constinit FiberLocal<uint64_t, FiberLocalSlot::LOCK_MEMORY_EXCEPTION_COUNTER> LockMemoryExceptionInThread::counter;
constinit FiberLocal<VariableContext, FiberLocalSlot::LOCK_MEMORY_EXCEPTION_LEVEL> LockMemoryExceptionInThread::level;
constinit FiberLocal<bool, FiberLocalSlot::LOCK_MEMORY_EXCEPTION_BLOCK_FAULT_INJECTIONS> LockMemoryExceptionInThread::block_fault_injections;
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

#include <Poco/DateTimeParser.h>
#include <Poco/Process.h>
#include <Poco/Thread.h>
#include <Poco/Timezone.h>
#include <Poco/UnicodeConverter.h>

namespace Poco
{
    ProcessImpl::PIDImpl ProcessImpl::idImpl() { return 1; }
    ThreadImpl * ThreadImpl::currentImpl() { return nullptr; }

    /// `Poco::LocalDateTime` (a conversion target of `Poco::Dynamic::Var`, which reads the JSON
    /// handed to `ch_format_json`) asks the local timezone; the real implementation reads
    /// `tzset`/`tzname`, which wasi-libc does not have. A browser sandbox is UTC.
    int Timezone::utcOffset() { return 0; }
    int Timezone::dst() { return 0; }
    bool Timezone::isDst(const Timestamp &) { return false; }
    int Timezone::tzd() { return 0; }
    std::string Timezone::name() { return "UTC"; }
    std::string Timezone::standardName() { return "UTC"; }
    std::string Timezone::dstName() { return "UTC"; }

    /// Named by `VarHolderImpl` conversions nothing here performs - AST JSON deserialization
    /// converts a `Var` to strings, numbers and booleans, never to a `DateTime` or a UTF-16
    /// string - but virtual `convert` overloads survive the link whether or not they are called.
    /// Both answers are the fail-closed ones: "the string did not parse as a date", which the
    /// caller turns into a `Poco::RangeException`, and a thrown `Poco::NotImplementedException`,
    /// both of which the error boundary below reports as an error.
    bool DateTimeParser::tryParse(const std::string &, const std::string &, DateTime &, int &)
    {
        return false;
    }

    void UnicodeConverter::convert(const std::string &, UTF16String &)
    {
        throw NotImplementedException("There is no UTF-16 conversion in WebAssembly");
    }
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

/// `Interpreters/Context.cpp` is not built either, and these two are defined there. There is no
/// server context in a browser, so they stay null and every `Context::getGlobalContextInstance`
/// in the parser - the `storage_shared_set_join_use_inner_uuid` lookup in `CreateQueryUUIDs`, the
/// access-control lookups in `AccessRightsElement` - takes its contextless branch.
ContextPtr ContextData::global_context_instance;
ContextPtr ContextData::background_context_instance;

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
/// acceptable. So every exported entry point arms a `setjmp` boundary and a throw returns to it,
/// with the message, as a parse failure.
///
/// The unwinding this replaces would have run destructors; `longjmp` does not, so a throw leaks
/// whatever the parser allocated below the boundary. The alternative is `-fwasm-exceptions`, which
/// costs 262 KB and an engine implementing the exception-handling proposal - too much to pay for
/// tidiness on a path that a browser hits only on an invalid query.
///
/// Recovery is for anything deriving from `Poco::Exception` and nothing else. The object arriving
/// at `__cxa_throw` is untyped, so its dynamic type has to be established from the `type_info`
/// argument before it can be read as anything; a `std::bad_alloc` from `operator new` is an
/// unrelated object, so only its type name can be reported, and the module stops. `DB::Exception`
/// is the exception the parser throws; the rest of the Poco hierarchy is thrown by
/// `Poco::JSON::Parser` and the `readJSON` overrides below `IAST::createFromJSON`, whose input -
/// the document handed to `ch_format_json` - is arbitrary, so a malformed one must come back as
/// an error rather than stop the module. (`createFromJSON` does contain `catch` blocks converting
/// Poco exceptions, but `-fignore-exceptions` emits no landing pads, so they never run here.)
///
/// There is no `dynamic_cast`-style hierarchy walk available - libc++abi's machinery is not linked
/// - but the Itanium RTTI data is present and self-describing: the `type_info` of a class with a
/// single public non-virtual base is an `abi::__si_class_type_info` holding a pointer to the
/// base's `type_info`, and every Poco exception is such a class. wasi-sdk exposes neither the
/// class (its `cxxabi.h` ends at the `__cxa_*` entry points) nor its `type_info` (so a `typeid`
/// comparison would not link), which is why the walk below matches the metatype by its mangled
/// name and reads the base pointer through a hand-declared layout.
///
/// The boundary itself is in `wasm_sjlj.c`, which is not part of the LTO unit; see the comment
/// there for why the two calls cannot live in this file.
/// ---------------------------------------------------------------------------------------------

namespace
{

/// The Itanium ABI layout of `abi::__si_class_type_info`: the `std::type_info` part (a vptr and
/// the mangled name), then the base class's `type_info`.
struct SiClassTypeInfoLayout
{
    void * vptr;
    const char * type_name;
    const std::type_info * base_type;
};

bool derivesFromPocoException(const std::type_info * type)
{
    /// Bounded, though no real hierarchy comes close: an RTTI graph is acyclic, but this reads
    /// ABI internals and must not loop forever if it misreads them.
    for (size_t depth = 0; type && depth < 16; ++depth)
    {
        if (*type == typeid(Poco::Exception))
            return true;

        /// Single public non-virtual inheritance is all the Poco hierarchy uses; a class whose
        /// `type_info` is anything else (`__class_type_info` for a root such as `std::bad_alloc`,
        /// `__vmi_class_type_info` for multiple or virtual bases) is not a Poco exception.
        if (strcmp(typeid(*type).name(), "N10__cxxabiv120__si_class_type_infoE") != 0)
            return false;

        type = reinterpret_cast<const SiClassTypeInfoLayout *>(type)->base_type;
    }
    return false;
}

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

[[noreturn]] void __cxa_throw(void * thrown, void * type_info, void (*)(void *))
{
    const auto * thrown_type = static_cast<const std::type_info *>(type_info);

    if (thrown_type && derivesFromPocoException(thrown_type))
    {
        /// A single-inheritance chain keeps the base at offset zero, so the cast is sound.
        const auto * exception = static_cast<const Poco::Exception *>(thrown);

        /// A `DB::Exception` message is self-contained; a Poco one is often empty or a detail
        /// ("Unexpected token", the offending string) that needs the exception's name to mean
        /// anything. Formatted without allocating: an allocation failing here would throw again.
        const char * message;
        static char composed[1024];
        if (*thrown_type == typeid(DB::Exception))
        {
            message = exception->what();
        }
        else
        {
            std::snprintf(composed, sizeof(composed), "%s: %s", exception->name(), exception->message().c_str());
            message = composed;
        }

        if (chParserRecoveryArmed())
            chParserLongjmp(message);

        std::fprintf(stderr, "ClickHouse parser: unrecoverable error: %s\n", message);
        std::abort();
    }

    /// The object cannot be read, so the type name is all there is to report.
    std::fprintf(stderr, "ClickHouse parser: unrecoverable error of type %s\n", thrown_type ? thrown_type->name() : "unknown");
    std::abort();
}

}
