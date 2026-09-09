/// Runtime checks for the parts of the Windows port that `clickhouse.exe local --query "SELECT 1"`
/// does not reach. See the comment in CMakeLists.txt next to this file for why they live in a
/// separate executable rather than in a unit test.
///
/// Every check must be self-contained and finish in milliseconds: this runs inside a build job.
/// A check that regresses has to *fail*, not hang, so the socket checks bound their own waiting
/// with a socket timeout - see `dontWaitOnIdleSocketReportsWouldBlock`.

#include <Poco/Net/Net.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Timespan.h>

#include <base/getMemoryAmount.h>

#include <Poco/UnWindows.h>

#include <cstdint>
#include <exception>
#include <iostream>
#include <optional>
#include <string>


namespace
{

int failures = 0;

void check(bool condition, const std::string & what)
{
    std::cout << (condition ? "PASS " : "FAIL ") << what << "\n";
    if (!condition)
        ++failures;
}

/// A connected loopback pair, both ends left in the blocking mode Winsock gives them, which is
/// the mode the checks below are about.
struct SocketPair
{
    Poco::Net::ServerSocket server{Poco::Net::SocketAddress("127.0.0.1", 0)};
    Poco::Net::StreamSocket client{server.address()};
    Poco::Net::StreamSocket accepted{server.acceptConnection()};
};


/// Winsock has no per-call `MSG_DONTWAIT`; ClickHouse's Poco defines the flag anyway and strips
/// it before the syscall, so without the emulation in `SocketImpl::emulatedDontWaitWouldBlock`
/// this call reaches a plain blocking `recv` on an idle socket and waits.
///
/// The receive timeout is what keeps a regression from hanging the build job: a blocking `recv`
/// that inherits `SO_RCVTIMEO` gives up with `WSAETIMEDOUT`, which Poco turns into
/// `TimeoutException` - a failure of this check rather than a job that never ends.
void dontWaitOnIdleSocketReportsWouldBlock()
{
    SocketPair pair;
    pair.client.setBlocking(true);
    pair.client.setReceiveTimeout(Poco::Timespan(5, 0));

    char buffer[16] = {};
    try
    {
        const int received = pair.client.receiveBytes(buffer, sizeof(buffer), MSG_DONTWAIT);
        const int error = WSAGetLastError();
        check(received < 0 && error == WSAEWOULDBLOCK,
            "MSG_DONTWAIT on an idle blocking socket reports would-block "
            "(received " + std::to_string(received) + ", error " + std::to_string(error) + ")");
    }
    catch (const std::exception & e)
    {
        check(false, std::string("MSG_DONTWAIT on an idle blocking socket reports would-block, but it threw: ") + e.what());
    }
}

/// The other half of the contract: the emulation must not report would-block for a socket that
/// is ready. A zero-timeout readiness probe that got its polarity wrong would still pass the
/// check above.
void dontWaitOnReadySocketReceives()
{
    SocketPair pair;
    pair.client.setBlocking(true);
    pair.client.setReceiveTimeout(Poco::Timespan(5, 0));

    static constexpr char message[] = "windows";
    pair.accepted.sendBytes(message, sizeof(message));

    /// Loopback delivery is not instantaneous; wait for the data the same way any reader would.
    check(pair.client.poll(Poco::Timespan(5, 0), Poco::Net::Socket::SELECT_READ), "the sent bytes arrive within five seconds");

    char buffer[sizeof(message)] = {};
    try
    {
        const int received = pair.client.receiveBytes(buffer, sizeof(buffer), MSG_DONTWAIT);
        check(received == static_cast<int>(sizeof(message)) && std::string(buffer) == message,
            "MSG_DONTWAIT on a ready blocking socket still receives (received " + std::to_string(received) + ")");
    }
    catch (const std::exception & e)
    {
        check(false, std::string("MSG_DONTWAIT on a ready blocking socket still receives, but it threw: ") + e.what());
    }
}

/// The same emulation is applied to the sending side, where a socket with room in its send
/// buffer must not be reported as would-block either.
void dontWaitOnWritableSocketSends()
{
    SocketPair pair;
    pair.client.setBlocking(true);
    pair.client.setSendTimeout(Poco::Timespan(5, 0));

    static constexpr char message[] = "selftest";
    try
    {
        const int sent = pair.client.sendBytes(message, sizeof(message), MSG_DONTWAIT);
        check(sent == static_cast<int>(sizeof(message)),
            "MSG_DONTWAIT on a writable blocking socket still sends (sent " + std::to_string(sent) + ")");
    }
    catch (const std::exception & e)
    {
        check(false, std::string("MSG_DONTWAIT on a writable blocking socket still sends, but it threw: ") + e.what());
    }
}

/// The decision `getMemoryAmountOrZero` makes about a job object, exercised directly. It cannot
/// be exercised through the operating system here: Wine stubs
/// `JobObjectExtendedLimitInformation`, zeroing the caller's struct and reporting success (see
/// `NtQueryInformationJobObject` in Wine's `dlls/ntdll/unix/sync.c`), so a limit set with
/// `SetInformationJobObject` is never read back. That stub is also exactly the input the last
/// two rows below stand for.
void jobObjectMemoryLimitIsDecidedFromTheFlags()
{
    static constexpr uint64_t job_limit = 512ull << 20;
    static constexpr uint64_t process_limit = 256ull << 20;

    check(windowsJobObjectMemoryLimit(0, job_limit, process_limit) == std::nullopt,
        "no limit flag means no limit, whatever the limit fields hold");
    check(windowsJobObjectMemoryLimit(JOB_OBJECT_LIMIT_JOB_MEMORY, job_limit, process_limit) == job_limit,
        "the job-wide limit is used when only its flag is set");
    check(windowsJobObjectMemoryLimit(JOB_OBJECT_LIMIT_PROCESS_MEMORY, job_limit, process_limit) == process_limit,
        "the per-process limit is used when only its flag is set");
    check(windowsJobObjectMemoryLimit(JOB_OBJECT_LIMIT_JOB_MEMORY | JOB_OBJECT_LIMIT_PROCESS_MEMORY, job_limit, process_limit) == process_limit,
        "the smaller of the two limits wins when both flags are set");
    check(windowsJobObjectMemoryLimit(JOB_OBJECT_LIMIT_JOB_MEMORY | JOB_OBJECT_LIMIT_PROCESS_MEMORY, process_limit, job_limit) == process_limit,
        "the smaller of the two limits wins regardless of which field it is in");
    check(windowsJobObjectMemoryLimit(JOB_OBJECT_LIMIT_WORKINGSET, job_limit, process_limit) == std::nullopt,
        "an unrelated limit flag does not make the memory fields meaningful");
}

/// What the process actually reports, against what the operating system says about its job.
/// Under Wine the job reports nothing and this only proves the new query does not disturb the
/// answer - which is the regression that matters, since reading `JobMemoryLimit` without
/// checking its flag would clamp the amount to the zero Wine's stub leaves behind. On a real
/// Windows container, where the job does report a limit, it proves the clamp.
void memoryAmountRespectsTheJobObject()
{
    const uint64_t amount = getMemoryAmountOrZero();
    check(amount > 0, "the memory amount is known (" + std::to_string(amount) + " bytes)");

    JOBOBJECT_EXTENDED_LIMIT_INFORMATION info{};
    if (!QueryInformationJobObject(nullptr, JobObjectExtendedLimitInformation, &info, sizeof(info), nullptr))
    {
        std::cout << "INFO this process is not in a job object that can be queried (error "
                  << GetLastError() << "), so there is no limit to respect\n";
        return;
    }

    const std::optional<uint64_t> limit = windowsJobObjectMemoryLimit(
        info.BasicLimitInformation.LimitFlags,
        static_cast<uint64_t>(info.JobMemoryLimit),
        static_cast<uint64_t>(info.ProcessMemoryLimit));

    if (!limit.has_value())
    {
        std::cout << "INFO the job object imposes no memory limit (flags " << info.BasicLimitInformation.LimitFlags
                  << "), so there is no limit to respect\n";
        return;
    }

    check(amount <= *limit,
        "the memory amount stays within the job object limit (" + std::to_string(amount) + " <= " + std::to_string(*limit) + ")");
}

}


int main(int, char **)
{
    /// `WSAStartup`. Nothing in this executable goes through the `main` of `clickhouse.exe`,
    /// which is where the server does it.
    Poco::Net::initializeNetwork();

    dontWaitOnIdleSocketReportsWouldBlock();
    dontWaitOnReadySocketReceives();
    dontWaitOnWritableSocketSends();
    jobObjectMemoryLimitIsDecidedFromTheFlags();
    memoryAmountRespectsTheJobObject();

    std::cout << (failures ? "FAILED: " + std::to_string(failures) + " check(s)\n" : "OK\n");
    return failures ? 1 : 0;
}
