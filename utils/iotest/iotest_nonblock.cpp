#include <IO/ReadHelpers.h>
#include <pcg_random.hpp>
#include <Poco/Exception.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <Common/randomSeed.h>

#include <iomanip>
#include <iostream>
#include <random>
#include <vector>

#include <fcntl.h>
#include <poll.h>
#include <cstdlib>
#include <ctime>
#include <unistd.h>

#if defined (OS_LINUX)
#   include <malloc.h>
#endif


namespace DB
{
    namespace ErrorCodes
    {
        extern const int CANNOT_OPEN_FILE;
        extern const int CANNOT_CLOSE_FILE;
        extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
        extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
        extern const int CANNOT_FSYNC;
        extern const int SYSTEM_ERROR;
    }
}


enum Mode
{
    MODE_READ,
    MODE_WRITE,
};


int mainImpl(int argc, char ** argv)
{
    using namespace DB;

    const char * file_name = nullptr;
    Mode mode = MODE_READ;
    UInt64 min_offset = 0;
    UInt64 max_offset = 0;
    UInt64 block_size = 0;
    UInt64 descriptors = 0;
    UInt64 count = 0;

    if (argc != 8)
    {
        std::cerr << "Usage: " << argv[0] << " file_name r|w min_offset max_offset block_size descriptors count" << std::endl;
        return 1;
    }

    file_name = argv[1];
    min_offset = parse<UInt64>(argv[3]);
    max_offset = parse<UInt64>(argv[4]);
    block_size = parse<UInt64>(argv[5]);
    descriptors = parse<UInt64>(argv[6]);
    count = parse<UInt64>(argv[7]);

    if (!strcmp(argv[2], "r"))
        mode = MODE_READ;
    else if (!strcmp(argv[2], "w"))
        mode = MODE_WRITE;
    else
        throw Poco::Exception("Invalid mode");

    std::vector<int> fds(descriptors);
    for (size_t i = 0; i < descriptors; ++i)
    {
        fds[i] = open(file_name, O_SYNC | ((mode == MODE_READ) ? O_RDONLY : O_WRONLY));
        if (-1 == fds[i])
            throwFromErrno("Cannot open file", ErrorCodes::CANNOT_OPEN_FILE);
    }

    std::vector<char> buf(block_size);

    pcg64 rng(randomSeed());

    Stopwatch watch;

    std::vector<pollfd> polls(descriptors);

    for (size_t i = 0; i < descriptors; ++i)
    {
        polls[i].fd = fds[i];
        polls[i].events = (mode == MODE_READ) ? POLLIN : POLLOUT;
        polls[i].revents = 0;
    }

    size_t ops = 0;
    while (ops < count)
    {
        if (poll(polls.data(), descriptors, -1) <= 0)
            throwFromErrno("poll failed", ErrorCodes::SYSTEM_ERROR);
        for (size_t i = 0; i < descriptors; ++i)
        {
            if (!polls[i].revents)
                continue;

            if (polls[i].revents != polls[i].events)
                throw Poco::Exception("revents indicates error");
            polls[i].revents = 0;
            ++ops;

            uint64_t rand_result1 = rng();
            uint64_t rand_result2 = rng();
            uint64_t rand_result3 = rng();

            size_t rand_result = rand_result1 ^ (rand_result2 << 22) ^ (rand_result3 << 43);
            size_t offset;
            offset = min_offset + rand_result % ((max_offset - min_offset) / block_size) * block_size;

            if (mode == MODE_READ)
            {
                if (static_cast<int>(block_size) != pread(fds[i], buf.data(), block_size, offset))
                    throwFromErrno("Cannot read", ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);
            }
            else
            {
                if (static_cast<int>(block_size) != pwrite(fds[i], buf.data(), block_size, offset))
                    throwFromErrno("Cannot write", ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR);
            }
        }
    }

    for (size_t i = 0; i < descriptors; ++i)
    {
#if defined(OS_DARWIN)
        if (fsync(fds[i]))
            throwFromErrno("Cannot fsync", ErrorCodes::CANNOT_FSYNC);
#else
        if (fdatasync(fds[i]))
            throwFromErrno("Cannot fdatasync", ErrorCodes::CANNOT_FSYNC);
#endif
    }

    watch.stop();

    for (size_t i = 0; i < descriptors; ++i)
    {
        if (0 != close(fds[i]))
            throwFromErrno("Cannot close file", ErrorCodes::CANNOT_CLOSE_FILE);
    }

    std::cout << std::fixed << std::setprecision(2)
    << "Done " << count  << " ops" << " in " << watch.elapsedSeconds() << " sec."
    << ", " << count / watch.elapsedSeconds() << " ops/sec."
    << ", " << count * block_size / watch.elapsedSeconds() / 1000000 << " MB/sec."
    << std::endl;

    return 0;
}


int main(int argc, char ** argv)
{
    try
    {
        return mainImpl(argc, argv);
    }
    catch (const Poco::Exception & e)
    {
        std::cerr << e.what() << ", " << e.message() << std::endl;
        return 1;
    }
}
