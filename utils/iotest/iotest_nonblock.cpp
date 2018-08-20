#include <fcntl.h>
#include <port/unistd.h>
#include <stdlib.h>
#include <time.h>
#include <stdlib.h>
#if !defined(__APPLE__) && !defined(__FreeBSD__)
#include <malloc.h>
#endif
#include <poll.h>
#include <iostream>
#include <iomanip>
#include <vector>
#include <random>
#include <pcg_random.hpp>
#include <IO/ReadHelpers.h>
#include <Poco/Exception.h>
#include <Common/Exception.h>
#include <Common/randomSeed.h>
#include <common/ThreadPool.h>
#include <Common/Stopwatch.h>
#include <port/clock.h>

using DB::throwFromErrno;


enum Mode
{
    MODE_READ,
    MODE_WRITE,
};


int mainImpl(int argc, char ** argv)
{
    const char * file_name = 0;
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
    min_offset = DB::parse<UInt64>(argv[3]);
    max_offset = DB::parse<UInt64>(argv[4]);
    block_size = DB::parse<UInt64>(argv[5]);
    descriptors = DB::parse<UInt64>(argv[6]);
    count = DB::parse<UInt64>(argv[7]);

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
            throwFromErrno("Cannot open file");
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
        if (poll(&polls[0], descriptors, -1) <= 0)
            throwFromErrno("poll failed");
        for (size_t i = 0; i < descriptors; ++i)
        {
            if (!polls[i].revents)
                continue;

            if (polls[i].revents != polls[i].events)
                throw Poco::Exception("revents indicates error");
            polls[i].revents = 0;
            ++ops;

            long rand_result1 = rng();
            long rand_result2 = rng();
            long rand_result3 = rng();

            size_t rand_result = rand_result1 ^ (rand_result2 << 22) ^ (rand_result3 << 43);
            size_t offset;
            offset = min_offset + rand_result % ((max_offset - min_offset) / block_size) * block_size;

            if (mode == MODE_READ)
            {
                if (static_cast<int>(block_size) != pread(fds[i], &buf[0], block_size, offset))
                    throwFromErrno("Cannot read");
            }
            else
            {
                if (static_cast<int>(block_size) != pwrite(fds[i], &buf[0], block_size, offset))
                    throwFromErrno("Cannot write");
            }
        }
    }

    for (size_t i = 0; i < descriptors; ++i)
    {
        if (fsync(fds[i]))
            throwFromErrno("Cannot fsync");
    }

    watch.stop();

    for (size_t i = 0; i < descriptors; ++i)
    {
        if (0 != close(fds[i]))
            throwFromErrno("Cannot close file");
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
