#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadHelpers.h>
#include <pcg_random.hpp>
#include <Poco/Exception.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <Common/randomSeed.h>
#include <base/getPageSize.h>

#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <random>
#include <vector>

#include <fcntl.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int CANNOT_OPEN_FILE;
        extern const int CANNOT_CLOSE_FILE;
        extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
        extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
    }
}


enum Mode
{
    MODE_NONE = 0,
    MODE_READ = 1,
    MODE_WRITE = 2,
    MODE_ALIGNED = 4,
    MODE_DIRECT = 8,
    MODE_SYNC = 16,
};


void thread(int fd, int mode, size_t min_offset, size_t max_offset, size_t block_size, size_t count)
{
    using namespace DB;

    Memory<> direct_buf(block_size, ::getPageSize());
    std::vector<char> simple_buf(block_size);

    char * buf;
    if ((mode & MODE_DIRECT))
        buf = direct_buf.data();
    else
        buf = &simple_buf[0];

    pcg64 rng(randomSeed());

    for (size_t i = 0; i < count; ++i)
    {
        uint64_t rand_result1 = rng();
        uint64_t rand_result2 = rng();
        uint64_t rand_result3 = rng();

        size_t rand_result = rand_result1 ^ (rand_result2 << 22) ^ (rand_result3 << 43);
        size_t offset;
        if ((mode & MODE_DIRECT) || (mode & MODE_ALIGNED))
            offset = min_offset + rand_result % ((max_offset - min_offset) / block_size) * block_size;
        else
            offset = min_offset + rand_result % (max_offset - min_offset - block_size + 1);

        if (mode & MODE_READ)
        {
            if (static_cast<int>(block_size) != pread(fd, buf, block_size, offset))
                throwFromErrno("Cannot read", ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);
        }
        else
        {
            if (static_cast<int>(block_size) != pwrite(fd, buf, block_size, offset))
                throwFromErrno("Cannot write", ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR);
        }
    }
}


int mainImpl(int argc, char ** argv)
{
    using namespace DB;

    const char * file_name = nullptr;
    int mode = MODE_NONE;
    UInt64 min_offset = 0;
    UInt64 max_offset = 0;
    UInt64 block_size = 0;
    UInt64 threads = 0;
    UInt64 count = 0;

    if (argc != 8)
    {
        std::cerr << "Usage: " << argv[0] << " file_name (r|w)[a][d][s] min_offset max_offset block_size threads count" << std::endl <<
                     "a - aligned, d - direct, s - sync" << std::endl;
        return 1;
    }

    file_name = argv[1];
    min_offset = parse<UInt64>(argv[3]);
    max_offset = parse<UInt64>(argv[4]);
    block_size = parse<UInt64>(argv[5]);
    threads = parse<UInt64>(argv[6]);
    count = parse<UInt64>(argv[7]);

    for (int i = 0; argv[2][i]; ++i)
    {
        char c = argv[2][i];
        switch (c)
        {
            case 'r':
                mode |= MODE_READ;
                break;
            case 'w':
                mode |= MODE_WRITE;
                break;
            case 'a':
                mode |= MODE_ALIGNED;
                break;
            case 'd':
                mode |= MODE_DIRECT;
                break;
            case 's':
                mode |= MODE_SYNC;
                break;
            default:
                throw Poco::Exception("Invalid mode");
        }
    }

    ThreadPool pool(threads);

    #ifndef __APPLE__
    int fd = open(file_name, ((mode & MODE_READ) ? O_RDONLY : O_WRONLY) | ((mode & MODE_DIRECT) ? O_DIRECT : 0) | ((mode & MODE_SYNC) ? O_SYNC : 0));
    #else
    int fd = open(file_name, ((mode & MODE_READ) ? O_RDONLY : O_WRONLY) | ((mode & MODE_SYNC) ? O_SYNC : 0));
    #endif
    if (-1 == fd)
        throwFromErrno("Cannot open file", ErrorCodes::CANNOT_OPEN_FILE);
    #ifdef __APPLE__
    if (mode & MODE_DIRECT)
        if (fcntl(fd, F_NOCACHE, 1) == -1)
            throwFromErrno("Cannot open file", ErrorCodes::CANNOT_CLOSE_FILE);
    #endif
    Stopwatch watch;

    for (size_t i = 0; i < threads; ++i)
        pool.scheduleOrThrowOnError([=]{ thread(fd, mode, min_offset, max_offset, block_size, count); });
    pool.wait();

    #if defined(OS_DARWIN)
        fsync(fd);
    #else
        fdatasync(fd);
    #endif

    watch.stop();

    if (0 != close(fd))
        throwFromErrno("Cannot close file", ErrorCodes::CANNOT_CLOSE_FILE);

    std::cout << std::fixed << std::setprecision(2)
        << "Done " << count << " * " << threads << " ops";
    if (mode & MODE_ALIGNED)
        std::cout << " (aligned)";
    if (mode & MODE_DIRECT)
        std::cout << " (direct)";
    if (mode & MODE_SYNC)
        std::cout << " (sync)";
    std::cout << " in " << watch.elapsedSeconds() << " sec."
        << ", " << count * threads / watch.elapsedSeconds() << " ops/sec."
        << ", " << count * threads * block_size / watch.elapsedSeconds() / 1000000 << " MB/sec."
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
