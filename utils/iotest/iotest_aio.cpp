#if !defined(OS_LINUX)
int main(int, char **) { return 0; }
#else

#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>
#include <iostream>
#include <iomanip>
#include <vector>
#include <Poco/Exception.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <Common/Stopwatch.h>
#include <Common/randomSeed.h>
#include <pcg_random.hpp>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadHelpers.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <IO/AIO.h>
#include <malloc.h>
#include <sys/syscall.h>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int CANNOT_OPEN_FILE;
        extern const int CANNOT_CLOSE_FILE;
        extern const int CANNOT_IO_SUBMIT;
        extern const int CANNOT_IO_GETEVENTS;
    }
}


enum Mode
{
    MODE_READ = 1,
    MODE_WRITE = 2,
};


void thread(int fd, int mode, size_t min_offset, size_t max_offset, size_t block_size, size_t buffers_count, size_t count)
{
    using namespace DB;

    AIOContext ctx;

    std::vector<Memory<>> buffers(buffers_count);
    for (size_t i = 0; i < buffers_count; ++i)
        buffers[i] = Memory<>(block_size, sysconf(_SC_PAGESIZE));

    pcg64_fast rng(randomSeed());

    size_t in_progress = 0;
    size_t blocks_sent = 0;
    std::vector<bool> buffer_used(buffers_count, false);
    std::vector<iocb> iocbs(buffers_count);
    std::vector<iocb*> query_cbs;
    std::vector<io_event> events(buffers_count);

    while (blocks_sent < count || in_progress > 0)
    {
        /// Prepare queries.
        query_cbs.clear();
        for (size_t i = 0; i < buffers_count; ++i)
        {
            if (blocks_sent >= count || in_progress >= buffers_count)
                break;

            if (buffer_used[i])
                continue;

            buffer_used[i] = true;
            ++blocks_sent;
            ++in_progress;

            char * buf = buffers[i].data();

            uint64_t rand_result1 = rng();
            uint64_t rand_result2 = rng();
            uint64_t rand_result3 = rng();

            size_t rand_result = rand_result1 ^ (rand_result2 << 22) ^ (rand_result3 << 43);
            size_t offset = min_offset + rand_result % ((max_offset - min_offset) / block_size) * block_size;

            iocb & cb = iocbs[i];
            memset(&cb, 0, sizeof(cb));
            cb.aio_buf = reinterpret_cast<UInt64>(buf);
            cb.aio_fildes = fd;
            cb.aio_nbytes = block_size;
            cb.aio_offset = offset;
            cb.aio_data = static_cast<UInt64>(i);

            if (mode == MODE_READ)
            {
                cb.aio_lio_opcode = IOCB_CMD_PREAD;
            }
            else
            {
                cb.aio_lio_opcode = IOCB_CMD_PWRITE;
            }

            query_cbs.push_back(&cb);
        }

        /// Send queries.
        if  (io_submit(ctx.ctx, query_cbs.size(), &query_cbs[0]) < 0)
            throwFromErrno("io_submit failed", ErrorCodes::CANNOT_IO_SUBMIT);

        /// Receive answers. If we have something else to send, then receive at least one answer (after that send them), otherwise wait all answers.
        memset(&events[0], 0, buffers_count * sizeof(events[0]));
        int evs = io_getevents(ctx.ctx, (blocks_sent < count ? 1 : in_progress), buffers_count, &events[0], nullptr);
        if (evs < 0)
            throwFromErrno("io_getevents failed", ErrorCodes::CANNOT_IO_GETEVENTS);

        for (int i = 0; i < evs; ++i)
        {
            int b = static_cast<int>(events[i].data);
            if (events[i].res != static_cast<int>(block_size))
                throw Poco::Exception("read/write error");
            --in_progress;
            buffer_used[b] = false;
        }
    }
}


int mainImpl(int argc, char ** argv)
{
    using namespace DB;

    const char * file_name = nullptr;
    int mode = MODE_READ;
    UInt64 min_offset = 0;
    UInt64 max_offset = 0;
    UInt64 block_size = 0;
    UInt64 buffers_count = 0;
    UInt64 threads_count = 0;
    UInt64 count = 0;

    if (argc != 9)
    {
        std::cerr << "Usage: " << argv[0] << " file_name r|w min_offset max_offset block_size threads buffers count" << std::endl;
        return 1;
    }

    file_name = argv[1];
    if (argv[2][0] == 'w')
        mode = MODE_WRITE;
    min_offset = parse<UInt64>(argv[3]);
    max_offset = parse<UInt64>(argv[4]);
    block_size = parse<UInt64>(argv[5]);
    threads_count = parse<UInt64>(argv[6]);
    buffers_count = parse<UInt64>(argv[7]);
    count = parse<UInt64>(argv[8]);

    int fd = open(file_name, ((mode == MODE_READ) ? O_RDONLY : O_WRONLY) | O_DIRECT);
    if (-1 == fd)
        throwFromErrno("Cannot open file", ErrorCodes::CANNOT_OPEN_FILE);

    ThreadPool pool(threads_count);

    Stopwatch watch;

    for (size_t i = 0; i < threads_count; ++i)
        pool.scheduleOrThrowOnError([=]{ thread(fd, mode, min_offset, max_offset, block_size, buffers_count, count); });
    pool.wait();

    watch.stop();

    if (0 != close(fd))
        throwFromErrno("Cannot close file", ErrorCodes::CANNOT_CLOSE_FILE);

    std::cout << std::fixed << std::setprecision(2)
    << "Done " << count << " * " << threads_count << " ops";
    std::cout << " in " << watch.elapsedSeconds() << " sec."
    << ", " << count * threads_count / watch.elapsedSeconds() << " ops/sec."
    << ", " << count * threads_count * block_size / watch.elapsedSeconds() / 1000000 << " MB/sec."
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
#endif
