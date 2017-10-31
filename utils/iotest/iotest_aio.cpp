#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>

#include <iostream>
#include <iomanip>
#include <vector>

#include <Poco/NumberParser.h>
#include <Poco/NumberFormatter.h>
#include <Poco/Exception.h>

#include <Common/Exception.h>

#include <common/ThreadPool.h>
#include <Common/Stopwatch.h>

#include <IO/BufferWithOwnMemory.h>

#include <stdlib.h>
#if !defined(__APPLE__) && !defined(__FreeBSD__)
#include <malloc.h>
#endif

#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#if !defined(__APPLE__) && !defined(__FreeBSD__)
#include <linux/aio_abi.h>
#endif
#include <sys/syscall.h>

using DB::throwFromErrno;

inline int io_setup(unsigned nr, aio_context_t *ctxp)
{
    return syscall(__NR_io_setup, nr, ctxp);
}

inline int io_destroy(aio_context_t ctx)
{
    return syscall(__NR_io_destroy, ctx);
}

inline int io_submit(aio_context_t ctx, long nr, struct iocb **iocbpp)
{
    return syscall(__NR_io_submit, ctx, nr, iocbpp);
}

inline int io_getevents(aio_context_t ctx, long min_nr, long max_nr,
                        struct io_event *events, struct timespec *timeout)
{
    return syscall(__NR_io_getevents, ctx, min_nr, max_nr, events, timeout);
}

enum Mode
{
    MODE_READ = 1,
    MODE_WRITE = 2,
};


struct AioContext
{
    aio_context_t ctx;

    AioContext()
    {
        ctx = 0;
        if (io_setup(128, &ctx) < 0)
            throwFromErrno("io_setup failed");
    }

    ~AioContext()
    {
        io_destroy(ctx);
    }
};


void thread(int fd, int mode, size_t min_offset, size_t max_offset, size_t block_size, size_t buffers_count, size_t count)
{
    AioContext ctx;

    std::vector<DB::Memory> buffers(buffers_count);
    for (size_t i = 0; i < buffers_count; ++i)
        buffers[i] = DB::Memory(block_size, sysconf(_SC_PAGESIZE));

    drand48_data rand_data;
    timespec times;
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &times);
    srand48_r(times.tv_nsec, &rand_data);

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

            long rand_result1 = 0;
            long rand_result2 = 0;
            long rand_result3 = 0;
            lrand48_r(&rand_data, &rand_result1);
            lrand48_r(&rand_data, &rand_result2);
            lrand48_r(&rand_data, &rand_result3);

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
            throwFromErrno("io_submit failed");

        /// Receive answers. If we have something else to send, then receive at least one answer (after that send them), otherwise wait all answers.
        memset(&events[0], 0, buffers_count * sizeof(events[0]));
        int evs = io_getevents(ctx.ctx, (blocks_sent < count ? 1 : in_progress), buffers_count, &events[0], nullptr);
        if (evs < 0)
            throwFromErrno("io_getevents failed");

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
    const char * file_name = 0;
    int mode = MODE_READ;
    size_t min_offset = 0;
    size_t max_offset = 0;
    size_t block_size = 0;
    size_t buffers_count = 0;
    size_t threads_count = 0;
    size_t count = 0;

    if (argc != 9)
    {
        std::cerr << "Usage: " << argv[0] << " file_name r|w min_offset max_offset block_size threads buffers count" << std::endl;
        return 1;
    }

    file_name = argv[1];
    if (argv[2][0] == 'w')
        mode = MODE_WRITE;
    min_offset = Poco::NumberParser::parseUnsigned64(argv[3]);
    max_offset = Poco::NumberParser::parseUnsigned64(argv[4]);
    block_size = Poco::NumberParser::parseUnsigned64(argv[5]);
    threads_count = Poco::NumberParser::parseUnsigned(argv[6]);
    buffers_count = Poco::NumberParser::parseUnsigned(argv[7]);
    count = Poco::NumberParser::parseUnsigned(argv[8]);

    int fd = open(file_name, ((mode == MODE_READ) ? O_RDONLY : O_WRONLY) | O_DIRECT);
    if (-1 == fd)
        throwFromErrno("Cannot open file");

    ThreadPool pool(threads_count);

    Stopwatch watch;

    for (size_t i = 0; i < threads_count; ++i)
        pool.schedule(std::bind(thread, fd, mode, min_offset, max_offset, block_size, buffers_count, count));
    pool.wait();

    watch.stop();

    if (0 != close(fd))
        throwFromErrno("Cannot close file");

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
