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
#include <Poco/SharedPtr.h>

#include <statdaemons/threadpool.hpp>
#include <statdaemons/Stopwatch.h>


enum Mode
{
	MODE_READ,
	MODE_WRITE,
};


typedef Poco::SharedPtr<Poco::Exception> ExceptionPtr;


void throwFromErrno(const std::string & s)
{
	char buf[128];
	throw Poco::Exception(s + ", errno: " + Poco::NumberFormatter::format(errno)
		+ ", strerror: " + std::string(strerror_r(errno, buf, sizeof(buf))));
}


void thread(int fd, Mode mode, size_t min_offset, size_t max_offset, size_t block_size, size_t count, ExceptionPtr & exception)
{
	try
	{
		std::vector<char> buf(block_size);
		drand48_data rand_data;

		timespec times;
		clock_gettime(CLOCK_THREAD_CPUTIME_ID, &times);
		srand48_r(times.tv_nsec, &rand_data);

		for (size_t i = 0; i < count; ++i)
		{
			long rand_result1 = 0;
			long rand_result2 = 0;
			long rand_result3 = 0;
			lrand48_r(&rand_data, &rand_result1);
			lrand48_r(&rand_data, &rand_result2);
			lrand48_r(&rand_data, &rand_result3);

			size_t rand_result = rand_result1 ^ (rand_result2 << 22) ^ (rand_result3 << 43);
			size_t offset = min_offset + rand_result % (max_offset - min_offset - block_size);

			if (mode == MODE_READ)
			{
				if (static_cast<int>(block_size) != pread(fd, &buf[0], block_size, offset))
					throwFromErrno("Cannot read");
			}
			else
			{
				if (static_cast<int>(block_size) != pwrite(fd, &buf[0], block_size, offset))
					throwFromErrno("Cannot write");
			}
		}
	}
	catch (const Poco::Exception & e)
	{
		exception = e.clone();
	}
	catch (...)
	{
		exception = new Poco::Exception("Unknown exception");
	}
}


int mainImpl(int argc, char ** argv)
{
	const char * file_name = 0;
	Mode mode = MODE_READ;
	size_t min_offset = 0;
	size_t max_offset = 0;
	size_t block_size = 0;
	size_t threads = 0;
	size_t count = 0;

	if (argc != 8)
	{
		std::cerr << "Usage: " << argv[0] << " file_name r|w min_offset max_offset block_size threads count" << std::endl;
		return 1;
	}

	file_name = argv[1];
	min_offset = Poco::NumberParser::parseUnsigned64(argv[3]);
	max_offset = Poco::NumberParser::parseUnsigned64(argv[4]);
	block_size = Poco::NumberParser::parseUnsigned64(argv[5]);
	threads = Poco::NumberParser::parseUnsigned(argv[6]);
	count = Poco::NumberParser::parseUnsigned(argv[7]);

	if (!strcmp(argv[2], "r"))
		mode = MODE_READ;
	else if (!strcmp(argv[2], "w"))
		mode = MODE_WRITE;

	boost::threadpool::pool pool(threads);

	int fd = open(file_name, O_SYNC | (mode == MODE_READ ? O_RDONLY : O_WRONLY));
	if (-1 == fd)
		throwFromErrno("Cannot open file");

	typedef std::vector<ExceptionPtr> Exceptions;
	Exceptions exceptions(threads);

	Stopwatch watch;
	
	for (size_t i = 0; i < threads; ++i)
		pool.schedule(boost::bind(thread, fd, mode, min_offset, max_offset, block_size, count, boost::ref(exceptions[i])));
	pool.wait();

	for (size_t i = 0; i < threads; ++i)
		if (exceptions[i])
			exceptions[i]->rethrow();

	watch.stop();

	if (0 != close(fd))
		throwFromErrno("Cannot close file");

	std::cout << std::fixed << std::setprecision(2)
		<< "Done " << count << " * " << threads << " ops in " << watch.elapsedSeconds() << " sec."
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
