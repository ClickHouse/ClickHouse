#include <unistd.h>
#include <zstd/zstd.h>
#include <vector>
#include <stdexcept>


int main(int argc, char ** argv)
{
	bool compress = argc == 1;

	const size_t size = 1048576;
	std::vector<char> src_buf(size);
	std::vector<char> dst_buf;

	ssize_t read_res = read(STDIN_FILENO, &src_buf[0], size);
	if (read_res <= 0)
		throw std::runtime_error("Cannot read from stdin");

	src_buf.resize(read_res);

	size_t zstd_res;

	if (compress)
	{
		dst_buf.resize(ZSTD_compressBound(src_buf.size()));

		zstd_res = ZSTD_compress(
			&dst_buf[0],
			dst_buf.size(),
			&src_buf[0],
			src_buf.size(),
			1);
	}
	else
	{
		dst_buf.resize(size);

		zstd_res = ZSTD_decompress(
			&dst_buf[0],
			dst_buf.size(),
			&src_buf[0],
			src_buf.size());
	}

	if (ZSTD_isError(zstd_res))
		throw std::runtime_error(ZSTD_getErrorName(zstd_res));

	dst_buf.resize(zstd_res);

	ssize_t write_res = write(STDOUT_FILENO, &dst_buf[0], dst_buf.size());
	if (write_res != static_cast<ssize_t>(dst_buf.size()))
		throw std::runtime_error("Cannot write to stdout");

	return 0;
}
