#include <port/unistd.h>
#include <zstd.h>
#include <vector>
#include <stdexcept>
#include <sys/types.h>


int main(int argc, char ** argv)
{
    bool compress = argc == 1;

    const size_t size = 1048576;
    std::vector<char> src_buf(size);
    std::vector<char> dst_buf;

    size_t pos = 0;
    while (true)
    {
        ssize_t read_res = read(STDIN_FILENO, &src_buf[pos], size - pos);
        if (read_res < 0)
            throw std::runtime_error("Cannot read from stdin");
        if (read_res == 0)
            break;
        pos += read_res;
    }

    src_buf.resize(pos);

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

    pos = 0;
    while (pos < dst_buf.size())
    {
        ssize_t write_res = write(STDOUT_FILENO, &dst_buf[pos], dst_buf.size());
        if (write_res <= 0)
            throw std::runtime_error("Cannot write to stdout");
        pos += write_res;
    }

    return 0;
}
