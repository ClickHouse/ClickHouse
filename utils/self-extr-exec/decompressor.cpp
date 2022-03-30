#include <cstddef>
#include <cstdio>
#include <zstd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>

/// decompress part
int doDecompress(char * input, char * output, off_t & in_offset, off_t & out_offset,
               off_t input_size, off_t output_size, ZSTD_DCtx* dctx)
{
    size_t decompressed_size = ZSTD_decompressDCtx(dctx, output + out_offset, output_size, input + in_offset, input_size);
    if (ZSTD_isError(decompressed_size))
    {
        return 1;
    }
    in_offset += input_size;
    out_offset += decompressed_size;
    return 0;
}

/// decompress data from in_fd into out_fd
int decompress(int in_fd, int out_fd)
{
    /// Read data about output file.
    /// Compressed data will replace data in file
    struct stat info_in;
    fstat(in_fd, &info_in);

    /// NOTE: next parametrs depend on binary size
    // 22558008ull for full, 6405000ull for stripped;
    off_t in_offset = 6405000ull /*size of decompressor*/, out_offset = 0;

    /// mmap files
    char * input = static_cast<char*>(mmap(nullptr, info_in.st_size, PROT_READ, MAP_SHARED , in_fd, 0));
    if (input == reinterpret_cast<char*>(-1))
    {
        perror(nullptr);
        return 1;
    }

    /// Create context
    ZSTD_DCtx * dctx = ZSTD_createDCtx();

    /// Read size of file. It will help to avoid using additional memory 
    /// during decompression.
    size_t * file_size = reinterpret_cast<size_t *>(input + in_offset);
    in_offset += sizeof(size_t);
    
    /// Prepare output file
    ftruncate(out_fd, *file_size);
    char * output = static_cast<char*>(mmap(nullptr, *file_size, PROT_READ | PROT_WRITE | PROT_EXEC, MAP_SHARED, out_fd, 0));
    if (output == reinterpret_cast<char*>(-1))
    {
        perror(nullptr);
        return 1;
    }    

    off_t size = 0;
    off_t max_block_size = 1ull<<27;

    /// Compress data
    while (in_offset < info_in.st_size)
    {
        size = ZSTD_findFrameCompressedSize(input + in_offset, max_block_size);

        /// Compress data or exit if error happens
        if (0 != doDecompress(input, output, in_offset, out_offset, size, max_block_size, dctx))
        {
            munmap(input, info_in.st_size);
            munmap(output, *file_size);
            return 1;
        }
    }

    /// Shrink file size and unmap
    munmap(output, *file_size);
    munmap(input, info_in.st_size);
    return 0;
}

int main(int /*argc*/, char* argv[])
{
    int input_fd = open(argv[0], O_RDONLY);
    if (input_fd == -1)
    {
        perror(nullptr);
        return 0;
    }
    
    int output_fd = open("clickhouse.decompressed", O_RDWR | O_CREAT, 0775);
    if (output_fd == -1)
    {
        perror(nullptr);
        return 0;
    }

    if (0 != decompress(input_fd, output_fd))
    {
        return 1;
    }

    fsync(output_fd);
    close(output_fd);
    close(input_fd);

    /// NOTE: This command should not depend from any variables.
    /// It should be changed if file changes.
    execl("/usr/bin/bash", "bash", "-c", "mv ./clickhouse.decompressed ./clickhouse", NULL);
    return 0;
}
