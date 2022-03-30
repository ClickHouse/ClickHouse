#include <iostream>
#include <zstd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>

/// Main compression part
int doCompress(char * input, char * output, off_t & in_offset, off_t & out_offset,
               off_t input_size, off_t output_size, ZSTD_CCtx * cctx)
{
    size_t compressed_size = ZSTD_compress2(cctx, output + out_offset, output_size, input + in_offset, input_size);
    if (ZSTD_isError(compressed_size))
    {
        std::cout << "Cannot compress block with ZSTD: " + std::string(ZSTD_getErrorName(compressed_size)) << std::endl;
        return 1;
    }
    in_offset += input_size;
    out_offset += compressed_size;
    return 0;
}

/// compress data from opened file into output file
int compress(int in_fd, int out_fd, int level=5)
{
    /// read data about input file
    struct stat info_in;
    fstat(in_fd, &info_in);
    if (info_in.st_size == 0) {
	    std::cout << "Empty input file" << std::endl;
	    return 1;
    }
    std::cout << "In current size is " << info_in.st_size << std::endl;

    /// Read data about output file.
    /// Compressed data will be added to the end of file
    /// It will allow to create self extracting executable from file
    struct stat info_out;
    fstat(out_fd, &info_out); 
    std::cout << "Out current size is " << info_out.st_size << std::endl;

    /// NOTE: next parametrs depend on binary size
    // 6402520 is size of stripped decompressor
    size_t start = 6405000ull;

    // 22558008ull size of decompressor
    // size_t start = 22558008ull;

    /// As experiments showed, size of compressed file is 4 times less than clickhouse executable
    /// Get a little bit more memory to prevent errors with size. 
    /// For compression this difference will not be huge
    ftruncate(out_fd, start + info_in.st_size / 3);
    off_t in_offset = 0, out_offset = start;

    /// mmap files
    char * input = static_cast<char*>(mmap(nullptr, info_in.st_size, PROT_READ, MAP_PRIVATE , in_fd, 0));
    char * output = static_cast<char*>(mmap(nullptr, start + info_in.st_size / 3, PROT_READ | PROT_WRITE | PROT_EXEC, MAP_SHARED, out_fd, 0));
    if (input == reinterpret_cast<char*>(-1) || output == reinterpret_cast<char*>(-1))
    {
        perror(nullptr);
        return 1;
    }

    /// Create context
    ZSTD_CCtx * cctx = ZSTD_createCCtx();
    ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, level);
    ZSTD_CCtx_setParameter(cctx, ZSTD_c_checksumFlag, 1);

    /// Remember size of file. It will help to avoid using additional memory 
    /// during decompression
    char * file_size = reinterpret_cast<char *>(&info_in.st_size);
    for (size_t i = 0; i < sizeof(info_in.st_size)/sizeof(char); ++i)
        output[out_offset++] = *(file_size + i);


    /// limits for size of block to prevent high memory usage or bad compression
    off_t max_block_size = 1ull<<27;
    off_t min_block_size = 1ull<<23;
    off_t size = 0;
    std::cout << in_offset <<  " " << out_offset << std::endl;

    /// Compress data
    while (in_offset < info_in.st_size)
    {
        /// take blocks of maximum size
        /// optimize last block (it can be bigger, if it is not too huge)
        if (info_in.st_size - in_offset < max_block_size || info_in.st_size - in_offset < max_block_size + min_block_size)
            size = info_in.st_size - in_offset;
        else
            size = max_block_size;

        /// Compress data or exit if error happens
        if (0 != doCompress(input, output, in_offset, out_offset, size, ZSTD_compressBound(size), cctx))
        {
            ftruncate(out_fd, info_out.st_size);
            munmap(input, info_in.st_size);
            munmap(output, start + info_in.st_size / 3);
            return 1;
        }
        std::cout << in_offset <<  " " << out_offset << std::endl;
    }

    /// Shrink file size and unmap
    ftruncate(out_fd, out_offset);
    munmap(input, info_in.st_size);
    munmap(output, start + info_in.st_size / 3);
    return 0;
}

int main(int argc, char* argv[])
{
    if (argc < 3)
    {
        std::cout << "Not enough arguments.\ncompressor [file that should be compressed] [file name for compressed file] [OPTIONAL level of compression]" << std::endl;
        return 0;
    }

    int input_fd = open(argv[1], O_RDONLY);
    if (input_fd == -1)
    {
        perror(nullptr);
        return 0;
    }
    
    int output_fd = open(argv[2], O_RDWR | O_CREAT, 0775);
    if (output_fd == -1)
    {
        perror(nullptr);
        return 0;
    }

    int result;
    if (argc == 4)
        result = compress(input_fd, output_fd, strtol(argv[3], nullptr, 10));
    else
        result = compress(input_fd, output_fd);

    if (result == 0)
        std::cout << "Successfully compressed" << std::endl;
    else
        std::cout << "An error has occurred" << std::endl;

    close(input_fd);
    close(output_fd);
    return 0;
}
