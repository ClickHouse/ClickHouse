#include <cstring>
#include <iostream>
#include <zstd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>

/*
Overview of compression:
     ______________________
    |     Decompressor     |
    |----------------------|
    |   Compressed file 1  |
    |   Compressed file 2  |
    |         ...          |
    |----------------------|
    |   Info about 1 file  |
    |   Info about 2 file  |
    |         ...          |
    |----------------------|
    |      Metadata        |
    |______________________|
*/

/*
Metadata contains:
    1) number of files to support multiple file compression
    2) start_of_files_data to know start of files metadata
    3) end of binary to know start of compressed data
    4) uncompressed data size
*/
struct MetaData
{
    size_t number_of_files     = 0;
    size_t start_of_files_data = 0;
};

/// Information about each file for correct extraction.
/// Each file data is followed by name of file
/// with length equals to name_length.
struct FileData
{
    size_t start             = 0;
    size_t end               = 0;
    size_t name_length       = 0;
    size_t uncompressed_size = 0;
};

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
int compress(int in_fd, int out_fd, int level, off_t& pointer, const struct stat& info_in)
{
    /// As experiments showed, size of compressed file is 4 times less than clickhouse executable
    /// Get a little bit more memory to prevent errors with size.
    /// For compression this difference will not be huge
    if (0 != ftruncate(out_fd, pointer + info_in.st_size / 3))
    {
        perror(nullptr);
        return 1;
    }
    off_t in_offset = 0;

    /// mmap files
    char * input = static_cast<char*>(mmap(nullptr, info_in.st_size, PROT_READ, MAP_PRIVATE, in_fd, 0));
    char * output = static_cast<char*>(
        mmap(nullptr, pointer + info_in.st_size / 3,
            PROT_READ | PROT_WRITE, MAP_SHARED,
            out_fd,
            0)
        );
    if (input == MAP_FAILED || output == MAP_FAILED)
    {
        perror(nullptr);
        return 1;
    }

    /// Create context
    ZSTD_CCtx * cctx = ZSTD_createCCtx();
    if (cctx == nullptr)
    {
        std::cout << "Failed to create context for compression" << std::endl;
        return 1;
    }

    size_t check_result;

    /// Set level and enable checksums
    check_result = ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, level);
    if (ZSTD_isError(check_result))
    {
        std::cout << "Failed to set compression level: " + std::string(ZSTD_getErrorName(check_result)) << std::endl;
        return 1;
    }
    check_result = ZSTD_CCtx_setParameter(cctx, ZSTD_c_checksumFlag, 1);
    if (ZSTD_isError(check_result))
    {
        std::cout << "Failed to set checksums: " + std::string(ZSTD_getErrorName(check_result)) << std::endl;
        return 1;
    }

    /// limits for size of block to prevent high memory usage or bad compression
    off_t max_block_size = 1ull<<27;
    off_t min_block_size = 1ull<<23;
    off_t size = 0;

    /// TODO: Maybe make better information instead of offsets
    std::cout << "Current offset in infile is|\t Output pointer is " << std::endl;
    std::cout << in_offset << "\t\t\t\t" << pointer << std::endl;

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
        if (0 != doCompress(input, output, in_offset, pointer, size, ZSTD_compressBound(size), cctx))
        {
            if (0 != munmap(input, info_in.st_size))
                perror(nullptr);
            if (0 != munmap(output, pointer + info_in.st_size / 3))
                perror(nullptr);
            return 1;
        }
        std::cout << in_offset << "\t\t\t" << pointer << std::endl;
    }

    /// Shrink file size and unmap
    if (0 != ftruncate(out_fd, pointer) || 0 != munmap(input, info_in.st_size) ||
        0 != munmap(output, pointer + info_in.st_size / 3))
    {
        perror(nullptr);
        return 1;
    }
    return 0;
}

/// Save Metadata at the end of file
int saveMetaData(char* filenames[], int count, int output_fd, const MetaData& metadata,
                 FileData* files_data, size_t pointer, size_t sum_file_size)
{
    /// Allocate memory for metadata
    if (0 != ftruncate(output_fd, pointer + count * sizeof(FileData) + sum_file_size + sizeof(MetaData)))
    {
        perror(nullptr);
        return 1;
    }

    char * output = static_cast<char*>(
        mmap(nullptr,
            pointer + count * sizeof(FileData) + sum_file_size + sizeof(MetaData),
            PROT_READ | PROT_WRITE, MAP_SHARED,
            output_fd,
            0)
        );
    if (output == MAP_FAILED)
    {
        perror(nullptr);
        return 1;
    }

    /// save information about files and their names
    for (int i = 0; i < count; ++i)
    {
        /// Save file data
        memcpy(output + pointer, reinterpret_cast<char*>(files_data + i), sizeof(FileData));
        pointer += sizeof(FileData);

        /// Save file name
        memcpy(output + pointer, filenames[i], files_data[i].name_length);
        pointer += files_data[i].name_length;
    }

    /// Save metadata
    memcpy(output + pointer, reinterpret_cast<const char*>(&metadata), sizeof(MetaData));
    return 0;
}

/// Fills metadata and calls compression function for each file
int compressFiles(char* filenames[], int count, int output_fd, int level, const struct stat& info_out)
{
    /// TODO: check that compression can be done (?)
    /// It is difficult to predict compressed size and
    /// the upper estimate of memory (size + 1/3 sum_of_files_size)
    /// is very rude and can fail even if compression can be successfully done

    MetaData metadata;
    size_t sum_file_size = 0;
    metadata.number_of_files = count;
    off_t pointer = info_out.st_size;

    /// Store information about each file and compress it
    FileData* files_data = new FileData[count];
    char * names[count];
    for (int i = 0; i < count; ++i)
    {
        std::cout << "Start compression for " << filenames[i] << std::endl;

        int input_fd = open(filenames[i], O_RDONLY);
        if (input_fd == -1)
        {
            perror(nullptr);
            delete [] files_data;
            return 1;
        }

        /// Remember information about file name
        /// This should be made after the file is opened
        /// because filename should be extracted from path
        names[i] = strrchr(filenames[i], '/') + 1;
        files_data[i].name_length = strlen(names[i]);
        sum_file_size += files_data[i].name_length;

        /// read data about input file
        struct stat info_in;
        if (0 != fstat(input_fd, &info_in))
        {
            perror(nullptr);
            delete [] files_data;
            return 1;
        }

        if (info_in.st_size == 0)
        {
            std::cout << "Empty input file will be skipped." << std::endl;
            continue;
        }

        std::cout << "Input file current size is " << info_in.st_size << std::endl;

        /// Remember information about uncompressed size of file and
        /// start of it's compression version
        files_data[i].uncompressed_size = info_in.st_size;
        files_data[i].start = pointer;

        /// Compressed data will be added to the end of file
        /// It will allow to create self extracting executable from file
        if (0 != compress(input_fd, output_fd, level, pointer, info_in))
        {
            perror(nullptr);
            delete [] files_data;
            return 1;
        }

        /// This error is less important, than others.
        /// If file cannot be closed, in some cases it will lead to
        /// error in other function that will stop compression process
        if (0 != close(input_fd))
            perror(nullptr);

        files_data[i].end = pointer;
    }

    /// save location of files information
    metadata.start_of_files_data = pointer;

    if (0 != saveMetaData(names, count, output_fd, metadata, files_data, pointer, sum_file_size))
    {
        delete [] files_data;
        return 1;
    }

    delete [] files_data;
    return 0;
}

int main(int argc, char* argv[])
{
    if (argc < 3)
    {
        std::cout << "Not enough arguments.\ncompressor [OPTIONAL --level of compression] [file name for compressed file] [files that should be compressed]" << std::endl;
        return 0;
    }

    int start_of_files = 1;

    /// Set compression level
    int level = 5;
    if (0 == memcmp(argv[1], "--level=", 8))
    {
        level = strtol(argv[1] + 8, nullptr, 10);
        ++start_of_files;
    }

    int output_fd = open(argv[start_of_files], O_RDWR | O_CREAT, 0775);
    if (output_fd == -1)
    {
        perror(nullptr);
        return 1;
    }
    ++start_of_files;

    struct stat info_out;
    if (0 != fstat(output_fd, &info_out))
    {
        perror(nullptr);
        return 1;
    }

    std::cout << "Compression with level " << level << std::endl;
    if (0 != compressFiles(&argv[start_of_files], argc - start_of_files, output_fd, level, info_out))
    {
        std::cout << "Compression was not successful." << std::endl;

        /// Cancel changes. Reset the file to its original state
        if (0 != ftruncate(output_fd, info_out.st_size))
        {
            perror(nullptr);
        }
    }
    else
    {
        std::cout << "Successfully compressed" << std::endl;
    }

    if (0 != close(output_fd))
    {
        perror(nullptr);
        return 1;
    }
    return 0;
}
