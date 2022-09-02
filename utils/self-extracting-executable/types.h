#pragma once

#include <cstdint>
#include <sys/types.h>
#include <sys/stat.h>

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
    uint64_t number_of_files     = 0;
    uint64_t start_of_files_data = 0;
};

/// Information about each file for correct extraction.
/// Each file data is followed by name of file
/// with length equals to name_length.
struct FileData
{
    uint64_t start             = 0;
    uint64_t end               = 0;
    uint64_t name_length       = 0;
    uint64_t uncompressed_size = 0;
    uint64_t umask             = 0;
    bool     exec              = false;
};

