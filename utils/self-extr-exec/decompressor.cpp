#include <cstddef>
#include <cstdio>
#include <cstring>
#include <zstd.h>
#include <sys/mman.h>
#include <sys/statfs.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <stdlib.h>

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

/// decompress part
int doDecompress(char * input, char * output, off_t & in_offset, off_t & out_offset,
               off_t input_size, off_t output_size, ZSTD_DCtx* dctx)
{
    size_t decompressed_size = ZSTD_decompressDCtx(dctx, output + out_offset, output_size, input + in_offset, input_size);
    if (ZSTD_isError(decompressed_size))
    {
        printf("%s\n", ZSTD_getErrorName(decompressed_size));
        return 1;
    }
    return 0;
}

/// decompress data from in_fd into out_fd
int decompress(char * input, char * output, off_t start, off_t end, size_t max_number_of_forks=10)
{
    off_t in_pointer = start, out_pointer = 0;
    off_t size = 0;
    off_t max_block_size = 1ull<<27;
    off_t decompressed_size = 0;
    size_t number_of_forks = 0;

    /// Create context
    ZSTD_DCtx * dctx = ZSTD_createDCtx();
    if (dctx == nullptr)
    {
        printf("Failed to create context for compression");
        return 1;
    }
    pid_t pid;
    bool error_happened = false;

    /// Compress data
    while (in_pointer < end  && !error_happened)
    {
        size = ZSTD_findFrameCompressedSize(input + in_pointer, max_block_size);
        if (ZSTD_isError(size))
        {
            printf("%s\n", ZSTD_getErrorName(size));
            break;
        }

        decompressed_size = ZSTD_getFrameContentSize(input + in_pointer, max_block_size);
        if (ZSTD_isError(decompressed_size))
        {
            printf("%s\n", ZSTD_getErrorName(decompressed_size));
            break;
        }

        pid = fork();
        if (-1 == pid)
        {
            perror(nullptr);
            /// Decompress data in main process. Exit if error happens
            if (0 != doDecompress(input, output, in_pointer, out_pointer, size, max_block_size, dctx))
                break;
        }
        else if (pid == 0)
        {
            /// Decompress data. Exit if error happens
            if (0 != doDecompress(input, output, in_pointer, out_pointer, size, max_block_size, dctx))
                exit(1);
            exit(0);
        }
        else
        {
            ++number_of_forks;
            while (number_of_forks >= max_number_of_forks)
            {
                /// Wait any fork
                int status;
                waitpid(0, &status, 0);

                /// If error happened, stop processing
                if (WEXITSTATUS(status) != 0)
                {
                    error_happened = true;
                    break;
                }

                --number_of_forks;
            }
            in_pointer += size;
            out_pointer += decompressed_size;
        }
    }

    /// wait for all working decompressions
    while (number_of_forks > 0)
    {
        /// Wait any fork
        int status;
        waitpid(0, &status, 0);

        if (WEXITSTATUS(status) != 0)
        {
            error_happened = true;
        }

        --number_of_forks;
    }

    /// If error happen end of processed part will not reach end
    if (in_pointer < end || error_happened)
        return 1;

    return 0;
}


/// Read data about files and decomrpess them.
int decompressFiles(int input_fd, char* argv[], bool & have_compressed_analoge)
{
    /// Read data about output file.
    /// Compressed data will replace data in file
    struct stat info_in;
    if (0 != fstat(input_fd, &info_in))
    {
        perror(nullptr);
        return 1;
    }

    /// mmap input file
    char * input = static_cast<char*>(mmap(nullptr, info_in.st_size, PROT_READ, MAP_PRIVATE, input_fd, 0));
    if (input == MAP_FAILED)
    {
        perror(nullptr);
        return 1;
    }

    /// Read metadata from end of file
    MetaData metadata = *reinterpret_cast<MetaData*>(input + info_in.st_size - sizeof(MetaData));

    /// Prepare to read information about files and decompress them
    off_t files_pointer = metadata.start_of_files_data;
    size_t decompressed_full_size = 0;

    /// Read files metadata and check if decompression is possible
    off_t check_pointer = metadata.start_of_files_data;
    for (size_t i = 0; i < metadata.number_of_files; ++i)
    {
        FileData data = *reinterpret_cast<FileData*>(input + check_pointer);
        decompressed_full_size += data.uncompressed_size;
        check_pointer += sizeof(FileData) + data.name_length;
    }

    /// Check free space
    struct statfs fs_info;
    if (0 != fstatfs(input_fd, &fs_info))
    {
        perror(nullptr);
        if (0 != munmap(input, info_in.st_size))
                perror(nullptr);
        return 1;
    }
    if (fs_info.f_blocks * info_in.st_blksize < decompressed_full_size)
    {
        printf("Not enough space for decompression. Have %lu, need %zu.",
                fs_info.f_blocks * info_in.st_blksize, decompressed_full_size);
        return 1;
    }

    FileData file_info;
    /// Decompress files with appropriate file names
    for (size_t i = 0; i < metadata.number_of_files; ++i)
    {
        /// Read information about file
        file_info = *reinterpret_cast<FileData*>(input + files_pointer);
        files_pointer += sizeof(FileData);
        char file_name[file_info.name_length + 1];
        /// Filename should be ended with \0
        memset(file_name, '\0', file_info.name_length + 1);
        memcpy(file_name, input + files_pointer, file_info.name_length);
        files_pointer += file_info.name_length;

        /// Open file for decompressed data
        int output_fd;
        /// Check that name differs from executable filename
        if (0 == memcmp(file_name, strrchr(argv[0], '/') + 1, file_info.name_length))
        {
            /// Add .decompressed
            char new_file_name[file_info.name_length + 13];
            memcpy(new_file_name, file_name, file_info.name_length);
            memcpy(new_file_name + file_info.name_length, ".decompressed", 13);
            output_fd = open(new_file_name, O_RDWR | O_CREAT, 0775);
            have_compressed_analoge = true;
        }
        else
        {
            output_fd = open(file_name, O_RDWR | O_CREAT, 0775);
        }
        if (output_fd == -1)
        {
            perror(nullptr);
            if (0 != munmap(input, info_in.st_size))
                perror(nullptr);
            return 0;
        }

        /// Prepare output file
        if (0 != ftruncate(output_fd, file_info.uncompressed_size))
        {
            perror(nullptr);
            if (0 != munmap(input, info_in.st_size))
                perror(nullptr);
            return 1;
        }

        char * output = static_cast<char*>(
            mmap(nullptr,
                file_info.uncompressed_size,
                PROT_READ | PROT_WRITE, MAP_SHARED,
                output_fd,
                0)
            );
        if (output == MAP_FAILED)
        {
            perror(nullptr);
            if (0 != munmap(input, info_in.st_size))
                perror(nullptr);
            return 1;
        }

        /// Decompress data into file
        if (0 != decompress(input, output, file_info.start, file_info.end))
        {
            if (0 != munmap(input, info_in.st_size))
                perror(nullptr);
            if (0 != munmap(output, file_info.uncompressed_size))
                perror(nullptr);
            return 1;
        }

        if (0 != fsync(output_fd))
            perror(nullptr);
        if (0 != close(output_fd))
            perror(nullptr);
    }

    if (0 != munmap(input, info_in.st_size))
        perror(nullptr);
    return 0;
}

/// Copy particular part of command and update shift
void fill(char * dest, char * source, size_t length, size_t& shift)
{
    memcpy(dest + shift, source, length);
    shift += length;
}

/// Set command to `mv filename.decompressed filename && filename agrs...`
void fillCommand(char command[], int argc, char * argv[], size_t length)
{
    memset(command, '\0', 3 + strlen(argv[0]) + 14 + strlen(argv[0]) + 4 + strlen(argv[0]) + length + argc);

    /// position in command
    size_t shift = 0;

    /// Support variables to create command
    char mv[] = "mv ";
    char decompressed[] = ".decompressed ";
    char add_command[] = " && ";
    char space[] = " ";

    fill(command, mv, 3, shift);
    fill(command, argv[0], strlen(argv[0]), shift);
    fill(command, decompressed, 14, shift);
    fill(command, argv[0], strlen(argv[0]), shift);
    fill(command, add_command, 4, shift);
    fill(command, argv[0], strlen(argv[0]), shift);
    fill(command, space, 1, shift);

    /// forward all arguments
    for (int i = 1; i < argc; ++i)
    {
        fill(command, argv[i], strlen(argv[i]), shift);
        if (i != argc - 1)
            fill(command, space, 1, shift);
    }
}

int main(int argc, char* argv[])
{
    int input_fd = open(argv[0], O_RDONLY);
    if (input_fd == -1)
    {
        perror(nullptr);
        return 0;
    }

    bool have_compressed_analoge = false;

    /// Decompress all files
    if (0 != decompressFiles(input_fd, argv, have_compressed_analoge))
    {
        printf("Error happened");
        if (0 != close(input_fd))
            perror(nullptr);
        return 1;
    }

    if (0 != close(input_fd))
        perror(nullptr);

    /// According to documentation `mv` will rename file if it
    /// doesn't move to other directory.
    /// Sometimes `rename` doesn't exist by default and
    /// `rename.ul` is set instead. It will lead to errors
    /// that can be easily avoided with help of `mv`

    if (!have_compressed_analoge)
    {
        printf("Can't apply arguments to this binary");
        /// remove file
        char * name = strrchr(argv[0], '/') + 1;
        execlp("rm", "rm", name, NULL);
        perror(nullptr);
        return 1;
    }
    else
    {
        /// move decompressed file instead of this binary and apply command
        char bash[] = "/usr/bin/bash";
        char executable[] = "-c";

        /// length of forwarded args
        size_t length = 0;
        for (int i = 1; i < argc; ++i)
            length += strlen(argv[i]);

        /// mv filename.decompressed filename && filename agrs...
        char command[3 + strlen(argv[0]) + 14 + strlen(argv[0]) + 4 + strlen(argv[0]) + length + argc];
        fillCommand(command, argc, argv, length);

        /// replace file and call executable
        char * newargv[] = { bash, executable, command, nullptr };
        char * newenviron[] = { nullptr };
        execve("/usr/bin/bash", newargv, newenviron);

        /// This part of code will be reached only if error happened
        perror(nullptr);
        return 1;
    }
}
