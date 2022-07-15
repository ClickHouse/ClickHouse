#include <zstd.h>
#include <sys/mman.h>
#if defined __APPLE__
#include <sys/mount.h>
#else
#include <sys/statfs.h>
#endif
#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <iostream>
#if defined __APPLE__

// dependencies
#include <machine/endian.h>
#include <libkern/OSByteOrder.h>

// define 64 bit macros
#define le64toh(x) OSSwapLittleToHostInt64(x)

#else
#include <endian.h>
#endif

#include "types.h"

/// decompress part
int doDecompress(char * input, char * output, off_t & in_offset, off_t & out_offset,
               off_t input_size, off_t output_size, ZSTD_DCtx* dctx)
{
    size_t decompressed_size = ZSTD_decompressDCtx(dctx, output + out_offset, output_size, input + in_offset, input_size);
    if (ZSTD_isError(decompressed_size))
    {
        std::cerr << "Error (ZSTD):" << decompressed_size << " " << ZSTD_getErrorName(decompressed_size) << std::endl;
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
        std::cerr << "Error (ZSTD): failed to create decompression context" << std::endl;
        return 1;
    }
    pid_t pid;
    bool error_happened = false;

    /// Decompress data
    while (in_pointer < end && !error_happened)
    {
        size = ZSTD_findFrameCompressedSize(input + in_pointer, max_block_size);
        if (ZSTD_isError(size))
        {
            std::cerr << "Error (ZSTD): " << size << " " << ZSTD_getErrorName(size) << std::endl;
            error_happened = true;
            break;
        }

        decompressed_size = ZSTD_getFrameContentSize(input + in_pointer, max_block_size);
        if (ZSTD_isError(decompressed_size))
        {
            std::cerr << "Error (ZSTD): " << decompressed_size << " " << ZSTD_getErrorName(decompressed_size) << std::endl;
            error_happened = true;
            break;
        }

        pid = fork();
        if (-1 == pid)
        {
            perror(nullptr);
            /// If fork failed just decompress data in main process.
            if (0 != doDecompress(input, output, in_pointer, out_pointer, size, decompressed_size, dctx))
            {
                error_happened = true;
                break;
            }
            in_pointer += size;
            out_pointer += decompressed_size;
        }
        else if (pid == 0)
        {
            /// Decompress data in child process.
            if (0 != doDecompress(input, output, in_pointer, out_pointer, size, decompressed_size, dctx))
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

        if (WIFEXITED(status))
        {
            if (WEXITSTATUS(status) != 0)
                error_happened = true;
        }
        else
        {
            error_happened = true;
            if (WIFSIGNALED(status))
            {
                if (WCOREDUMP(status))
                    std::cerr << "Error: child process core dumped with signal " << WTERMSIG(status) << std::endl;
                else
                    std::cerr << "Error: child process was terminated with signal " << WTERMSIG(status) << std::endl;
            }
        }

        if (WEXITSTATUS(status) != 0)
            error_happened = true;

        --number_of_forks;
    }

    /// If error happen end of processed part will not reach end
    if (in_pointer < end || error_happened)
        return 1;

    return 0;
}


/// Read data about files and decomrpess them.
int decompressFiles(int input_fd, char * path, char * name, bool & have_compressed_analoge)
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
    off_t files_pointer = le64toh(metadata.start_of_files_data);
    size_t decompressed_full_size = 0;

    /// Read files metadata and check if decompression is possible
    off_t check_pointer = le64toh(metadata.start_of_files_data);
    for (size_t i = 0; i < le64toh(metadata.number_of_files); ++i)
    {
        FileData data = *reinterpret_cast<FileData*>(input + check_pointer);
        decompressed_full_size += le64toh(data.uncompressed_size);
        check_pointer += sizeof(FileData) + le64toh(data.name_length);
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
        std::cerr << "Not enough space for decompression. Have " << fs_info.f_blocks * info_in.st_blksize << ", need " << decompressed_full_size << std::endl;
        return 1;
    }

    FileData file_info;
    /// Decompress files with appropriate file names
    for (size_t i = 0; i < le64toh(metadata.number_of_files); ++i)
    {
        /// Read information about file
        file_info = *reinterpret_cast<FileData*>(input + files_pointer);
        files_pointer += sizeof(FileData);

        size_t file_name_len =
            (strcmp(input + files_pointer, name) ? le64toh(file_info.name_length) : le64toh(file_info.name_length) + 13);

        size_t file_path_len = path ? strlen(path) + 1 + file_name_len : file_name_len;

        char file_name[file_path_len];
        memset(file_name, '\0', file_path_len);
        if (path)
        {
            strcat(file_name, path);
            strcat(file_name, "/");
        }
        strcat(file_name, input + files_pointer);
        files_pointer += le64toh(file_info.name_length);
        if (file_name_len != le64toh(file_info.name_length))
        {
            strcat(file_name, ".decompressed");
            have_compressed_analoge = true;
        }

        int output_fd = open(file_name, O_RDWR | O_CREAT, le64toh(file_info.umask));

        if (output_fd == -1)
        {
            perror(nullptr);
            if (0 != munmap(input, info_in.st_size))
                perror(nullptr);
            return 1;
        }

        /// Prepare output file
        if (0 != ftruncate(output_fd, le64toh(file_info.uncompressed_size)))
        {
            perror(nullptr);
            if (0 != munmap(input, info_in.st_size))
                perror(nullptr);
            return 1;
        }

        char * output = static_cast<char*>(
            mmap(nullptr,
                le64toh(file_info.uncompressed_size),
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
        if (0 != decompress(input, output, le64toh(file_info.start), le64toh(file_info.end)))
        {
            if (0 != munmap(input, info_in.st_size))
                perror(nullptr);
            if (0 != munmap(output, le64toh(file_info.uncompressed_size)))
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
    char file_path[strlen(argv[0]) + 1];
    memset(file_path, 0, sizeof(file_path));
    strcpy(file_path, argv[0]);

    char * path = nullptr;
    char * name = strrchr(file_path, '/');
    if (name)
    {
        path = file_path;
        *name = 0;
        ++name;
    }
    else
        name = file_path;

    int input_fd = open(argv[0], O_RDONLY);
    if (input_fd == -1)
    {
        perror(nullptr);
        return 1;
    }

    bool have_compressed_analoge = false;

    /// Decompress all files
    if (0 != decompressFiles(input_fd, path, name, have_compressed_analoge))
    {
        printf("Error happened during decompression.\n");
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
        printf("No target executable - decompression only was performed.\n");
        /// remove file
        execlp("rm", "rm", argv[0], NULL);
        perror(nullptr);
        return 1;
    }
    else
    {
        /// move decompressed file instead of this binary and apply command
        char bash[] = "sh";
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
        execvp(bash, newargv);

        /// This part of code will be reached only if error happened
        perror(nullptr);
        return 1;
    }
}
