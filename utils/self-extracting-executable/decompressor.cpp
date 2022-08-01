#include <zstd.h>
#include <sys/mman.h>
#if defined(OS_DARWIN) || defined(OS_FREEBSD)
#   include <sys/mount.h>
#else
#   include <sys/statfs.h>
#endif
#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <filesystem>

#if (defined(OS_DARWIN) || defined(OS_FREEBSD)) && defined(__GNUC__)
#   include <machine/endian.h>
#else
#   include <endian.h>
#endif

#if defined OS_DARWIN
#   include <libkern/OSByteOrder.h>
    // define 64 bit macros
#   define le64toh(x) OSSwapLittleToHostInt64(x)
#endif

#include "types.h"

char decompressed_suffix[7] = {0};
uint64_t decompressed_umask = 0;

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

    ZSTD_freeDCtx(dctx);

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
            (strcmp(input + files_pointer, name) ? le64toh(file_info.name_length) : le64toh(file_info.name_length) + 13 + 7);

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
            strcat(file_name, ".decompressed.XXXXXX");
            int fd = mkstemp(file_name);
            if (fd == -1)
            {
                perror(nullptr);
                return 1;
            }
            close(fd);
            strncpy(decompressed_suffix, file_name + strlen(file_name) - 6, 6);
            decompressed_umask = le64toh(file_info.umask);
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


int main(int/* argc*/, char* argv[])
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

    if (unlink(argv[0]))
    {
        perror(nullptr);
        return 1;
    }

    if (!have_compressed_analoge)
        printf("No target executable - decompression only was performed.\n");
    else
    {
        const char * const decompressed_name_fmt = "%s.decompressed.%s";
        int decompressed_name_len = snprintf(nullptr, 0, decompressed_name_fmt, argv[0], decompressed_suffix);
        char decompressed_name[decompressed_name_len + 1];
        decompressed_name_len = snprintf(decompressed_name, decompressed_name_len + 1, decompressed_name_fmt, argv[0], decompressed_suffix);

        std::error_code ec;
        std::filesystem::copy_file(static_cast<char *>(decompressed_name), argv[0], ec);
        if (ec)
        {
            std::cerr << ec.message() << std::endl;
            return 1;
        }

        if (chmod(argv[0], decompressed_umask))
        {
            perror(nullptr);
            return 1;
        }

        if (unlink(decompressed_name))
        {
            perror(nullptr);
            return 1;
        }

        execv(argv[0], argv);

        /// This part of code will be reached only if error happened
        perror(nullptr);
        return 1;
    }
}
