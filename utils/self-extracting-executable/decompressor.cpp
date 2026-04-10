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
#include <fstream>
#include <sstream>
#include <vector>

#if defined(OS_DARWIN) && defined(__GNUC__)
#   include <machine/endian.h>
#elif defined(OS_FREEBSD) && defined(__GNUC__)
#   include <machine/endian.h>
#   include <sys/endian.h>
#else
#   include <endian.h>
#endif

#if defined OS_DARWIN
#   include <mach-o/dyld.h>
#   include <libkern/OSByteOrder.h>
    // define 64 bit macros
#   define le64toh(x) OSSwapLittleToHostInt64(x)
#endif

#if defined(OS_FREEBSD)
#   include <sys/sysctl.h>
#endif

#include <types.h>

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
    std::cerr << "." << std::flush;
    return 0;
}

/// decompress data from in_fd into out_fd
int decompress(char * input, char * output, off_t start, off_t end, size_t max_number_of_forks=10)
{
    off_t in_pointer = start;
    off_t out_pointer = 0;
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
            perror("fork");
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
                _exit(1);
            _exit(0);
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

bool isSudo()
{
    return geteuid() == 0;
}

/// Read data about files and decompress them.
int decompressFiles(int input_fd, char * path, char * name, bool & have_compressed_analoge, bool & has_exec, char * decompressed_suffix, uint64_t * decompressed_umask)
{
    /// Read data about output file.
    /// Compressed data will replace data in file
    struct stat info_in;
    if (0 != fstat(input_fd, &info_in))
    {
        perror("fstat");
        return 1;
    }

    /// mmap input file
    char * input = static_cast<char*>(mmap(nullptr, info_in.st_size, PROT_READ, MAP_PRIVATE, input_fd, 0));
    if (input == MAP_FAILED)
    {
        perror("mmap");
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
        perror("fstatfs");
        if (0 != munmap(input, info_in.st_size))
                perror("munmap");
        return 1;
    }
    if (fs_info.f_blocks * info_in.st_blksize < decompressed_full_size)
    {
        std::cerr << "Not enough space for decompression. Have " << fs_info.f_blocks * info_in.st_blksize << ", need " << decompressed_full_size << std::endl;
        return 1;
    }

    bool is_sudo = isSudo();

    FileData file_info;
    /// Decompress files with appropriate file names
    for (size_t i = 0; i < le64toh(metadata.number_of_files); ++i)
    {
        /// Read information about file
        file_info = *reinterpret_cast<FileData*>(input + files_pointer);
        files_pointer += sizeof(FileData);

        /// for output filename matching compressed allow additional 13 + 7 symbols for ".decompressed.XXXXXX" suffix
        size_t file_name_len = file_info.exec ? strlen(name) + 13 + 7 + 1 : le64toh(file_info.name_length);

        size_t file_path_len = path ? strlen(path) + 1 + file_name_len : file_name_len;

        std::vector<char> file_name(file_path_len);
        memset(file_name.data(), '\0', file_path_len);
        if (path)
        {
            strcat(file_name.data(), path); // NOLINT(clang-analyzer-security.insecureAPI.strcpy)
            strcat(file_name.data(), "/"); // NOLINT(clang-analyzer-security.insecureAPI.strcpy)
        }

        bool same_name = false;
        if (file_info.exec)
        {
            has_exec = true;
            strcat(file_name.data(), name); // NOLINT(clang-analyzer-security.insecureAPI.strcpy)
        }
        else
        {
            if (strcmp(name, input + files_pointer) == 0)
                same_name = true;
            strcat(file_name.data(), input + files_pointer); // NOLINT(clang-analyzer-security.insecureAPI.strcpy)
        }

        files_pointer += le64toh(file_info.name_length);
        if (file_info.exec || same_name)
        {
            strcat(file_name.data(), ".decompressed.XXXXXX"); // NOLINT(clang-analyzer-security.insecureAPI.strcpy)
            int fd = mkstemp(file_name.data());
            if (fd == -1)
            {
                perror("mkstemp");
                return 1;
            }
            if (0 != close(fd))
                perror("close");
            strncpy(decompressed_suffix, file_name.data() + strlen(file_name.data()) - 6, 6);
            *decompressed_umask = le64toh(file_info.umask);
            have_compressed_analoge = true;
        }

        int output_fd = open(file_name.data(), O_RDWR | O_CREAT, le64toh(file_info.umask));

        if (output_fd == -1)
        {
            perror("open");
            if (0 != munmap(input, info_in.st_size))
                perror("munmap");
            return 1;
        }

        /// Prepare output file
        if (0 != ftruncate(output_fd, le64toh(file_info.uncompressed_size)))
        {
            perror("ftruncate");
            if (0 != munmap(input, info_in.st_size))
                perror("munmap");
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
            perror("mmap");
            if (0 != munmap(input, info_in.st_size))
                perror("munmap");
            return 1;
        }

        /// Decompress data into file
        if (0 != decompress(input, output, le64toh(file_info.start), le64toh(file_info.end)))
        {
            if (0 != munmap(input, info_in.st_size))
                perror("munmap");
            if (0 != munmap(output, le64toh(file_info.uncompressed_size)))
                perror("munmap");
            return 1;
        }

        if (0 != munmap(output, le64toh(file_info.uncompressed_size)))
            perror("munmap");
        if (0 != fsync(output_fd))
            perror("fsync");
        if (0 != close(output_fd))
            perror("close");

        if (is_sudo)
            chown(file_name.data(), info_in.st_uid, info_in.st_gid);
    }

    if (0 != munmap(input, info_in.st_size))
        perror("munmap");

    std::cerr << std::endl;
    return 0;
}

#if defined(OS_DARWIN)

    int read_exe_path(char *exe, size_t buf_sz)
    {
        uint32_t size = static_cast<uint32_t>(buf_sz);
        std::vector<char> apple(size);
        if (_NSGetExecutablePath(apple.data(), &size) != 0)
            return 1;
        if (realpath(apple.data(), exe) == nullptr)
            return 1;
        return 0;
    }

#elif defined(OS_FREEBSD)

    int read_exe_path(char *exe, size_t buf_sz)
    {
        int name[] = { CTL_KERN, KERN_PROC, KERN_PROC_PATHNAME, -1 };
        size_t length = buf_sz;
        int error = sysctl(name, 4, exe, &length, nullptr, 0);
        if (error < 0 || length <= 1)
            return 1;
        return 0;
    }

#else

    int read_exe_path(char *exe, size_t buf_sz)
    {
        ssize_t n = readlink("/proc/self/exe", exe, buf_sz - 1);
        if (n > 0)
            exe[n] = '\0';
        return n > 0 && n < static_cast<ssize_t>(buf_sz);
    }

#endif

#if !defined(OS_DARWIN) && !defined(OS_FREEBSD)

uint64_t getInode(const char * self)
{
    std::ifstream maps("/proc/self/maps");
    if (maps.fail())
    {
        perror("open maps");
        return 0;
    }

    /// Record example for /proc/self/maps:
    /// address                   perms offset  device inode                     pathname
    /// 561a247de000-561a247e0000 r--p 00000000 103:01 1564                      /usr/bin/cat
    /// see "man 5 proc"
    for (std::string line; std::getline(maps, line);)
    {
        std::stringstream ss(line); // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        std::string addr;
        std::string mode;
        std::string offset;
        std::string id;
        std::string path;
        uint64_t inode = 0;
        if (ss >> addr >> mode >> offset >> id >> inode >> path && path == self)
            return inode;
    }

    return 0;
}

#endif

int main(int/* argc*/, char* argv[])
{
    char self[4096] = {0};
    if (read_exe_path(self, 4096) == -1)
    {
        perror("read_exe_path");
        return 1;
    }

    std::vector<char> file_path(strlen(self) + 1);
    strcpy(file_path.data(), self); // NOLINT(clang-analyzer-security.insecureAPI.strcpy)

    char * path = nullptr;
    char * name = strrchr(file_path.data(), '/');
    if (name)
    {
        path = file_path.data();
        *name = 0;
        ++name;
    }
    else
        name = file_path.data();

    struct stat input_info;
    if (0 != stat(self, &input_info))
    {
        perror("stat");
        return 1;
    }

#if !defined(OS_DARWIN) && !defined(OS_FREEBSD)
    /// get inode of this executable
    uint64_t inode = getInode(self);
    if (inode == 0)
    {
        std::cerr << "Unable to obtain inode for exe '" << self << "'." << std::endl;
        return 1;
    }

    std::cerr << "Decompressing the binary" << std::flush;

    std::stringstream lock_path; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    lock_path << "/tmp/" << name << ".decompression." << inode << ".lock";
    int lock = open(lock_path.str().c_str(), O_CREAT | O_RDWR, 0666);
    if (lock < 0)
    {
        perror("lock open");
        return 1;
    }

    /// lock file should be closed on exec call
    fcntl(lock, F_SETFD, FD_CLOEXEC);

    if (lockf(lock, F_LOCK, 0))
    {
        perror("lockf");
        return 1;
    }

    /// inconsistency in WSL1 Ubuntu - inode reported in /proc/self/maps is a 64bit to
    /// 32bit conversion of input_info.st_ino
    if (input_info.st_ino & 0xFFFFFFFF00000000 && !(inode & 0xFFFFFFFF00000000))
        input_info.st_ino &= 0x00000000FFFFFFFF;

    /// if decompression was performed by another process since this copy was started
    /// then file referred by path "self" is already pointing to different inode
    if (input_info.st_ino != inode)
    {
        struct stat lock_info;
        if (0 != fstat(lock, &lock_info))
        {
            perror("fstat lock");
            return 1;
        }

        /// size 1 of lock file indicates that another decompressor has found active executable
        if (lock_info.st_size == 1)
            execv(self, argv);  // NOLINT(clang-analyzer-optin.taint.GenericTaint)

        printf("No target executable - decompression only was performed.\n"); // NOLINT(modernize-use-std-print)
        return 0;
    }
#endif

    int input_fd = open(self, O_RDONLY);
    if (input_fd == -1)
    {
        perror("open");
        return 1;
    }

    bool have_compressed_analoge = false;
    bool has_exec = false;
    char decompressed_suffix[7] = {0};
    uint64_t decompressed_umask = 0;

    /// Decompress all files
    if (0 != decompressFiles(input_fd, path, name, have_compressed_analoge, has_exec, decompressed_suffix, &decompressed_umask))
    {
        printf("Error happened during decompression.\n"); // NOLINT(modernize-use-std-print)
        if (0 != close(input_fd))
            perror("close");
        return 1;
    }

    if (0 != close(input_fd))
        perror("close");

    if (unlink(self))
    {
        perror("unlink");
        return 1;
    }

    if (!have_compressed_analoge)
        printf("No target executable - decompression only was performed.\n"); // NOLINT(modernize-use-std-print)
    else
    {
        const char * const decompressed_name_fmt = "%s.decompressed.%s";
        int decompressed_name_len = snprintf(nullptr, 0, decompressed_name_fmt, self, decompressed_suffix);
        std::vector<char> decompressed_name(decompressed_name_len + 1);
        (void)snprintf(decompressed_name.data(), decompressed_name_len + 1, decompressed_name_fmt, self, decompressed_suffix);

#if defined(OS_DARWIN)
        // We can't just rename it on Mac due to security issues, so we copy it...
        std::error_code ec;
        std::filesystem::copy_file(static_cast<char *>(decompressed_name.data()), static_cast<char *>(self), ec);
        if (ec)
        {
            std::cerr << ec.message() << std::endl;
            return 1;
        }
#else
        if (link(decompressed_name.data(), self))
        {
            perror("link");
            return 1;
        }
#endif
        if (chmod(self, static_cast<uint32_t>(decompressed_umask)))
        {
            perror("chmod");
            return 1;
        }

        if (unlink(decompressed_name.data()))
        {
            perror("unlink");
            return 1;
        }

        if (isSudo())
            chown(static_cast<char *>(self), input_info.st_uid, input_info.st_gid);

        if (has_exec)
        {
#if !defined(OS_DARWIN) && !defined(OS_FREEBSD)
            /// write one byte to the lock in case other copies of compressed are running to indicate that
            /// execution should be performed
            write(lock, "1", 1);
#endif
            execv(self, argv); // NOLINT(clang-analyzer-optin.taint.GenericTaint)

            /// This part of code will be reached only if error happened
            perror("execv");
            return 1;
        }
#if !defined(OS_DARWIN) && !defined(OS_FREEBSD)
        /// since inodes can be reused - it's a precaution if lock file already exists and have size of 1
        ftruncate(lock, 0);
#endif

        printf("No target executable - decompression only was performed.\n"); // NOLINT(modernize-use-std-print)
    }
}
