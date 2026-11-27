#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sys/mman.h>
#include <thread>
#include <unistd.h>
#include <vector>
#include <zstd.h>
#include <zstd_errors.h>

#if defined(OS_DARWIN) && defined(__GNUC__)
#   include <machine/endian.h>
#elif defined(OS_FREEBSD) && defined(__GNUC__)
#   include <machine/endian.h>
#   include <sys/endian.h>
#else
#   include <endian.h>
#endif

#if defined OS_DARWIN
#   include <libkern/OSByteOrder.h>
    // define 64 bit macros
#   define htole64(x) OSSwapHostToLittleInt64(x)
#endif

#include <types.h>

/// blocking write
static ssize_t write_data(int fd, const void *buf, size_t count)
{
    for (size_t n = 0; n < count;)
    {
        ssize_t sz = write(fd, reinterpret_cast<const char*>(buf) + n, count - n);
        if (sz < 0)
        {
            if (errno == EINTR)
                continue;
            return sz;
        }
        n += sz;
    }
    return count;
}

/// blocking read
static ssize_t read_data(int fd, void *buf, size_t count)
{
    for (size_t n = 0; n < count;)
    {
        ssize_t sz = read(fd, reinterpret_cast<char*>(buf) + n, count - n);
        if (sz < 0)
        {
            if (errno == EINTR)
                continue;
            return sz;
        }
        if (sz == 0)
            return count - n;
        n += sz;
    }
    return count;
}

/// Compress one block into its own zstd frame
static size_t doCompress(const char * input, off_t input_size, char * output, size_t output_size, int level)
{
    ZSTD_CCtx * cctx = ZSTD_createCCtx();
    if (cctx == nullptr)
        return static_cast<size_t>(-ZSTD_error_memory_allocation);

    size_t result = ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, level);
    if (!ZSTD_isError(result))
        result = ZSTD_CCtx_setParameter(cctx, ZSTD_c_checksumFlag, 1);
    if (!ZSTD_isError(result))
        result = ZSTD_compress2(cctx, output, output_size, input, input_size);

    ZSTD_freeCCtx(cctx);
    return result;
}

/// compress data from opened file into output file
static int compress(int in_fd, int out_fd, int level, off_t & pointer, const struct stat & info_in, uint64_t & compressed_size)
{
    compressed_size = 0;

    /// mmap files
    char * input = static_cast<char*>(mmap(nullptr, info_in.st_size, PROT_READ, MAP_PRIVATE, in_fd, 0));
    if (input == MAP_FAILED)
    {
        perror("mmap");
        return 1;
    }

    /// limits for size of block to prevent high memory usage or bad compression
    off_t max_block_size = 1ull<<27;
    off_t min_block_size = 1ull<<23;

    /// Split input into blocks: take blocks of maximum size,
    /// optimize last block (it can be bigger, if it is not too huge)
    std::vector<std::pair<off_t, off_t>> blocks;
    for (off_t in_offset = 0; in_offset < info_in.st_size;)
    {
        off_t size = info_in.st_size - in_offset < max_block_size + min_block_size ? info_in.st_size - in_offset : max_block_size;
        blocks.emplace_back(in_offset, size);
        in_offset += size;
    }

    /// Blocks are independent frames, so a batch of them is compressed in parallel and written in order.
    /// Block can't become much bigger after compression.
    size_t num_threads = std::max<size_t>(1, std::min<size_t>(std::thread::hardware_concurrency(), blocks.size()));
    size_t output_size = 2 * max_block_size;
    char * output = static_cast<char*>(
        mmap(nullptr, num_threads * output_size,
            PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE,
            -1,
            0)
        );
    if (output == MAP_FAILED)
    {
        perror("mmap");
        munmap(input, info_in.st_size);
        return 1;
    }
    if (-1 == lseek(out_fd, 0, SEEK_END))
    {
        perror("lseek");
        munmap(input, info_in.st_size);
        munmap(output, num_threads * output_size);
        return 1;
    }

    uint64_t total_size = 0;
    int ret = 0;
    std::vector<size_t> results(num_threads);

    /// Compress data
    for (size_t batch_begin = 0; batch_begin < blocks.size() && ret == 0; batch_begin += num_threads)
    {
        size_t batch_size = std::min(num_threads, blocks.size() - batch_begin);
        std::vector<std::thread> threads;
        for (size_t i = 0; i < batch_size; ++i)
        {
            const auto & [offset, size] = blocks[batch_begin + i];
            threads.emplace_back([&, i, offset, size]
                { results[i] = doCompress(input + offset, size, output + i * output_size, output_size, level); });
        }
        for (auto & thread : threads)
            thread.join();

        /// Save data into file and refresh pointer
        for (size_t i = 0; i < batch_size; ++i)
        {
            size_t current_block_size = results[i];
            if (ZSTD_isError(current_block_size))
            {
                std::cerr << "Error (ZSTD): " << current_block_size << " " << ZSTD_getErrorName(current_block_size) << std::endl;
                ret = 1;
                break;
            }
            if (static_cast<ssize_t>(current_block_size) != write_data(out_fd, output + i * output_size, current_block_size))
            {
                perror("write");
                ret = 1;
                break;
            }
            off_t size = blocks[batch_begin + i].second;
            pointer += current_block_size;
            printf("...block compression rate: %.2f%%\n", static_cast<double>(current_block_size) / static_cast<double>(size) * 100); // NOLINT(modernize-use-std-print)
            total_size += size;
            compressed_size += current_block_size;
        }
    }

    if (0 != munmap(input, info_in.st_size) ||
        0 != munmap(output, num_threads * output_size))
    {
        perror("munmap");
        return 1;
    }
    if (ret != 0)
        return ret;

    std::cout <<
        "Compressed size: " << compressed_size <<
        ", compression rate: " << std::fixed << std::setprecision(2) <<
        static_cast<float>(compressed_size) / static_cast<float>(total_size) * 100 << "%"
        << std::endl;

    return 0;
}

/// Save Metadata at the end of file
static int saveMetaData(const char* filenames[], int count, int output_fd, const MetaData& metadata,
                 FileData* files_data, size_t pointer, size_t sum_file_size)
{
    /// Allocate memory for metadata
    if (0 != ftruncate(output_fd, pointer + count * sizeof(FileData) + sum_file_size + sizeof(MetaData)))
    {
        perror("ftruncate");
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
        perror("mmap");
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
static int compressFiles(const char* out_name, const char* exec, char* filenames[], int count, int output_fd, int level, const struct stat& info_out)
{
    MetaData metadata;
    size_t sum_file_size = 0;
    int is_exec = exec && *exec ? 1 : 0;
    metadata.number_of_files = htole64(count + is_exec);
    off_t pointer = info_out.st_size;

    uint64_t total_size = 0;
    uint64_t total_compressed_size = 0;

    /// Store information about each file and compress it
    FileData* files_data = new FileData[count + is_exec];
    std::vector<const char *> names(count + is_exec);
    for (int i = 0; i <= count; ++i)
    {
        const char* filename = nullptr;
        if (i == count)
        {
            if (!is_exec)
                continue;
            filename = exec;
            files_data[i].exec = true;
        }
        else
            filename = filenames[i];

        printf("Compressing: %s\n", filename); // NOLINT(modernize-use-std-print)

        int input_fd = open(filename, O_RDONLY);
        if (input_fd == -1)
        {
            perror("open");
            delete [] files_data;
            return 1;
        }

        /// Remember information about file name
        /// This should be made after the file is opened
        /// because filename should be extracted from path
        names[i] = strrchr(filename, '/');
        if (names[i])
            ++names[i];
        else
            names[i] = filename;
        size_t nlen = strlen(names[i]) + 1;
        files_data[i].name_length = htole64(nlen);
        sum_file_size += nlen;
        /// if no --exec is specified nor it's empty - file which is matching output name is executable
        if (!is_exec && !exec && strcmp(names[i], out_name) == 0)
            files_data[i].exec = true;

        /// read data about input file
        struct stat info_in{};
        if (0 != fstat(input_fd, &info_in))
        {
            perror("fstat");
            delete [] files_data;
            return 1;
        }

        if (info_in.st_size == 0)
        {
            printf("...empty file, skipped.\n"); // NOLINT(modernize-use-std-print)
            continue;
        }

        std::cout << "Size: " << info_in.st_size << std::endl;
        total_size += info_in.st_size;

        /// Save umask
        files_data[i].umask = htole64(info_in.st_mode);

        /// Remember information about uncompressed size of file and
        /// start of it's compression version
        files_data[i].uncompressed_size = htole64(info_in.st_size);
        files_data[i].start = htole64(pointer);

        uint64_t compressed_size = 0;

        /// Compressed data will be added to the end of file
        /// It will allow to create self extracting executable from file
        if (0 != compress(input_fd, output_fd, level, pointer, info_in, compressed_size))
        {
            perror("compress");
            delete [] files_data;
            return 1;
        }

        total_compressed_size += compressed_size;

        /// This error is less important, than others.
        /// If file cannot be closed, in some cases it will lead to
        /// error in other function that will stop compression process
        if (0 != close(input_fd))
            perror("close");

        files_data[i].end = htole64(pointer);
    }

    /// save location of files information
    metadata.start_of_files_data = htole64(pointer);

    if (0 != saveMetaData(names.data(), count + is_exec, output_fd, metadata, files_data, pointer, sum_file_size))
    {
        delete [] files_data;
        return 1;
    }

    std::cout << "Compression rate: " << std::fixed << std::setprecision(2)
              << static_cast<float>(total_compressed_size) / static_cast<float>(total_size) * 100 << "%" << std::endl;

    delete [] files_data;
    return 0;
}

static int copy_decompressor(int input_fd, ssize_t decompressor_size, int output_fd)
{
    const ssize_t buf_size = 1ul<<19;
    auto buf_memory = std::make_unique<char[]>(buf_size);
    char * buf = buf_memory.get();

    while (decompressor_size > 0)
    {
        ssize_t read_size = decompressor_size > buf_size ? buf_size : decompressor_size;
        ssize_t n = read_data(input_fd, buf, read_size);
        if (n < read_size)
        {
            perror("read");
            return 1;
        }
        decompressor_size -= n;

        if (n != write_data(output_fd, buf, n))
        {
            perror("write");
            return 1;
        }
    }

    return 0;
}

static int copy_decompressor_self(const char *self, int output_fd)
{
    int input_fd = open(self, O_RDONLY);
    if (input_fd == -1)
    {
        perror("open");
        return 1;
    }

    if (-1 == lseek(input_fd, -15, SEEK_END))
    {
        perror("lseek");
        if (0 != close(input_fd))
            perror("close");
        return 1;
    }

    char size_str[16] = {0};
    if (ssize_t sz = read_data(input_fd, size_str, 15); sz < 15)
    {
        if (sz < 0)
            perror("read");
        else
            std::cerr << "Error: unable to extract decompressor" << std::endl;
        if (0 != close(input_fd))
            perror("close");
        return 1;
    }

    char * end = nullptr;
    ssize_t decompressor_size = strtol(size_str, &end, 10);
    if (*end != 0)
    {
        std::cerr << "Error: unable to extract decompressor" << std::endl;
        if (0 != close(input_fd))
            perror("close");
        return 1;
    }

    if (-1 == lseek(input_fd, -(decompressor_size + 15), SEEK_END))
    {
        perror("lseek");
        if (0 != close(input_fd))
            perror("close");
        return 1;
    }

    int ret = copy_decompressor(input_fd, decompressor_size, output_fd);
    if (0 != close(input_fd))
        perror("close");
    return ret;
}

static int copy_decompressor_file(const char *path, int output_fd)
{
    struct stat info_in{};
    if (stat(path, &info_in) != 0)
    {
        std::cerr << "Error: decompressor file [" << path << "]." << std::endl;
        perror("stat");
        return 1;
    }

    if (!S_ISREG(info_in.st_mode))
    {
        std::cerr << "Error: decompressor path [" << path << "] is not a file." << std::endl;
        return 1;
    }

    int input_fd = open(path, O_RDONLY);
    if (input_fd == -1)
    {
        perror("open");
        return 1;
    }

    int ret = copy_decompressor(input_fd, info_in.st_size, output_fd);
    if (0 != close(input_fd))
        perror("close");
    return ret;
}

inline void usage(FILE * out, const char * name)
{
    (void)fprintf(out,
        "%s [--level=<level>] [--decompressor=<path>] [--exec=<path>] <output_file> [<input_file> [... <input_file>]]\n"
        "\t--level - compression level, max is %d, negative - prefer speed over compression\n"
        "\t          default is 5\n"
        "\t--decompressor - path to decompressor\n"
        "\t--exec - path to an input file to execute after decompression, if omitted then\n"
        "\t         an <input_file> having the same name as <output_file> becomes such executable.\n"
        "\t         This executable upon decompression will substitute started compressed preserving compressed name.\n"
        "\t         If no <path> is specified - nothing will be run - only decompression will be performed.\n",
        name, ZSTD_maxCLevel());
}

static const char * get_param(int argc, char * const argv[], const char * name)
{
    if (nullptr == name || name[0] == 0)
        return nullptr;

    for (int i = 1; i < argc; ++i)
    {
        const char * arg = argv[i];
        if (arg[0] != '-' || arg[1] != '-')
            return nullptr;

        size_t arg_len = strlen(arg);
        size_t name_len = strlen(name);

        const char * eq = strchr(arg + 2, '=');
        if (nullptr == eq)
            eq = arg + arg_len;

        if (name_len != static_cast<size_t>(eq - arg - 2))
            continue;

        if (0 == memcmp(name, arg + 2, name_len))
            return *eq == 0 ? eq : eq + 1;
    }

    return nullptr;
}

int main(int argc, char* argv[])
{
    /// musl defines `stdout` and `stderr` as recursive macros `(stdout)` / `(stderr)`,
    /// which trigger `-Wdisabled-macro-expansion` when used as function arguments.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdisabled-macro-expansion"
    if (argc == 1)
    {
        usage(stdout, argv[0]);
        return 0;
    }

    int start_of_files = 1;

    /// Set compression level
    int level = 5;
    const char * p = get_param(argc, argv, "level");
    if (p != nullptr)
    {
        if (p[0] != 0)
        {
            char * end = nullptr;
            level = static_cast<int>(strtol(p, &end, 10));
            if (*end != 0)
            {
                std::cerr << "Error: level [" << p << "] is not valid" << std::endl;
                usage(stderr, argv[0]);
                return 1;
            }
        }
        ++start_of_files;
    }

    /// Set decompressor
    const char * decompressor = get_param(argc, argv, "decompressor");
    if (decompressor != nullptr)
    {
        if (decompressor[0] == 0)
            decompressor = nullptr;
        ++start_of_files;
    }

    /// Specified executable
    const char * exec = get_param(argc, argv, "exec");
    if (exec != nullptr)
        ++start_of_files;

    if (argc < start_of_files + (exec == nullptr || *exec == 0 ? 1 : 0))
    {
        usage(stderr, argv[0]);
        return 1;
    }
#pragma clang diagnostic pop

    struct stat info_out{};
    if (stat(argv[start_of_files], &info_out) != -1 || errno != ENOENT)
    {
        std::cerr << "Error: output file [" << argv[start_of_files] << "] already exists" << std::endl;
        return 1;
    }

    int output_fd = open(argv[start_of_files], O_RDWR | O_CREAT, 0775);
    if (output_fd == -1)
    {
        perror("open");
        return 1;
    }

    const char* out_name = strrchr(argv[start_of_files], '/');
    if (out_name)
        ++out_name;
    else
        out_name = argv[start_of_files];
    ++start_of_files;

    if (decompressor != nullptr)
    {
        if (copy_decompressor_file(decompressor, output_fd))
            return 1;
    }
    else
    {
        if (copy_decompressor_self(argv[0], output_fd))
            return 1;
    }

    if (0 != fstat(output_fd, &info_out))
    {
        perror("fstat");
        return 1;
    }

    std::cout << "Compression with level: " << level << std::endl;
    if (0 != compressFiles(out_name, exec, &argv[start_of_files], argc - start_of_files, output_fd, level, info_out))
    {
        printf("Compression failed.\n"); // NOLINT(modernize-use-std-print)
        if (0 != close(output_fd))
            perror("close");
        unlink(argv[start_of_files - 1]);
        return 1;
    }

    printf("Successfully compressed.\n"); // NOLINT(modernize-use-std-print)

    if (0 != close(output_fd))
        perror("close");

    return 0;
}
