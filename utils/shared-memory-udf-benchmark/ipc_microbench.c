// Micro-benchmark of the raw IPC primitives that an executable-UDF transport could be built on.
// It measures the throughput of moving a large buffer from a parent to a child process, one chunk
// at a time, strictly synchronized (the parent waits for the child to consume each chunk before
// sending the next) — the same lock-step pattern the shared-memory UDF protocol uses.
//
// Mechanisms compared:
//   pipe        - bulk data goes through an anonymous pipe (two kernel copies per chunk);
//   tmpfs-mmap  - bulk in an mmap'd file under /dev/shm; a 1-byte pipe carries the control signal
//                 (this is what the shared-memory UDF transport does);
//   memfd-mmap  - same, but the shared memory is an anonymous memfd_create() region the child
//                 inherits (no filesystem path);
//   vmsplice    - the parent maps its buffer pages straight into a pipe with vmsplice(), saving the
//                 copy a write() makes on the sending side, and the child read()s them out.
//
// The parent's buffer is deliberately NOT donated with SPLICE_F_GIFT: gifted pages may be stolen by
// the kernel, so they could not be reused by the next iteration, and the receiving end can only
// avoid the second copy by splicing the pages somewhere it never looks at them (e.g. /dev/null) -
// which is not something a UDF that has to consume its input can do.
//
// For every mechanism the child actually reads all the bytes (touches the data), so the numbers
// reflect transfer + consume, not just signaling.
//
// Build:  cc -O2 -o ipc_microbench ipc_microbench.c
// Run:    ./ipc_microbench [chunk_bytes] [iterations]

#define _GNU_SOURCE
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/uio.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static size_t CHUNK = 8u << 20; // 8 MiB
static size_t ITERS = 256;

static double now_sec(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

// A chunk size that does not fit into memory must fail loudly here, not by writing through the
// NULL that `malloc` returned in the memset that follows every call. `alignment` of 0 means plain
// `malloc`; otherwise the size is rounded up, because `aligned_alloc` wants a multiple of the
// alignment and the chunk size comes from the command line.
static void *alloc_or_die(size_t size, size_t alignment)
{
    void *result = NULL;
    if (!alignment)
        result = malloc(size);
    else {
        size_t rounded = size + (alignment - size % alignment) % alignment;
        if (rounded >= size) result = aligned_alloc(alignment, rounded);
    }
    if (!result) { fprintf(stderr, "Cannot allocate %zu bytes\n", size); exit(1); }
    return result;
}

static void full_write(int fd, const void *buf, size_t n)
{
    const char *p = buf;
    while (n) { ssize_t w = write(fd, p, n); if (w <= 0) { perror("write"); _exit(1); } p += w; n -= (size_t)w; }
}

static void full_read(int fd, void *buf, size_t n)
{
    char *p = buf;
    while (n) { ssize_t r = read(fd, p, n); if (r <= 0) { perror("read"); _exit(1); } p += r; n -= (size_t)r; }
}

// Make the contents of a buffer observable to the compiler without adding another pass over it.
// The benchmark is Linux/GNU-specific already (`memfd_create`, `vmsplice`), so an empty GNU asm
// with a memory clobber is appropriate here. In particular, this prevents the child-side memcpy in
// bench_shared from being removed as a dead store before the process exits.
static void consume_buffer(const void *buf)
{
    __asm__ __volatile__("" : : "r"(buf) : "memory");
}

static void report(const char *name, double secs, int crosses_kernel)
{
    double gib = (double)CHUNK * (double)ITERS / (1u << 30);
    printf("%-12s %8.3f s   %8.2f GiB/s   bulk via syscalls: %s\n",
           name, secs, gib / secs, crosses_kernel ? "yes" : "no");
}

// -------- pipe: bulk through a data pipe, child acks via a control pipe --------
static void bench_pipe(void)
{
    int data[2], ack[2];
    if (pipe(data) || pipe(ack)) { perror("pipe"); exit(1); }
    char *buf = alloc_or_die(CHUNK, 0), *rcv = alloc_or_die(CHUNK, 0);
    memset(buf, 'x', CHUNK);

    pid_t pid = fork();
    if (pid == 0) {
        close(data[1]); close(ack[0]);
        for (size_t i = 0; i < ITERS; i++) { full_read(data[0], rcv, CHUNK); full_write(ack[1], "x", 1); }
        _exit(0);
    }
    close(data[0]); close(ack[1]);
    double t0 = now_sec();
    for (size_t i = 0; i < ITERS; i++) { char c; full_write(data[1], buf, CHUNK); full_read(ack[0], &c, 1); }
    double t1 = now_sec();
    close(data[1]); close(ack[0]); waitpid(pid, NULL, 0);
    free(buf); free(rcv);
    report("pipe", t1 - t0, 1);
}

// -------- shared mmap (tmpfs file or memfd): bulk in shared memory, 1-byte control pipe --------
static void bench_shared(const char *name, int fd)
{
    if (ftruncate(fd, (off_t)CHUNK)) { perror("ftruncate"); exit(1); }
    void *region = mmap(NULL, CHUNK, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (region == MAP_FAILED) { perror("mmap"); exit(1); }
    int go[2], ack[2];
    if (pipe(go) || pipe(ack)) { perror("pipe"); exit(1); }
    char *buf = alloc_or_die(CHUNK, 0), *rcv = alloc_or_die(CHUNK, 0);
    memset(buf, 'x', CHUNK);

    pid_t pid = fork();
    if (pid == 0) {
        close(go[1]); close(ack[0]);
        for (size_t i = 0; i < ITERS; i++) { char c; full_read(go[0], &c, 1); memcpy(rcv, region, CHUNK); consume_buffer(rcv); full_write(ack[1], "x", 1); }
        _exit(0);
    }
    close(go[0]); close(ack[1]);
    double t0 = now_sec();
    for (size_t i = 0; i < ITERS; i++) { char c; memcpy(region, buf, CHUNK); full_write(go[1], "x", 1); full_read(ack[0], &c, 1); }
    double t1 = now_sec();
    close(go[1]); close(ack[0]); waitpid(pid, NULL, 0);
    munmap(region, CHUNK); free(buf); free(rcv);
    report(name, t1 - t0, 0);
}

// -------- vmsplice: map buffer pages into a pipe, child reads them out --------
static void bench_vmsplice(void)
{
    int data[2], ack[2];
    if (pipe(data) || pipe(ack)) { perror("pipe"); exit(1); }
    char *buf = alloc_or_die(CHUNK, 4096), *rcv = alloc_or_die(CHUNK, 0);
    memset(buf, 'x', CHUNK);

    pid_t pid = fork();
    if (pid == 0) {
        close(data[1]); close(ack[0]);
        // Like every other mechanism here, the child actually consumes all the bytes.
        for (size_t i = 0; i < ITERS; i++) { full_read(data[0], rcv, CHUNK); full_write(ack[1], "x", 1); }
        _exit(0);
    }
    close(data[0]); close(ack[1]);
    double t0 = now_sec();
    for (size_t i = 0; i < ITERS; i++) {
        char c; struct iovec iov = { buf, CHUNK };
        size_t left = CHUNK; char *p = buf;
        while (left) { iov.iov_base = p; iov.iov_len = left; ssize_t s = vmsplice(data[1], &iov, 1, 0); if (s <= 0) { perror("vmsplice"); _exit(1); } p += s; left -= (size_t)s; }
        full_read(ack[0], &c, 1);
    }
    double t1 = now_sec();
    close(data[1]); close(ack[0]); waitpid(pid, NULL, 0);
    free(buf); free(rcv);
    report("vmsplice", t1 - t0, 1);
}

int main(int argc, char **argv)
{
    if (argc > 1) CHUNK = strtoull(argv[1], NULL, 10);
    if (argc > 2) ITERS = strtoull(argv[2], NULL, 10);
    printf("chunk = %zu bytes, iterations = %zu, total = %.1f GiB per mechanism\n\n",
           CHUNK, ITERS, (double)CHUNK * ITERS / (1u << 30));

    bench_pipe();

    char path[] = "/dev/shm/ipc_microbench_XXXXXX";
    int tfd = mkstemp(path);
    if (tfd < 0) { perror("mkstemp"); return 1; }
    unlink(path);
    bench_shared("tmpfs-mmap", tfd);
    close(tfd);

    int mfd = memfd_create("ipc_microbench", 0);
    if (mfd < 0) { perror("memfd_create"); return 1; }
    bench_shared("memfd-mmap", mfd);
    close(mfd);

    bench_vmsplice();
    return 0;
}
