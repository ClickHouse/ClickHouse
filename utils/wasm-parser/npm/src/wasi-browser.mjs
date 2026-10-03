/// Preview1 imports for a browser or worker. The module is a WASI reactor: it exports
/// `_initialize`, not `_start`. Only the calls wasi-libc actually makes need to succeed;
/// everything else returns `ENOSYS` so a new import is visible rather than silently succeeding.

const ERRNO_SUCCESS = 0;
const ERRNO_BADF = 8;
const ERRNO_NOSYS = 52;

export async function instantiate(bytes)
{
    const memoryRef = { memory: null };

    function view()
    {
        return new DataView(memoryRef.memory.buffer);
    }

    function u32(ptr, value)
    {
        view().setUint32(ptr, value, true);
    }

    function u64(ptr, value)
    {
        view().setBigUint64(ptr, BigInt(value), true);
    }

    const preview1 = {
        args_get: () => ERRNO_SUCCESS,
        args_sizes_get: (argc, buf_size) =>
        {
            u32(argc, 0);
            u32(buf_size, 0);
            return ERRNO_SUCCESS;
        },
        environ_get: () => ERRNO_SUCCESS,
        environ_sizes_get: (count, buf_size) =>
        {
            u32(count, 0);
            u32(buf_size, 0);
            return ERRNO_SUCCESS;
        },
        clock_res_get: (_id, resolution) =>
        {
            u64(resolution, 1000);
            return ERRNO_SUCCESS;
        },
        clock_time_get: (_id, _precision, time) =>
        {
            u64(time, BigInt(Date.now()) * 1000000n);
            return ERRNO_SUCCESS;
        },
        fd_advise: () => ERRNO_SUCCESS,
        fd_allocate: () => ERRNO_NOSYS,
        fd_close: () => ERRNO_SUCCESS,
        fd_datasync: () => ERRNO_SUCCESS,
        fd_fdstat_get: (fd, stat) =>
        {
            const data = view();
            data.setUint8(stat, fd < 3 ? 2 : 3);
            data.setUint16(stat + 2, 0, true);
            u64(stat + 8, 0);
            u64(stat + 16, 0);
            return ERRNO_SUCCESS;
        },
        fd_fdstat_set_flags: () => ERRNO_SUCCESS,
        fd_filestat_get: () => ERRNO_NOSYS,
        fd_filestat_set_size: () => ERRNO_NOSYS,
        fd_filestat_set_times: () => ERRNO_NOSYS,
        fd_pread: () => ERRNO_NOSYS,
        fd_prestat_get: () => ERRNO_BADF,
        fd_prestat_dir_name: () => ERRNO_BADF,
        fd_pwrite: () => ERRNO_NOSYS,
        fd_read: () => ERRNO_NOSYS,
        fd_readdir: () => ERRNO_NOSYS,
        fd_renumber: () => ERRNO_NOSYS,
        fd_seek: (_fd, _offset, _whence, newoffset) =>
        {
            u64(newoffset, 0);
            return ERRNO_SUCCESS;
        },
        fd_sync: () => ERRNO_SUCCESS,
        fd_tell: (_fd, offset) =>
        {
            u64(offset, 0);
            return ERRNO_SUCCESS;
        },
        fd_write: (_fd, iovs, iovs_len, nwritten) =>
        {
            const data = view();
            let written = 0;
            for (let i = 0; i < iovs_len; i++)
            {
                written += data.getUint32(iovs + i * 8 + 4, true);
            }
            u32(nwritten, written);
            return ERRNO_SUCCESS;
        },
        path_create_directory: () => ERRNO_NOSYS,
        path_filestat_get: () => ERRNO_NOSYS,
        path_filestat_set_times: () => ERRNO_NOSYS,
        path_link: () => ERRNO_NOSYS,
        path_open: () => ERRNO_NOSYS,
        path_readlink: () => ERRNO_NOSYS,
        path_remove_directory: () => ERRNO_NOSYS,
        path_rename: () => ERRNO_NOSYS,
        path_symlink: () => ERRNO_NOSYS,
        path_unlink_file: () => ERRNO_NOSYS,
        poll_oneoff: () => ERRNO_NOSYS,
        proc_exit: (code) =>
        {
            throw new Error(`WASI proc_exit(${code})`);
        },
        proc_raise: () => ERRNO_NOSYS,
        random_get: (ptr, len) =>
        {
            const bytes = new Uint8Array(memoryRef.memory.buffer, ptr, len);
            globalThis.crypto.getRandomValues(bytes);
            return ERRNO_SUCCESS;
        },
        sched_yield: () => ERRNO_SUCCESS,
        sock_accept: () => ERRNO_NOSYS,
        sock_recv: () => ERRNO_NOSYS,
        sock_send: () => ERRNO_NOSYS,
        sock_shutdown: () => ERRNO_NOSYS,
    };

    const { instance } = await WebAssembly.instantiate(bytes, {
        wasi_snapshot_preview1: preview1,
    });
    memoryRef.memory = instance.exports.memory;
    if (typeof instance.exports._initialize === 'function')
        instance.exports._initialize();
    return instance;
}
