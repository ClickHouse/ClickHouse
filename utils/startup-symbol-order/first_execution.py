#!/usr/bin/env python3
"""The order in which a program executes its functions for the first time, exactly.

The program is started stopped under ptrace, a breakpoint instruction is written at the entry of every
function of its symbol table, and on each hit the function is recorded, its original instruction is
restored and the thread continues. Every function traps at most once, so the overhead is one stop per
executed function, and the result is the complete list of executed functions in the order of their first
execution - unlike sampling, which misses short functions.

Supports AArch64 and x86-64 Linux, and statically or dynamically linked position-independent executables.
Only the code of the executable itself is traced, not the shared libraries.

The program must not check the integrity of its own code (the breakpoints modify it) and must not fork
without exec (the child would inherit the breakpoints without a tracer). For `clickhouse server`, set
`skip_binary_checksum_checks` and the `CLICKHOUSE_WATCHDOG_ENABLE=0` environment variable.

Usage as a program: first_execution.py <executable> <output file> -- <arguments...>
"""

import ctypes
import mmap
import os
import signal
import struct
import sys
import threading

EM_X86_64 = 62
EM_AARCH64 = 183

PTRACE_TRACEME = 0
PTRACE_CONT = 7
PTRACE_SETOPTIONS = 0x4200
PTRACE_GETREGSET = 0x4204
PTRACE_SETREGSET = 0x4205
PTRACE_O_TRACECLONE = 0x8
PTRACE_O_EXITKILL = 0x100000
NT_PRSTATUS = 1
WALL = 0x40000000

SHT_SYMTAB = 2
SHF_EXECINSTR = 0x4
STT_FUNC = 2
PT_LOAD = 1


class _IOVec(ctypes.Structure):
    _fields_ = [("base", ctypes.c_void_p), ("len", ctypes.c_size_t)]


def read_functions(path):
    """Returns (machine, the PT_LOAD segments, {virtual address: [names]}) of the function symbols."""
    with open(path, "rb") as f:
        elf = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
    if elf[:4] != b"\x7fELF" or elf[4] != 2:
        raise ValueError(f"{path} is not a 64-bit ELF file")
    machine = struct.unpack_from("<H", elf, 0x12)[0]
    if machine not in (EM_X86_64, EM_AARCH64):
        raise ValueError(f"{path}: unsupported machine {machine}")
    phoff, shoff = struct.unpack_from("<QQ", elf, 0x20)
    phentsize, phnum, shentsize, shnum = struct.unpack_from("<HHHH", elf, 0x36)

    loads = []
    for i in range(phnum):
        p_type, _, p_offset, p_vaddr, _, p_filesz = struct.unpack_from("<IIQQQQ", elf, phoff + i * phentsize)
        if p_type == PT_LOAD:
            loads.append((p_vaddr, p_offset, p_filesz))

    sections = [struct.unpack_from("<IIQQQQIIQQ", elf, shoff + i * shentsize) for i in range(shnum)]
    symtabs = [s for s in sections if s[1] == SHT_SYMTAB]
    if not symtabs:
        raise ValueError(f"{path} has no symbol table")
    symtab = symtabs[0]
    strtab_offset = sections[symtab[6]][4]
    executable_sections = {i for i, s in enumerate(sections) if s[2] & SHF_EXECINSTR}

    functions = {}
    for offset in range(symtab[4], symtab[4] + symtab[5], 24):
        st_name, st_info, _, st_shndx, st_value, st_size = struct.unpack_from("<IBBHQQ", elf, offset)
        if (st_info & 0xF) != STT_FUNC or st_size == 0 or st_shndx not in executable_sections:
            continue
        if machine == EM_AARCH64 and st_value % 4:
            continue
        name_begin = strtab_offset + st_name
        name = elf[name_begin:elf.find(b"\0", name_begin)].decode(errors="replace")
        functions.setdefault(st_value, []).append(name)
    return machine, loads, functions, elf


def trace(executable, args, env=None, cwd=None, on_started=None):
    """Runs `executable` with `args` and returns the names of its functions in the order of first execution.
    `on_started(pid)` is called once the program runs, from another thread, e.g. to stop it at some point."""
    machine, loads, functions, elf = read_functions(executable)
    if machine == EM_AARCH64:
        breakpoint_instruction = struct.pack("<I", 0xD4200000)  # BRK #0
        pc_index, pc_after_trap = 32, 0  # user_pt_regs: x0..x30, sp, pc; BRK leaves pc at the instruction
        registers = (ctypes.c_uint64 * 34)()
    else:
        breakpoint_instruction = b"\xcc"  # INT3
        pc_index, pc_after_trap = 16, 1  # user_regs_struct: rip; INT3 advances it past the instruction
        registers = (ctypes.c_uint64 * 27)()
    iov = _IOVec(ctypes.cast(registers, ctypes.c_void_p), ctypes.sizeof(registers))

    libc = ctypes.CDLL(None, use_errno=True)
    libc.ptrace.restype = ctypes.c_long
    libc.ptrace.argtypes = [ctypes.c_long, ctypes.c_long, ctypes.c_void_p, ctypes.c_void_p]

    def file_offset(vaddr):
        for p_vaddr, p_offset, p_filesz in loads:
            if p_vaddr <= vaddr < p_vaddr + p_filesz:
                return vaddr - p_vaddr + p_offset
        return None

    pid = os.fork()
    if pid == 0:
        try:
            if cwd:
                os.chdir(cwd)
            libc.ptrace(PTRACE_TRACEME, 0, None, None)
            os.execve(executable, [executable] + list(args), env if env is not None else os.environ)
        finally:
            os._exit(127)

    _, status = os.waitpid(pid, 0)
    if not (os.WIFSTOPPED(status) and os.WSTOPSIG(status) == signal.SIGTRAP):
        raise RuntimeError(f"{executable} did not stop at exec")
    libc.ptrace(PTRACE_SETOPTIONS, pid, None, PTRACE_O_TRACECLONE | PTRACE_O_EXITKILL)

    # The load address: the mapping of the file at offset 0 corresponds to the first PT_LOAD segment.
    real_path = os.path.realpath(executable)
    base = None
    with open(f"/proc/{pid}/maps") as maps:
        for line in maps:
            parts = line.split()
            if len(parts) >= 6 and parts[5] == real_path and int(parts[2], 16) == 0:
                base = int(parts[0].split("-")[0], 16) - (loads[0][0] & ~0xFFF)
                break
    if base is None:
        raise RuntimeError(f"{executable} is not mapped in the process")

    memory = os.open(f"/proc/{pid}/mem", os.O_RDWR)
    original = {}
    for vaddr in functions:
        offset = file_offset(vaddr)
        if offset is None:
            continue
        original[base + vaddr] = elf[offset:offset + len(breakpoint_instruction)]
        os.pwrite(memory, breakpoint_instruction, base + vaddr)

    if on_started:
        threading.Thread(target=on_started, args=(pid,), daemon=True).start()

    order = []
    restored = set()
    exit_status = None
    libc.ptrace(PTRACE_CONT, pid, None, 0)
    while True:
        try:
            tid, status = os.waitpid(-1, WALL)
        except ChildProcessError:
            break
        if not os.WIFSTOPPED(status):
            if tid == pid:
                exit_status = status
            continue
        sig = os.WSTOPSIG(status)
        if sig == signal.SIGTRAP and status >> 16:
            libc.ptrace(PTRACE_CONT, tid, None, 0)  # a clone event: the new thread reports its own stop
            continue
        if sig == signal.SIGTRAP:
            if libc.ptrace(PTRACE_GETREGSET, tid, NT_PRSTATUS, ctypes.byref(iov)) != 0:
                raise OSError(ctypes.get_errno(), "PTRACE_GETREGSET")
            address = registers[pc_index] - pc_after_trap
            if address in original:
                if address not in restored:
                    os.pwrite(memory, original[address], address)
                    restored.add(address)
                    order.extend(functions[address - base])
                if pc_after_trap:
                    registers[pc_index] = address
                    if libc.ptrace(PTRACE_SETREGSET, tid, NT_PRSTATUS, ctypes.byref(iov)) != 0:
                        raise OSError(ctypes.get_errno(), "PTRACE_SETREGSET")
                libc.ptrace(PTRACE_CONT, tid, None, 0)
                continue
            libc.ptrace(PTRACE_CONT, tid, None, sig)
            continue
        if sig == signal.SIGSTOP:
            libc.ptrace(PTRACE_CONT, tid, None, 0)  # a new thread starts with SIGSTOP
            continue
        libc.ptrace(PTRACE_CONT, tid, None, sig)
    os.close(memory)
    # A failed workload executes a different (error) path: its order must not be accepted.
    if exit_status is None:
        raise RuntimeError(f"{executable}: the exit status of the process is unknown")
    if os.WIFSIGNALED(exit_status):
        raise RuntimeError(f"{executable} was terminated by signal {os.WTERMSIG(exit_status)}")
    if os.WEXITSTATUS(exit_status) != 0:
        raise RuntimeError(f"{executable} exited with code {os.WEXITSTATUS(exit_status)}")
    return order


def main():
    if len(sys.argv) < 4 or sys.argv[3] != "--":
        sys.exit(f"Usage: {sys.argv[0]} <executable> <output file> -- <arguments...>")
    order = trace(sys.argv[1], sys.argv[4:])
    with open(sys.argv[2], "w") as f:
        f.write("".join(name + "\n" for name in dict.fromkeys(order)))


if __name__ == "__main__":
    main()
