#pragma once

/* Assembler macros for x86.
    Copyright (C) 2017-2020 Free Software Foundation, Inc.
    This file is part of the GNU C Library.

    The GNU C Library is free software; you can redistribute it and/or
    modify it under the terms of the GNU Lesser General Public
    License as published by the Free Software Foundation; either
    version 2.1 of the License, or (at your option) any later version.

    The GNU C Library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
    Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public
    License along with the GNU C Library; if not, see
    <https://www.gnu.org/licenses/>.  */

#ifndef _X86_SYSDEP_H
#define _X86_SYSDEP_H 1

#include "sysdep_generic.h"

/* __CET__ is defined by GCC with Control-Flow Protection values:

enum cf_protection_level
{
    CF_NONE = 0,
    CF_BRANCH = 1 << 0,
    CF_RETURN = 1 << 1,
    CF_FULL = CF_BRANCH | CF_RETURN,
    CF_SET = 1 << 2
};
*/

/* Set if CF_BRANCH (IBT) is enabled.  */
#define X86_FEATURE_1_IBT    (1U << 0)
/* Set if CF_RETURN (SHSTK) is enabled.  */
#define X86_FEATURE_1_SHSTK    (1U << 1)

#ifdef __CET__
# define CET_ENABLED    1
# define IBT_ENABLED    (__CET__ & X86_FEATURE_1_IBT)
# define SHSTK_ENABLED    (__CET__ & X86_FEATURE_1_SHSTK)
#else
# define CET_ENABLED    0
# define IBT_ENABLED    0
# define SHSTK_ENABLED    0
#endif

/* Offset for fxsave/xsave area used by _dl_runtime_resolve.  Also need
    space to preserve RCX, RDX, RSI, RDI, R8, R9 and RAX.  It must be
    aligned to 16 bytes for fxsave and 64 bytes for xsave.  */
#define STATE_SAVE_OFFSET (8 * 7 + 8)

/* Save SSE, AVX, AVX512, mask and bound registers.  */
#define STATE_SAVE_MASK \
    ((1 << 1) | (1 << 2) | (1 << 3) | (1 << 5) | (1 << 6) | (1 << 7))

#ifdef    __ASSEMBLER__

/* Syntactic details of assembler.  */

#ifdef _CET_ENDBR
# define _CET_NOTRACK notrack
#else
# define _CET_ENDBR
# define _CET_NOTRACK
#endif

/* ELF uses byte-counts for .align, most others use log2 of count of bytes.  */
#define ALIGNARG(log2) 1<<log2
#define ASM_SIZE_DIRECTIVE(name) .size name,.-name;

/* Define an entry point visible from C.  */
#define    ENTRY(name)                                  \
    .globl C_SYMBOL_NAME(name);                              \
    .type C_SYMBOL_NAME(name),@function;                          \
    .align ALIGNARG(4);                                  \
    C_LABEL(name)                                      \
    cfi_startproc;                                  \
    _CET_ENDBR;                                      \
    CALL_MCOUNT

#undef    END
#define END(name)                                  \
    cfi_endproc;                                      \
    ASM_SIZE_DIRECTIVE(name)

#define ENTRY_CHK(name) ENTRY (name)
#define END_CHK(name) END (name)

/* Since C identifiers are not normally prefixed with an underscore
    on this system, the asm identifier `syscall_error' intrudes on the
    C name space.  Make sure we use an innocuous name.  */
#define    syscall_error    __syscall_error
#define mcount        _mcount

#undef    PSEUDO_END
#define    PSEUDO_END(name)                              \
    END (name)

/* Local label name for asm code. */
#ifndef L
/* ELF-like local names start with `.L'.  */
# define L(name)    .L##name
#endif

#define atom_text_section .section ".text.atom", "ax"

#endif    /* __ASSEMBLER__ */

#endif    /* _X86_SYSDEP_H */
