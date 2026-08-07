/* memcpy with SSSE3
   Copyright (C) 2010-2020 Free Software Foundation, Inc.
   Contributed by Intel Corporation.
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

#include "sysdep.h"

#if 1

#include "asm-syntax.h"

#ifndef MEMCPY
# define MEMCPY        __memcpy_ssse3
# define MEMCPY_CHK    __memcpy_chk_ssse3
# define MEMPCPY    __mempcpy_ssse3
# define MEMPCPY_CHK    __mempcpy_chk_ssse3
#endif

#define JMPTBL(I, B)    I - B

/* Branch to an entry in a jump table.  TABLE is a jump table with
   relative offsets.  INDEX is a register contains the index into the
   jump table.  SCALE is the scale of INDEX.  */
#define BRANCH_TO_JMPTBL_ENTRY(TABLE, INDEX, SCALE)        \
  lea        TABLE(%rip), %r11;                \
  movslq    (%r11, INDEX, SCALE), INDEX;            \
  lea        (%r11, INDEX), INDEX;                \
  _CET_NOTRACK jmp *INDEX;                    \
  ud2

    .section .text.ssse3,"ax",@progbits
#if !defined USE_AS_MEMPCPY && !defined USE_AS_MEMMOVE
ENTRY (MEMPCPY_CHK)
    cmp    %RDX_LP, %RCX_LP
    jb    HIDDEN_JUMPTARGET (__chk_fail)
END (MEMPCPY_CHK)

ENTRY (MEMPCPY)
    mov    %RDI_LP, %RAX_LP
    add    %RDX_LP, %RAX_LP
    jmp    L(start)
END (MEMPCPY)
#endif

#if !defined USE_AS_BCOPY
ENTRY (MEMCPY_CHK)
    cmp    %RDX_LP, %RCX_LP
    jb    HIDDEN_JUMPTARGET (__chk_fail)
END (MEMCPY_CHK)
#endif

ENTRY (MEMCPY)
    mov    %RDI_LP, %RAX_LP
#ifdef USE_AS_MEMPCPY
    add    %RDX_LP, %RAX_LP
#endif

#ifdef __ILP32__
    /* Clear the upper 32 bits.  */
    mov    %edx, %edx
#endif

#ifdef USE_AS_MEMMOVE
    cmp    %rsi, %rdi
    jb    L(copy_forward)
    je    L(write_0bytes)
    cmp    $79, %rdx
    jbe    L(copy_forward)
    jmp    L(copy_backward)
L(copy_forward):
#endif
L(start):
    cmp    $79, %rdx
    lea     L(table_less_80bytes)(%rip), %r11
    ja    L(80bytesormore)
    movslq    (%r11, %rdx, 4), %r9
    add    %rdx, %rsi
    add    %rdx, %rdi
    add    %r11, %r9
    _CET_NOTRACK jmp *%r9
    ud2

    .p2align 4
L(80bytesormore):
#ifndef USE_AS_MEMMOVE
    cmp    %dil, %sil
    jle    L(copy_backward)
#endif

    movdqu    (%rsi), %xmm0
    mov    %rdi, %rcx
    and    $-16, %rdi
    add    $16, %rdi
    mov    %rcx, %r8
    sub    %rdi, %rcx
    add    %rcx, %rdx
    sub    %rcx, %rsi

#ifdef SHARED_CACHE_SIZE_HALF
    mov    $SHARED_CACHE_SIZE_HALF, %RCX_LP
#else
    mov    __x86_shared_cache_size_half(%rip), %RCX_LP
#endif
    cmp    %rcx, %rdx
    mov    %rsi, %r9
    ja    L(large_page_fwd)
    and    $0xf, %r9
    jz    L(shl_0)
#ifdef DATA_CACHE_SIZE_HALF
    mov    $DATA_CACHE_SIZE_HALF, %RCX_LP
#else
    mov    __x86_data_cache_size_half(%rip), %RCX_LP
#endif
    BRANCH_TO_JMPTBL_ENTRY (L(shl_table), %r9, 4)

    .p2align 4
L(copy_backward):
    movdqu    -16(%rsi, %rdx), %xmm0
    add    %rdx, %rsi
    lea    -16(%rdi, %rdx), %r8
    add    %rdx, %rdi

    mov    %rdi, %rcx
    and    $0xf, %rcx
    xor    %rcx, %rdi
    sub    %rcx, %rdx
    sub    %rcx, %rsi

#ifdef SHARED_CACHE_SIZE_HALF
    mov    $SHARED_CACHE_SIZE_HALF, %RCX_LP
#else
    mov    __x86_shared_cache_size_half(%rip), %RCX_LP
#endif

    cmp    %rcx, %rdx
    mov    %rsi, %r9
    ja    L(large_page_bwd)
    and    $0xf, %r9
    jz    L(shl_0_bwd)
#ifdef DATA_CACHE_SIZE_HALF
    mov    $DATA_CACHE_SIZE_HALF, %RCX_LP
#else
    mov    __x86_data_cache_size_half(%rip), %RCX_LP
#endif
    BRANCH_TO_JMPTBL_ENTRY (L(shl_table_bwd), %r9, 4)

    .p2align 4
L(shl_0):
    sub    $16, %rdx
    movdqa    (%rsi), %xmm1
    add    $16, %rsi
    movdqa    %xmm1, (%rdi)
    add    $16, %rdi
    cmp    $128, %rdx
    movdqu    %xmm0, (%r8)
    ja    L(shl_0_gobble)
    cmp    $64, %rdx
    jb    L(shl_0_less_64bytes)
    movaps    (%rsi), %xmm4
    movaps    16(%rsi), %xmm1
    movaps    32(%rsi), %xmm2
    movaps    48(%rsi), %xmm3
    movaps    %xmm4, (%rdi)
    movaps    %xmm1, 16(%rdi)
    movaps    %xmm2, 32(%rdi)
    movaps    %xmm3, 48(%rdi)
    sub    $64, %rdx
    add    $64, %rsi
    add    $64, %rdi
L(shl_0_less_64bytes):
    add    %rdx, %rsi
    add    %rdx, %rdi
    BRANCH_TO_JMPTBL_ENTRY (L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_0_gobble):
#ifdef DATA_CACHE_SIZE_HALF
    cmp    $DATA_CACHE_SIZE_HALF, %RDX_LP
#else
    cmp    __x86_data_cache_size_half(%rip), %RDX_LP
#endif
    lea    -128(%rdx), %rdx
    jae    L(shl_0_gobble_mem_loop)
L(shl_0_gobble_cache_loop):
    movdqa    (%rsi), %xmm4
    movaps    0x10(%rsi), %xmm1
    movaps    0x20(%rsi), %xmm2
    movaps    0x30(%rsi), %xmm3

    movdqa    %xmm4, (%rdi)
    movaps    %xmm1, 0x10(%rdi)
    movaps    %xmm2, 0x20(%rdi)
    movaps    %xmm3, 0x30(%rdi)

    sub    $128, %rdx
    movaps    0x40(%rsi), %xmm4
    movaps    0x50(%rsi), %xmm5
    movaps    0x60(%rsi), %xmm6
    movaps    0x70(%rsi), %xmm7
    lea    0x80(%rsi), %rsi
    movaps    %xmm4, 0x40(%rdi)
    movaps    %xmm5, 0x50(%rdi)
    movaps    %xmm6, 0x60(%rdi)
    movaps    %xmm7, 0x70(%rdi)
    lea    0x80(%rdi), %rdi

    jae    L(shl_0_gobble_cache_loop)
    cmp    $-0x40, %rdx
    lea    0x80(%rdx), %rdx
    jl    L(shl_0_cache_less_64bytes)

    movdqa    (%rsi), %xmm4
    sub    $0x40, %rdx
    movdqa    0x10(%rsi), %xmm1

    movdqa    %xmm4, (%rdi)
    movdqa    %xmm1, 0x10(%rdi)

    movdqa    0x20(%rsi), %xmm4
    movdqa    0x30(%rsi), %xmm1
    add    $0x40, %rsi

    movdqa    %xmm4, 0x20(%rdi)
    movdqa    %xmm1, 0x30(%rdi)
    add    $0x40, %rdi
L(shl_0_cache_less_64bytes):
    add    %rdx, %rsi
    add    %rdx, %rdi
    BRANCH_TO_JMPTBL_ENTRY (L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_0_gobble_mem_loop):
    prefetcht0 0x1c0(%rsi)
    prefetcht0 0x280(%rsi)

    movdqa    (%rsi), %xmm0
    movdqa    0x10(%rsi), %xmm1
    movdqa    0x20(%rsi), %xmm2
    movdqa    0x30(%rsi), %xmm3
    movdqa    0x40(%rsi), %xmm4
    movdqa    0x50(%rsi), %xmm5
    movdqa    0x60(%rsi), %xmm6
    movdqa    0x70(%rsi), %xmm7
    lea    0x80(%rsi), %rsi
    sub    $0x80, %rdx
    movdqa    %xmm0, (%rdi)
    movdqa    %xmm1, 0x10(%rdi)
    movdqa    %xmm2, 0x20(%rdi)
    movdqa    %xmm3, 0x30(%rdi)
    movdqa    %xmm4, 0x40(%rdi)
    movdqa    %xmm5, 0x50(%rdi)
    movdqa    %xmm6, 0x60(%rdi)
    movdqa    %xmm7, 0x70(%rdi)
    lea    0x80(%rdi), %rdi

    jae    L(shl_0_gobble_mem_loop)
    cmp    $-0x40, %rdx
    lea    0x80(%rdx), %rdx
    jl    L(shl_0_mem_less_64bytes)

    movdqa    (%rsi), %xmm0
    sub    $0x40, %rdx
    movdqa    0x10(%rsi), %xmm1

    movdqa    %xmm0, (%rdi)
    movdqa    %xmm1, 0x10(%rdi)

    movdqa    0x20(%rsi), %xmm0
    movdqa    0x30(%rsi), %xmm1
    add    $0x40, %rsi

    movdqa    %xmm0, 0x20(%rdi)
    movdqa    %xmm1, 0x30(%rdi)
    add    $0x40, %rdi
L(shl_0_mem_less_64bytes):
    cmp    $0x20, %rdx
    jb    L(shl_0_mem_less_32bytes)
    movdqa    (%rsi), %xmm0
    sub    $0x20, %rdx
    movdqa    0x10(%rsi), %xmm1
    add    $0x20, %rsi
    movdqa    %xmm0, (%rdi)
    movdqa    %xmm1, 0x10(%rdi)
    add    $0x20, %rdi
L(shl_0_mem_less_32bytes):
    add    %rdx, %rdi
    add    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY (L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_0_bwd):
    sub    $16, %rdx
    movdqa    -0x10(%rsi), %xmm1
    sub    $16, %rsi
    movdqa    %xmm1, -0x10(%rdi)
    sub    $16, %rdi
    cmp    $0x80, %rdx
    movdqu    %xmm0, (%r8)
    ja    L(shl_0_gobble_bwd)
    cmp    $64, %rdx
    jb    L(shl_0_less_64bytes_bwd)
    movaps    -0x10(%rsi), %xmm0
    movaps    -0x20(%rsi), %xmm1
    movaps    -0x30(%rsi), %xmm2
    movaps    -0x40(%rsi), %xmm3
    movaps    %xmm0, -0x10(%rdi)
    movaps    %xmm1, -0x20(%rdi)
    movaps    %xmm2, -0x30(%rdi)
    movaps    %xmm3, -0x40(%rdi)
    sub    $64, %rdx
    sub    $0x40, %rsi
    sub    $0x40, %rdi
L(shl_0_less_64bytes_bwd):
    BRANCH_TO_JMPTBL_ENTRY (L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_0_gobble_bwd):
#ifdef DATA_CACHE_SIZE_HALF
    cmp    $DATA_CACHE_SIZE_HALF, %RDX_LP
#else
    cmp    __x86_data_cache_size_half(%rip), %RDX_LP
#endif
    lea    -128(%rdx), %rdx
    jae    L(shl_0_gobble_mem_bwd_loop)
L(shl_0_gobble_bwd_loop):
    movdqa    -0x10(%rsi), %xmm0
    movaps    -0x20(%rsi), %xmm1
    movaps    -0x30(%rsi), %xmm2
    movaps    -0x40(%rsi), %xmm3

    movdqa    %xmm0, -0x10(%rdi)
    movaps    %xmm1, -0x20(%rdi)
    movaps    %xmm2, -0x30(%rdi)
    movaps    %xmm3, -0x40(%rdi)

    sub    $0x80, %rdx
    movaps    -0x50(%rsi), %xmm4
    movaps    -0x60(%rsi), %xmm5
    movaps    -0x70(%rsi), %xmm6
    movaps    -0x80(%rsi), %xmm7
    lea    -0x80(%rsi), %rsi
    movaps    %xmm4, -0x50(%rdi)
    movaps    %xmm5, -0x60(%rdi)
    movaps    %xmm6, -0x70(%rdi)
    movaps    %xmm7, -0x80(%rdi)
    lea    -0x80(%rdi), %rdi

    jae    L(shl_0_gobble_bwd_loop)
    cmp    $-0x40, %rdx
    lea    0x80(%rdx), %rdx
    jl    L(shl_0_gobble_bwd_less_64bytes)

    movdqa    -0x10(%rsi), %xmm0
    sub    $0x40, %rdx
    movdqa    -0x20(%rsi), %xmm1

    movdqa    %xmm0, -0x10(%rdi)
    movdqa    %xmm1, -0x20(%rdi)

    movdqa    -0x30(%rsi), %xmm0
    movdqa    -0x40(%rsi), %xmm1
    sub    $0x40, %rsi

    movdqa    %xmm0, -0x30(%rdi)
    movdqa    %xmm1, -0x40(%rdi)
    sub    $0x40, %rdi
L(shl_0_gobble_bwd_less_64bytes):
    BRANCH_TO_JMPTBL_ENTRY (L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_0_gobble_mem_bwd_loop):
    prefetcht0 -0x1c0(%rsi)
    prefetcht0 -0x280(%rsi)
    movdqa    -0x10(%rsi), %xmm0
    movdqa    -0x20(%rsi), %xmm1
    movdqa    -0x30(%rsi), %xmm2
    movdqa    -0x40(%rsi), %xmm3
    movdqa    -0x50(%rsi), %xmm4
    movdqa    -0x60(%rsi), %xmm5
    movdqa    -0x70(%rsi), %xmm6
    movdqa    -0x80(%rsi), %xmm7
    lea    -0x80(%rsi), %rsi
    sub    $0x80, %rdx
    movdqa    %xmm0, -0x10(%rdi)
    movdqa    %xmm1, -0x20(%rdi)
    movdqa    %xmm2, -0x30(%rdi)
    movdqa    %xmm3, -0x40(%rdi)
    movdqa    %xmm4, -0x50(%rdi)
    movdqa    %xmm5, -0x60(%rdi)
    movdqa    %xmm6, -0x70(%rdi)
    movdqa    %xmm7, -0x80(%rdi)
    lea    -0x80(%rdi), %rdi

    jae    L(shl_0_gobble_mem_bwd_loop)
    cmp    $-0x40, %rdx
    lea    0x80(%rdx), %rdx
    jl    L(shl_0_mem_bwd_less_64bytes)

    movdqa    -0x10(%rsi), %xmm0
    sub    $0x40, %rdx
    movdqa    -0x20(%rsi), %xmm1

    movdqa    %xmm0, -0x10(%rdi)
    movdqa    %xmm1, -0x20(%rdi)

    movdqa    -0x30(%rsi), %xmm0
    movdqa    -0x40(%rsi), %xmm1
    sub    $0x40, %rsi

    movdqa    %xmm0, -0x30(%rdi)
    movdqa    %xmm1, -0x40(%rdi)
    sub    $0x40, %rdi
L(shl_0_mem_bwd_less_64bytes):
    cmp    $0x20, %rdx
    jb    L(shl_0_mem_bwd_less_32bytes)
    movdqa    -0x10(%rsi), %xmm0
    sub    $0x20, %rdx
    movdqa    -0x20(%rsi), %xmm1
    sub    $0x20, %rsi
    movdqa    %xmm0, -0x10(%rdi)
    movdqa    %xmm1, -0x20(%rdi)
    sub    $0x20, %rdi
L(shl_0_mem_bwd_less_32bytes):
    BRANCH_TO_JMPTBL_ENTRY (L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_1):
    lea    (L(shl_1_loop_L1)-L(shl_1))(%r9), %r9
    cmp    %rcx, %rdx
    movaps    -0x01(%rsi), %xmm1
    jb    L(L1_fwd)
    lea    (L(shl_1_loop_L2)-L(shl_1_loop_L1))(%r9), %r9
L(L1_fwd):
    lea    -64(%rdx), %rdx
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_1_loop_L2):
    prefetchnta 0x1c0(%rsi)
L(shl_1_loop_L1):
    sub    $64, %rdx
    movaps    0x0f(%rsi), %xmm2
    movaps    0x1f(%rsi), %xmm3
    movaps    0x2f(%rsi), %xmm4
    movaps    0x3f(%rsi), %xmm5
    movdqa    %xmm5, %xmm6
    palignr    $1, %xmm4, %xmm5
    lea    64(%rsi), %rsi
    palignr    $1, %xmm3, %xmm4
    palignr    $1, %xmm2, %xmm3
    lea    64(%rdi), %rdi
    palignr    $1, %xmm1, %xmm2
    movdqa    %xmm6, %xmm1
    movdqa    %xmm2, -0x40(%rdi)
    movaps    %xmm3, -0x30(%rdi)
    jb    L(shl_1_end)
    movaps    %xmm4, -0x20(%rdi)
    movaps    %xmm5, -0x10(%rdi)
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_1_end):
    movaps    %xmm4, -0x20(%rdi)
    lea    64(%rdx), %rdx
    movaps    %xmm5, -0x10(%rdi)
    add    %rdx, %rdi
    movdqu    %xmm0, (%r8)
    add    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY(L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_1_bwd):
    lea    (L(shl_1_bwd_loop_L1)-L(shl_1_bwd))(%r9), %r9
    cmp    %rcx, %rdx
    movaps    -0x01(%rsi), %xmm1
    jb    L(L1_bwd)
    lea    (L(shl_1_bwd_loop_L2)-L(shl_1_bwd_loop_L1))(%r9), %r9
L(L1_bwd):
    lea    -64(%rdx), %rdx
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_1_bwd_loop_L2):
    prefetchnta -0x1c0(%rsi)
L(shl_1_bwd_loop_L1):
    movaps    -0x11(%rsi), %xmm2
    sub    $0x40, %rdx
    movaps    -0x21(%rsi), %xmm3
    movaps    -0x31(%rsi), %xmm4
    movaps    -0x41(%rsi), %xmm5
    lea    -0x40(%rsi), %rsi
    palignr    $1, %xmm2, %xmm1
    palignr    $1, %xmm3, %xmm2
    palignr    $1, %xmm4, %xmm3
    palignr    $1, %xmm5, %xmm4

    movaps    %xmm1, -0x10(%rdi)
    movaps    %xmm5, %xmm1

    movaps    %xmm2, -0x20(%rdi)
    lea    -0x40(%rdi), %rdi

    movaps    %xmm3, 0x10(%rdi)
    jb    L(shl_1_bwd_end)
    movaps    %xmm4, (%rdi)
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_1_bwd_end):
    movaps    %xmm4, (%rdi)
    lea    64(%rdx), %rdx
    movdqu    %xmm0, (%r8)
    BRANCH_TO_JMPTBL_ENTRY(L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_2):
    lea    (L(shl_2_loop_L1)-L(shl_2))(%r9), %r9
    cmp    %rcx, %rdx
    movaps    -0x02(%rsi), %xmm1
    jb    L(L2_fwd)
    lea    (L(shl_2_loop_L2)-L(shl_2_loop_L1))(%r9), %r9
L(L2_fwd):
    lea    -64(%rdx), %rdx
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_2_loop_L2):
    prefetchnta 0x1c0(%rsi)
L(shl_2_loop_L1):
    sub    $64, %rdx
    movaps    0x0e(%rsi), %xmm2
    movaps    0x1e(%rsi), %xmm3
    movaps    0x2e(%rsi), %xmm4
    movaps    0x3e(%rsi), %xmm5
    movdqa    %xmm5, %xmm6
    palignr    $2, %xmm4, %xmm5
    lea    64(%rsi), %rsi
    palignr    $2, %xmm3, %xmm4
    palignr    $2, %xmm2, %xmm3
    lea    64(%rdi), %rdi
    palignr    $2, %xmm1, %xmm2
    movdqa    %xmm6, %xmm1
    movdqa    %xmm2, -0x40(%rdi)
    movaps    %xmm3, -0x30(%rdi)
    jb    L(shl_2_end)
    movaps    %xmm4, -0x20(%rdi)
    movaps    %xmm5, -0x10(%rdi)
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_2_end):
    movaps    %xmm4, -0x20(%rdi)
    lea    64(%rdx), %rdx
    movaps    %xmm5, -0x10(%rdi)
    add    %rdx, %rdi
    movdqu    %xmm0, (%r8)
    add    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY(L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_2_bwd):
    lea    (L(shl_2_bwd_loop_L1)-L(shl_2_bwd))(%r9), %r9
    cmp    %rcx, %rdx
    movaps    -0x02(%rsi), %xmm1
    jb    L(L2_bwd)
    lea    (L(shl_2_bwd_loop_L2)-L(shl_2_bwd_loop_L1))(%r9), %r9
L(L2_bwd):
    lea    -64(%rdx), %rdx
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_2_bwd_loop_L2):
    prefetchnta -0x1c0(%rsi)
L(shl_2_bwd_loop_L1):
    movaps    -0x12(%rsi), %xmm2
    sub    $0x40, %rdx
    movaps    -0x22(%rsi), %xmm3
    movaps    -0x32(%rsi), %xmm4
    movaps    -0x42(%rsi), %xmm5
    lea    -0x40(%rsi), %rsi
    palignr    $2, %xmm2, %xmm1
    palignr    $2, %xmm3, %xmm2
    palignr    $2, %xmm4, %xmm3
    palignr    $2, %xmm5, %xmm4

    movaps    %xmm1, -0x10(%rdi)
    movaps    %xmm5, %xmm1

    movaps    %xmm2, -0x20(%rdi)
    lea    -0x40(%rdi), %rdi

    movaps    %xmm3, 0x10(%rdi)
    jb    L(shl_2_bwd_end)
    movaps    %xmm4, (%rdi)
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_2_bwd_end):
    movaps    %xmm4, (%rdi)
    lea    64(%rdx), %rdx
    movdqu    %xmm0, (%r8)
    BRANCH_TO_JMPTBL_ENTRY(L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_3):
    lea    (L(shl_3_loop_L1)-L(shl_3))(%r9), %r9
    cmp    %rcx, %rdx
    movaps    -0x03(%rsi), %xmm1
    jb    L(L3_fwd)
    lea    (L(shl_3_loop_L2)-L(shl_3_loop_L1))(%r9), %r9
L(L3_fwd):
    lea    -64(%rdx), %rdx
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_3_loop_L2):
    prefetchnta 0x1c0(%rsi)
L(shl_3_loop_L1):
    sub    $64, %rdx
    movaps    0x0d(%rsi), %xmm2
    movaps    0x1d(%rsi), %xmm3
    movaps    0x2d(%rsi), %xmm4
    movaps    0x3d(%rsi), %xmm5
    movdqa    %xmm5, %xmm6
    palignr    $3, %xmm4, %xmm5
    lea    64(%rsi), %rsi
    palignr    $3, %xmm3, %xmm4
    palignr    $3, %xmm2, %xmm3
    lea    64(%rdi), %rdi
    palignr    $3, %xmm1, %xmm2
    movdqa    %xmm6, %xmm1
    movdqa    %xmm2, -0x40(%rdi)
    movaps    %xmm3, -0x30(%rdi)
    jb    L(shl_3_end)
    movaps    %xmm4, -0x20(%rdi)
    movaps    %xmm5, -0x10(%rdi)
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_3_end):
    movaps    %xmm4, -0x20(%rdi)
    lea    64(%rdx), %rdx
    movaps    %xmm5, -0x10(%rdi)
    add    %rdx, %rdi
    movdqu    %xmm0, (%r8)
    add    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY(L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_3_bwd):
    lea    (L(shl_3_bwd_loop_L1)-L(shl_3_bwd))(%r9), %r9
    cmp    %rcx, %rdx
    movaps    -0x03(%rsi), %xmm1
    jb    L(L3_bwd)
    lea    (L(shl_3_bwd_loop_L2)-L(shl_3_bwd_loop_L1))(%r9), %r9
L(L3_bwd):
    lea    -64(%rdx), %rdx
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_3_bwd_loop_L2):
    prefetchnta -0x1c0(%rsi)
L(shl_3_bwd_loop_L1):
    movaps    -0x13(%rsi), %xmm2
    sub    $0x40, %rdx
    movaps    -0x23(%rsi), %xmm3
    movaps    -0x33(%rsi), %xmm4
    movaps    -0x43(%rsi), %xmm5
    lea    -0x40(%rsi), %rsi
    palignr    $3, %xmm2, %xmm1
    palignr    $3, %xmm3, %xmm2
    palignr    $3, %xmm4, %xmm3
    palignr    $3, %xmm5, %xmm4

    movaps    %xmm1, -0x10(%rdi)
    movaps    %xmm5, %xmm1

    movaps    %xmm2, -0x20(%rdi)
    lea    -0x40(%rdi), %rdi

    movaps    %xmm3, 0x10(%rdi)
    jb    L(shl_3_bwd_end)
    movaps    %xmm4, (%rdi)
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_3_bwd_end):
    movaps    %xmm4, (%rdi)
    lea    64(%rdx), %rdx
    movdqu    %xmm0, (%r8)
    BRANCH_TO_JMPTBL_ENTRY(L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_4):
    lea    (L(shl_4_loop_L1)-L(shl_4))(%r9), %r9
    cmp    %rcx, %rdx
    movaps    -0x04(%rsi), %xmm1
    jb    L(L4_fwd)
    lea    (L(shl_4_loop_L2)-L(shl_4_loop_L1))(%r9), %r9
L(L4_fwd):
    lea    -64(%rdx), %rdx
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_4_loop_L2):
    prefetchnta 0x1c0(%rsi)
L(shl_4_loop_L1):
    sub    $64, %rdx
    movaps    0x0c(%rsi), %xmm2
    movaps    0x1c(%rsi), %xmm3
    movaps    0x2c(%rsi), %xmm4
    movaps    0x3c(%rsi), %xmm5
    movdqa    %xmm5, %xmm6
    palignr    $4, %xmm4, %xmm5
    lea    64(%rsi), %rsi
    palignr    $4, %xmm3, %xmm4
    palignr    $4, %xmm2, %xmm3
    lea    64(%rdi), %rdi
    palignr    $4, %xmm1, %xmm2
    movdqa    %xmm6, %xmm1
    movdqa    %xmm2, -0x40(%rdi)
    movaps    %xmm3, -0x30(%rdi)
    jb    L(shl_4_end)
    movaps    %xmm4, -0x20(%rdi)
    movaps    %xmm5, -0x10(%rdi)
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_4_end):
    movaps    %xmm4, -0x20(%rdi)
    lea    64(%rdx), %rdx
    movaps    %xmm5, -0x10(%rdi)
    add    %rdx, %rdi
    movdqu    %xmm0, (%r8)
    add    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY(L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_4_bwd):
    lea    (L(shl_4_bwd_loop_L1)-L(shl_4_bwd))(%r9), %r9
    cmp    %rcx, %rdx
    movaps    -0x04(%rsi), %xmm1
    jb    L(L4_bwd)
    lea    (L(shl_4_bwd_loop_L2)-L(shl_4_bwd_loop_L1))(%r9), %r9
L(L4_bwd):
    lea    -64(%rdx), %rdx
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_4_bwd_loop_L2):
    prefetchnta -0x1c0(%rsi)
L(shl_4_bwd_loop_L1):
    movaps    -0x14(%rsi), %xmm2
    sub    $0x40, %rdx
    movaps    -0x24(%rsi), %xmm3
    movaps    -0x34(%rsi), %xmm4
    movaps    -0x44(%rsi), %xmm5
    lea    -0x40(%rsi), %rsi
    palignr    $4, %xmm2, %xmm1
    palignr    $4, %xmm3, %xmm2
    palignr    $4, %xmm4, %xmm3
    palignr    $4, %xmm5, %xmm4

    movaps    %xmm1, -0x10(%rdi)
    movaps    %xmm5, %xmm1

    movaps    %xmm2, -0x20(%rdi)
    lea    -0x40(%rdi), %rdi

    movaps    %xmm3, 0x10(%rdi)
    jb    L(shl_4_bwd_end)
    movaps    %xmm4, (%rdi)
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_4_bwd_end):
    movaps    %xmm4, (%rdi)
    lea    64(%rdx), %rdx
    movdqu    %xmm0, (%r8)
    BRANCH_TO_JMPTBL_ENTRY(L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_5):
    lea    (L(shl_5_loop_L1)-L(shl_5))(%r9), %r9
    cmp    %rcx, %rdx
    movaps    -0x05(%rsi), %xmm1
    jb    L(L5_fwd)
    lea    (L(shl_5_loop_L2)-L(shl_5_loop_L1))(%r9), %r9
L(L5_fwd):
    lea    -64(%rdx), %rdx
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_5_loop_L2):
    prefetchnta 0x1c0(%rsi)
L(shl_5_loop_L1):
    sub    $64, %rdx
    movaps    0x0b(%rsi), %xmm2
    movaps    0x1b(%rsi), %xmm3
    movaps    0x2b(%rsi), %xmm4
    movaps    0x3b(%rsi), %xmm5
    movdqa    %xmm5, %xmm6
    palignr    $5, %xmm4, %xmm5
    lea    64(%rsi), %rsi
    palignr    $5, %xmm3, %xmm4
    palignr    $5, %xmm2, %xmm3
    lea    64(%rdi), %rdi
    palignr    $5, %xmm1, %xmm2
    movdqa    %xmm6, %xmm1
    movdqa    %xmm2, -0x40(%rdi)
    movaps    %xmm3, -0x30(%rdi)
    jb    L(shl_5_end)
    movaps    %xmm4, -0x20(%rdi)
    movaps    %xmm5, -0x10(%rdi)
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_5_end):
    movaps    %xmm4, -0x20(%rdi)
    lea    64(%rdx), %rdx
    movaps    %xmm5, -0x10(%rdi)
    add    %rdx, %rdi
    movdqu    %xmm0, (%r8)
    add    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY(L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_5_bwd):
    lea    (L(shl_5_bwd_loop_L1)-L(shl_5_bwd))(%r9), %r9
    cmp    %rcx, %rdx
    movaps    -0x05(%rsi), %xmm1
    jb    L(L5_bwd)
    lea    (L(shl_5_bwd_loop_L2)-L(shl_5_bwd_loop_L1))(%r9), %r9
L(L5_bwd):
    lea    -64(%rdx), %rdx
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_5_bwd_loop_L2):
    prefetchnta -0x1c0(%rsi)
L(shl_5_bwd_loop_L1):
    movaps    -0x15(%rsi), %xmm2
    sub    $0x40, %rdx
    movaps    -0x25(%rsi), %xmm3
    movaps    -0x35(%rsi), %xmm4
    movaps    -0x45(%rsi), %xmm5
    lea    -0x40(%rsi), %rsi
    palignr    $5, %xmm2, %xmm1
    palignr    $5, %xmm3, %xmm2
    palignr    $5, %xmm4, %xmm3
    palignr    $5, %xmm5, %xmm4

    movaps    %xmm1, -0x10(%rdi)
    movaps    %xmm5, %xmm1

    movaps    %xmm2, -0x20(%rdi)
    lea    -0x40(%rdi), %rdi

    movaps    %xmm3, 0x10(%rdi)
    jb    L(shl_5_bwd_end)
    movaps    %xmm4, (%rdi)
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_5_bwd_end):
    movaps    %xmm4, (%rdi)
    lea    64(%rdx), %rdx
    movdqu    %xmm0, (%r8)
    BRANCH_TO_JMPTBL_ENTRY(L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_6):
    lea    (L(shl_6_loop_L1)-L(shl_6))(%r9), %r9
    cmp    %rcx, %rdx
    movaps    -0x06(%rsi), %xmm1
    jb    L(L6_fwd)
    lea    (L(shl_6_loop_L2)-L(shl_6_loop_L1))(%r9), %r9
L(L6_fwd):
    lea    -64(%rdx), %rdx
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_6_loop_L2):
    prefetchnta 0x1c0(%rsi)
L(shl_6_loop_L1):
    sub    $64, %rdx
    movaps    0x0a(%rsi), %xmm2
    movaps    0x1a(%rsi), %xmm3
    movaps    0x2a(%rsi), %xmm4
    movaps    0x3a(%rsi), %xmm5
    movdqa    %xmm5, %xmm6
    palignr    $6, %xmm4, %xmm5
    lea    64(%rsi), %rsi
    palignr    $6, %xmm3, %xmm4
    palignr    $6, %xmm2, %xmm3
    lea    64(%rdi), %rdi
    palignr    $6, %xmm1, %xmm2
    movdqa    %xmm6, %xmm1
    movdqa    %xmm2, -0x40(%rdi)
    movaps    %xmm3, -0x30(%rdi)
    jb    L(shl_6_end)
    movaps    %xmm4, -0x20(%rdi)
    movaps    %xmm5, -0x10(%rdi)
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_6_end):
    movaps    %xmm4, -0x20(%rdi)
    lea    64(%rdx), %rdx
    movaps    %xmm5, -0x10(%rdi)
    add    %rdx, %rdi
    movdqu    %xmm0, (%r8)
    add    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY(L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_6_bwd):
    lea    (L(shl_6_bwd_loop_L1)-L(shl_6_bwd))(%r9), %r9
    cmp    %rcx, %rdx
    movaps    -0x06(%rsi), %xmm1
    jb    L(L6_bwd)
    lea    (L(shl_6_bwd_loop_L2)-L(shl_6_bwd_loop_L1))(%r9), %r9
L(L6_bwd):
    lea    -64(%rdx), %rdx
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_6_bwd_loop_L2):
    prefetchnta -0x1c0(%rsi)
L(shl_6_bwd_loop_L1):
    movaps    -0x16(%rsi), %xmm2
    sub    $0x40, %rdx
    movaps    -0x26(%rsi), %xmm3
    movaps    -0x36(%rsi), %xmm4
    movaps    -0x46(%rsi), %xmm5
    lea    -0x40(%rsi), %rsi
    palignr    $6, %xmm2, %xmm1
    palignr    $6, %xmm3, %xmm2
    palignr    $6, %xmm4, %xmm3
    palignr    $6, %xmm5, %xmm4

    movaps    %xmm1, -0x10(%rdi)
    movaps    %xmm5, %xmm1

    movaps    %xmm2, -0x20(%rdi)
    lea    -0x40(%rdi), %rdi

    movaps    %xmm3, 0x10(%rdi)
    jb    L(shl_6_bwd_end)
    movaps    %xmm4, (%rdi)
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_6_bwd_end):
    movaps    %xmm4, (%rdi)
    lea    64(%rdx), %rdx
    movdqu    %xmm0, (%r8)
    BRANCH_TO_JMPTBL_ENTRY(L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_7):
    lea    (L(shl_7_loop_L1)-L(shl_7))(%r9), %r9
    cmp    %rcx, %rdx
    movaps    -0x07(%rsi), %xmm1
    jb    L(L7_fwd)
    lea    (L(shl_7_loop_L2)-L(shl_7_loop_L1))(%r9), %r9
L(L7_fwd):
    lea    -64(%rdx), %rdx
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_7_loop_L2):
    prefetchnta 0x1c0(%rsi)
L(shl_7_loop_L1):
    sub    $64, %rdx
    movaps    0x09(%rsi), %xmm2
    movaps    0x19(%rsi), %xmm3
    movaps    0x29(%rsi), %xmm4
    movaps    0x39(%rsi), %xmm5
    movdqa    %xmm5, %xmm6
    palignr    $7, %xmm4, %xmm5
    lea    64(%rsi), %rsi
    palignr    $7, %xmm3, %xmm4
    palignr    $7, %xmm2, %xmm3
    lea    64(%rdi), %rdi
    palignr    $7, %xmm1, %xmm2
    movdqa    %xmm6, %xmm1
    movdqa    %xmm2, -0x40(%rdi)
    movaps    %xmm3, -0x30(%rdi)
    jb    L(shl_7_end)
    movaps    %xmm4, -0x20(%rdi)
    movaps    %xmm5, -0x10(%rdi)
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_7_end):
    movaps    %xmm4, -0x20(%rdi)
    lea    64(%rdx), %rdx
    movaps    %xmm5, -0x10(%rdi)
    add    %rdx, %rdi
    movdqu    %xmm0, (%r8)
    add    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY(L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_7_bwd):
    lea    (L(shl_7_bwd_loop_L1)-L(shl_7_bwd))(%r9), %r9
    cmp    %rcx, %rdx
    movaps    -0x07(%rsi), %xmm1
    jb    L(L7_bwd)
    lea    (L(shl_7_bwd_loop_L2)-L(shl_7_bwd_loop_L1))(%r9), %r9
L(L7_bwd):
    lea    -64(%rdx), %rdx
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_7_bwd_loop_L2):
    prefetchnta -0x1c0(%rsi)
L(shl_7_bwd_loop_L1):
    movaps    -0x17(%rsi), %xmm2
    sub    $0x40, %rdx
    movaps    -0x27(%rsi), %xmm3
    movaps    -0x37(%rsi), %xmm4
    movaps    -0x47(%rsi), %xmm5
    lea    -0x40(%rsi), %rsi
    palignr    $7, %xmm2, %xmm1
    palignr    $7, %xmm3, %xmm2
    palignr    $7, %xmm4, %xmm3
    palignr    $7, %xmm5, %xmm4

    movaps    %xmm1, -0x10(%rdi)
    movaps    %xmm5, %xmm1

    movaps    %xmm2, -0x20(%rdi)
    lea    -0x40(%rdi), %rdi

    movaps    %xmm3, 0x10(%rdi)
    jb    L(shl_7_bwd_end)
    movaps    %xmm4, (%rdi)
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_7_bwd_end):
    movaps    %xmm4, (%rdi)
    lea    64(%rdx), %rdx
    movdqu    %xmm0, (%r8)
    BRANCH_TO_JMPTBL_ENTRY(L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_8):
    lea    (L(shl_8_loop_L1)-L(shl_8))(%r9), %r9
    cmp    %rcx, %rdx
    movaps    -0x08(%rsi), %xmm1
    jb    L(L8_fwd)
    lea    (L(shl_8_loop_L2)-L(shl_8_loop_L1))(%r9), %r9
L(L8_fwd):
    lea    -64(%rdx), %rdx
    _CET_NOTRACK jmp *%r9
L(shl_8_loop_L2):
    prefetchnta 0x1c0(%rsi)
L(shl_8_loop_L1):
    sub    $64, %rdx
    movaps    0x08(%rsi), %xmm2
    movaps    0x18(%rsi), %xmm3
    movaps    0x28(%rsi), %xmm4
    movaps    0x38(%rsi), %xmm5
    movdqa    %xmm5, %xmm6
    palignr    $8, %xmm4, %xmm5
    lea    64(%rsi), %rsi
    palignr    $8, %xmm3, %xmm4
    palignr    $8, %xmm2, %xmm3
    lea    64(%rdi), %rdi
    palignr    $8, %xmm1, %xmm2
    movdqa    %xmm6, %xmm1
    movdqa    %xmm2, -0x40(%rdi)
    movaps    %xmm3, -0x30(%rdi)
    jb    L(shl_8_end)
    movaps    %xmm4, -0x20(%rdi)
    movaps    %xmm5, -0x10(%rdi)
    _CET_NOTRACK jmp *%r9
    ud2
    .p2align 4
L(shl_8_end):
    lea    64(%rdx), %rdx
    movaps    %xmm4, -0x20(%rdi)
    add    %rdx, %rsi
    movaps    %xmm5, -0x10(%rdi)
    add    %rdx, %rdi
    movdqu    %xmm0, (%r8)
    BRANCH_TO_JMPTBL_ENTRY(L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_8_bwd):
    lea    (L(shl_8_bwd_loop_L1)-L(shl_8_bwd))(%r9), %r9
    cmp    %rcx, %rdx
    movaps    -0x08(%rsi), %xmm1
    jb    L(L8_bwd)
    lea    (L(shl_8_bwd_loop_L2)-L(shl_8_bwd_loop_L1))(%r9), %r9
L(L8_bwd):
    lea    -64(%rdx), %rdx
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_8_bwd_loop_L2):
    prefetchnta -0x1c0(%rsi)
L(shl_8_bwd_loop_L1):
    movaps    -0x18(%rsi), %xmm2
    sub    $0x40, %rdx
    movaps    -0x28(%rsi), %xmm3
    movaps    -0x38(%rsi), %xmm4
    movaps    -0x48(%rsi), %xmm5
    lea    -0x40(%rsi), %rsi
    palignr    $8, %xmm2, %xmm1
    palignr    $8, %xmm3, %xmm2
    palignr    $8, %xmm4, %xmm3
    palignr    $8, %xmm5, %xmm4

    movaps    %xmm1, -0x10(%rdi)
    movaps    %xmm5, %xmm1

    movaps    %xmm2, -0x20(%rdi)
    lea    -0x40(%rdi), %rdi

    movaps    %xmm3, 0x10(%rdi)
    jb    L(shl_8_bwd_end)
    movaps    %xmm4, (%rdi)
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_8_bwd_end):
    movaps    %xmm4, (%rdi)
    lea    64(%rdx), %rdx
    movdqu    %xmm0, (%r8)
    BRANCH_TO_JMPTBL_ENTRY(L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_9):
    lea    (L(shl_9_loop_L1)-L(shl_9))(%r9), %r9
    cmp    %rcx, %rdx
    movaps    -0x09(%rsi), %xmm1
    jb    L(L9_fwd)
    lea    (L(shl_9_loop_L2)-L(shl_9_loop_L1))(%r9), %r9
L(L9_fwd):
    lea    -64(%rdx), %rdx
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_9_loop_L2):
    prefetchnta 0x1c0(%rsi)
L(shl_9_loop_L1):
    sub    $64, %rdx
    movaps    0x07(%rsi), %xmm2
    movaps    0x17(%rsi), %xmm3
    movaps    0x27(%rsi), %xmm4
    movaps    0x37(%rsi), %xmm5
    movdqa    %xmm5, %xmm6
    palignr    $9, %xmm4, %xmm5
    lea    64(%rsi), %rsi
    palignr    $9, %xmm3, %xmm4
    palignr    $9, %xmm2, %xmm3
    lea    64(%rdi), %rdi
    palignr    $9, %xmm1, %xmm2
    movdqa    %xmm6, %xmm1
    movdqa    %xmm2, -0x40(%rdi)
    movaps    %xmm3, -0x30(%rdi)
    jb    L(shl_9_end)
    movaps    %xmm4, -0x20(%rdi)
    movaps    %xmm5, -0x10(%rdi)
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_9_end):
    movaps    %xmm4, -0x20(%rdi)
    lea    64(%rdx), %rdx
    movaps    %xmm5, -0x10(%rdi)
    add    %rdx, %rdi
    movdqu    %xmm0, (%r8)
    add    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY(L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_9_bwd):
    lea    (L(shl_9_bwd_loop_L1)-L(shl_9_bwd))(%r9), %r9
    cmp    %rcx, %rdx
    movaps    -0x09(%rsi), %xmm1
    jb    L(L9_bwd)
    lea    (L(shl_9_bwd_loop_L2)-L(shl_9_bwd_loop_L1))(%r9), %r9
L(L9_bwd):
    lea    -64(%rdx), %rdx
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_9_bwd_loop_L2):
    prefetchnta -0x1c0(%rsi)
L(shl_9_bwd_loop_L1):
    movaps    -0x19(%rsi), %xmm2
    sub    $0x40, %rdx
    movaps    -0x29(%rsi), %xmm3
    movaps    -0x39(%rsi), %xmm4
    movaps    -0x49(%rsi), %xmm5
    lea    -0x40(%rsi), %rsi
    palignr    $9, %xmm2, %xmm1
    palignr    $9, %xmm3, %xmm2
    palignr    $9, %xmm4, %xmm3
    palignr    $9, %xmm5, %xmm4

    movaps    %xmm1, -0x10(%rdi)
    movaps    %xmm5, %xmm1

    movaps    %xmm2, -0x20(%rdi)
    lea    -0x40(%rdi), %rdi

    movaps    %xmm3, 0x10(%rdi)
    jb    L(shl_9_bwd_end)
    movaps    %xmm4, (%rdi)
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_9_bwd_end):
    movaps    %xmm4, (%rdi)
    lea    64(%rdx), %rdx
    movdqu    %xmm0, (%r8)
    BRANCH_TO_JMPTBL_ENTRY(L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_10):
    lea    (L(shl_10_loop_L1)-L(shl_10))(%r9), %r9
    cmp    %rcx, %rdx
    movaps    -0x0a(%rsi), %xmm1
    jb    L(L10_fwd)
    lea    (L(shl_10_loop_L2)-L(shl_10_loop_L1))(%r9), %r9
L(L10_fwd):
    lea    -64(%rdx), %rdx
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_10_loop_L2):
    prefetchnta 0x1c0(%rsi)
L(shl_10_loop_L1):
    sub    $64, %rdx
    movaps    0x06(%rsi), %xmm2
    movaps    0x16(%rsi), %xmm3
    movaps    0x26(%rsi), %xmm4
    movaps    0x36(%rsi), %xmm5
    movdqa    %xmm5, %xmm6
    palignr    $10, %xmm4, %xmm5
    lea    64(%rsi), %rsi
    palignr    $10, %xmm3, %xmm4
    palignr    $10, %xmm2, %xmm3
    lea    64(%rdi), %rdi
    palignr    $10, %xmm1, %xmm2
    movdqa    %xmm6, %xmm1
    movdqa    %xmm2, -0x40(%rdi)
    movaps    %xmm3, -0x30(%rdi)
    jb    L(shl_10_end)
    movaps    %xmm4, -0x20(%rdi)
    movaps    %xmm5, -0x10(%rdi)
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_10_end):
    movaps    %xmm4, -0x20(%rdi)
    lea    64(%rdx), %rdx
    movaps    %xmm5, -0x10(%rdi)
    add    %rdx, %rdi
    movdqu    %xmm0, (%r8)
    add    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY(L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_10_bwd):
    lea    (L(shl_10_bwd_loop_L1)-L(shl_10_bwd))(%r9), %r9
    cmp    %rcx, %rdx
    movaps    -0x0a(%rsi), %xmm1
    jb    L(L10_bwd)
    lea    (L(shl_10_bwd_loop_L2)-L(shl_10_bwd_loop_L1))(%r9), %r9
L(L10_bwd):
    lea    -64(%rdx), %rdx
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_10_bwd_loop_L2):
    prefetchnta -0x1c0(%rsi)
L(shl_10_bwd_loop_L1):
    movaps    -0x1a(%rsi), %xmm2
    sub    $0x40, %rdx
    movaps    -0x2a(%rsi), %xmm3
    movaps    -0x3a(%rsi), %xmm4
    movaps    -0x4a(%rsi), %xmm5
    lea    -0x40(%rsi), %rsi
    palignr    $10, %xmm2, %xmm1
    palignr    $10, %xmm3, %xmm2
    palignr    $10, %xmm4, %xmm3
    palignr    $10, %xmm5, %xmm4

    movaps    %xmm1, -0x10(%rdi)
    movaps    %xmm5, %xmm1

    movaps    %xmm2, -0x20(%rdi)
    lea    -0x40(%rdi), %rdi

    movaps    %xmm3, 0x10(%rdi)
    jb    L(shl_10_bwd_end)
    movaps    %xmm4, (%rdi)
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_10_bwd_end):
    movaps    %xmm4, (%rdi)
    lea    64(%rdx), %rdx
    movdqu    %xmm0, (%r8)
    BRANCH_TO_JMPTBL_ENTRY(L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_11):
    lea    (L(shl_11_loop_L1)-L(shl_11))(%r9), %r9
    cmp    %rcx, %rdx
    movaps    -0x0b(%rsi), %xmm1
    jb    L(L11_fwd)
    lea    (L(shl_11_loop_L2)-L(shl_11_loop_L1))(%r9), %r9
L(L11_fwd):
    lea    -64(%rdx), %rdx
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_11_loop_L2):
    prefetchnta 0x1c0(%rsi)
L(shl_11_loop_L1):
    sub    $64, %rdx
    movaps    0x05(%rsi), %xmm2
    movaps    0x15(%rsi), %xmm3
    movaps    0x25(%rsi), %xmm4
    movaps    0x35(%rsi), %xmm5
    movdqa    %xmm5, %xmm6
    palignr    $11, %xmm4, %xmm5
    lea    64(%rsi), %rsi
    palignr    $11, %xmm3, %xmm4
    palignr    $11, %xmm2, %xmm3
    lea    64(%rdi), %rdi
    palignr    $11, %xmm1, %xmm2
    movdqa    %xmm6, %xmm1
    movdqa    %xmm2, -0x40(%rdi)
    movaps    %xmm3, -0x30(%rdi)
    jb    L(shl_11_end)
    movaps    %xmm4, -0x20(%rdi)
    movaps    %xmm5, -0x10(%rdi)
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_11_end):
    movaps    %xmm4, -0x20(%rdi)
    lea    64(%rdx), %rdx
    movaps    %xmm5, -0x10(%rdi)
    add    %rdx, %rdi
    movdqu    %xmm0, (%r8)
    add    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY(L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_11_bwd):
    lea    (L(shl_11_bwd_loop_L1)-L(shl_11_bwd))(%r9), %r9
    cmp    %rcx, %rdx
    movaps    -0x0b(%rsi), %xmm1
    jb    L(L11_bwd)
    lea    (L(shl_11_bwd_loop_L2)-L(shl_11_bwd_loop_L1))(%r9), %r9
L(L11_bwd):
    lea    -64(%rdx), %rdx
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_11_bwd_loop_L2):
    prefetchnta -0x1c0(%rsi)
L(shl_11_bwd_loop_L1):
    movaps    -0x1b(%rsi), %xmm2
    sub    $0x40, %rdx
    movaps    -0x2b(%rsi), %xmm3
    movaps    -0x3b(%rsi), %xmm4
    movaps    -0x4b(%rsi), %xmm5
    lea    -0x40(%rsi), %rsi
    palignr    $11, %xmm2, %xmm1
    palignr    $11, %xmm3, %xmm2
    palignr    $11, %xmm4, %xmm3
    palignr    $11, %xmm5, %xmm4

    movaps    %xmm1, -0x10(%rdi)
    movaps    %xmm5, %xmm1

    movaps    %xmm2, -0x20(%rdi)
    lea    -0x40(%rdi), %rdi

    movaps    %xmm3, 0x10(%rdi)
    jb    L(shl_11_bwd_end)
    movaps    %xmm4, (%rdi)
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_11_bwd_end):
    movaps    %xmm4, (%rdi)
    lea    64(%rdx), %rdx
    movdqu    %xmm0, (%r8)
    BRANCH_TO_JMPTBL_ENTRY(L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_12):
    lea    (L(shl_12_loop_L1)-L(shl_12))(%r9), %r9
    cmp    %rcx, %rdx
    movaps    -0x0c(%rsi), %xmm1
    jb    L(L12_fwd)
    lea    (L(shl_12_loop_L2)-L(shl_12_loop_L1))(%r9), %r9
L(L12_fwd):
    lea    -64(%rdx), %rdx
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_12_loop_L2):
    prefetchnta 0x1c0(%rsi)
L(shl_12_loop_L1):
    sub    $64, %rdx
    movaps    0x04(%rsi), %xmm2
    movaps    0x14(%rsi), %xmm3
    movaps    0x24(%rsi), %xmm4
    movaps    0x34(%rsi), %xmm5
    movdqa    %xmm5, %xmm6
    palignr    $12, %xmm4, %xmm5
    lea    64(%rsi), %rsi
    palignr    $12, %xmm3, %xmm4
    palignr    $12, %xmm2, %xmm3
    lea    64(%rdi), %rdi
    palignr    $12, %xmm1, %xmm2
    movdqa    %xmm6, %xmm1
    movdqa    %xmm2, -0x40(%rdi)
    movaps    %xmm3, -0x30(%rdi)
    jb    L(shl_12_end)
    movaps    %xmm4, -0x20(%rdi)
    movaps    %xmm5, -0x10(%rdi)
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_12_end):
    movaps    %xmm4, -0x20(%rdi)
    lea    64(%rdx), %rdx
    movaps    %xmm5, -0x10(%rdi)
    add    %rdx, %rdi
    movdqu    %xmm0, (%r8)
    add    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY(L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_12_bwd):
    lea    (L(shl_12_bwd_loop_L1)-L(shl_12_bwd))(%r9), %r9
    cmp    %rcx, %rdx
    movaps    -0x0c(%rsi), %xmm1
    jb    L(L12_bwd)
    lea    (L(shl_12_bwd_loop_L2)-L(shl_12_bwd_loop_L1))(%r9), %r9
L(L12_bwd):
    lea    -64(%rdx), %rdx
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_12_bwd_loop_L2):
    prefetchnta -0x1c0(%rsi)
L(shl_12_bwd_loop_L1):
    movaps    -0x1c(%rsi), %xmm2
    sub    $0x40, %rdx
    movaps    -0x2c(%rsi), %xmm3
    movaps    -0x3c(%rsi), %xmm4
    movaps    -0x4c(%rsi), %xmm5
    lea    -0x40(%rsi), %rsi
    palignr    $12, %xmm2, %xmm1
    palignr    $12, %xmm3, %xmm2
    palignr    $12, %xmm4, %xmm3
    palignr    $12, %xmm5, %xmm4

    movaps    %xmm1, -0x10(%rdi)
    movaps    %xmm5, %xmm1

    movaps    %xmm2, -0x20(%rdi)
    lea    -0x40(%rdi), %rdi

    movaps    %xmm3, 0x10(%rdi)
    jb    L(shl_12_bwd_end)
    movaps    %xmm4, (%rdi)
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_12_bwd_end):
    movaps    %xmm4, (%rdi)
    lea    64(%rdx), %rdx
    movdqu    %xmm0, (%r8)
    BRANCH_TO_JMPTBL_ENTRY(L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_13):
    lea    (L(shl_13_loop_L1)-L(shl_13))(%r9), %r9
    cmp    %rcx, %rdx
    movaps    -0x0d(%rsi), %xmm1
    jb    L(L13_fwd)
    lea    (L(shl_13_loop_L2)-L(shl_13_loop_L1))(%r9), %r9
L(L13_fwd):
    lea    -64(%rdx), %rdx
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_13_loop_L2):
    prefetchnta 0x1c0(%rsi)
L(shl_13_loop_L1):
    sub    $64, %rdx
    movaps    0x03(%rsi), %xmm2
    movaps    0x13(%rsi), %xmm3
    movaps    0x23(%rsi), %xmm4
    movaps    0x33(%rsi), %xmm5
    movdqa    %xmm5, %xmm6
    palignr    $13, %xmm4, %xmm5
    lea    64(%rsi), %rsi
    palignr    $13, %xmm3, %xmm4
    palignr    $13, %xmm2, %xmm3
    lea    64(%rdi), %rdi
    palignr    $13, %xmm1, %xmm2
    movdqa    %xmm6, %xmm1
    movdqa    %xmm2, -0x40(%rdi)
    movaps    %xmm3, -0x30(%rdi)
    jb    L(shl_13_end)
    movaps    %xmm4, -0x20(%rdi)
    movaps    %xmm5, -0x10(%rdi)
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_13_end):
    movaps    %xmm4, -0x20(%rdi)
    lea    64(%rdx), %rdx
    movaps    %xmm5, -0x10(%rdi)
    add    %rdx, %rdi
    movdqu    %xmm0, (%r8)
    add    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY(L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_13_bwd):
    lea    (L(shl_13_bwd_loop_L1)-L(shl_13_bwd))(%r9), %r9
    cmp    %rcx, %rdx
    movaps    -0x0d(%rsi), %xmm1
    jb    L(L13_bwd)
    lea    (L(shl_13_bwd_loop_L2)-L(shl_13_bwd_loop_L1))(%r9), %r9
L(L13_bwd):
    lea    -64(%rdx), %rdx
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_13_bwd_loop_L2):
    prefetchnta -0x1c0(%rsi)
L(shl_13_bwd_loop_L1):
    movaps    -0x1d(%rsi), %xmm2
    sub    $0x40, %rdx
    movaps    -0x2d(%rsi), %xmm3
    movaps    -0x3d(%rsi), %xmm4
    movaps    -0x4d(%rsi), %xmm5
    lea    -0x40(%rsi), %rsi
    palignr    $13, %xmm2, %xmm1
    palignr    $13, %xmm3, %xmm2
    palignr    $13, %xmm4, %xmm3
    palignr    $13, %xmm5, %xmm4

    movaps    %xmm1, -0x10(%rdi)
    movaps    %xmm5, %xmm1

    movaps    %xmm2, -0x20(%rdi)
    lea    -0x40(%rdi), %rdi

    movaps    %xmm3, 0x10(%rdi)
    jb    L(shl_13_bwd_end)
    movaps    %xmm4, (%rdi)
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_13_bwd_end):
    movaps    %xmm4, (%rdi)
    lea    64(%rdx), %rdx
    movdqu    %xmm0, (%r8)
    BRANCH_TO_JMPTBL_ENTRY(L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_14):
    lea    (L(shl_14_loop_L1)-L(shl_14))(%r9), %r9
    cmp    %rcx, %rdx
    movaps    -0x0e(%rsi), %xmm1
    jb    L(L14_fwd)
    lea    (L(shl_14_loop_L2)-L(shl_14_loop_L1))(%r9), %r9
L(L14_fwd):
    lea    -64(%rdx), %rdx
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_14_loop_L2):
    prefetchnta 0x1c0(%rsi)
L(shl_14_loop_L1):
    sub    $64, %rdx
    movaps    0x02(%rsi), %xmm2
    movaps    0x12(%rsi), %xmm3
    movaps    0x22(%rsi), %xmm4
    movaps    0x32(%rsi), %xmm5
    movdqa    %xmm5, %xmm6
    palignr    $14, %xmm4, %xmm5
    lea    64(%rsi), %rsi
    palignr    $14, %xmm3, %xmm4
    palignr    $14, %xmm2, %xmm3
    lea    64(%rdi), %rdi
    palignr    $14, %xmm1, %xmm2
    movdqa    %xmm6, %xmm1
    movdqa    %xmm2, -0x40(%rdi)
    movaps    %xmm3, -0x30(%rdi)
    jb    L(shl_14_end)
    movaps    %xmm4, -0x20(%rdi)
    movaps    %xmm5, -0x10(%rdi)
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_14_end):
    movaps    %xmm4, -0x20(%rdi)
    lea    64(%rdx), %rdx
    movaps    %xmm5, -0x10(%rdi)
    add    %rdx, %rdi
    movdqu    %xmm0, (%r8)
    add    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY(L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_14_bwd):
    lea    (L(shl_14_bwd_loop_L1)-L(shl_14_bwd))(%r9), %r9
    cmp    %rcx, %rdx
    movaps    -0x0e(%rsi), %xmm1
    jb    L(L14_bwd)
    lea    (L(shl_14_bwd_loop_L2)-L(shl_14_bwd_loop_L1))(%r9), %r9
L(L14_bwd):
    lea    -64(%rdx), %rdx
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_14_bwd_loop_L2):
    prefetchnta -0x1c0(%rsi)
L(shl_14_bwd_loop_L1):
    movaps    -0x1e(%rsi), %xmm2
    sub    $0x40, %rdx
    movaps    -0x2e(%rsi), %xmm3
    movaps    -0x3e(%rsi), %xmm4
    movaps    -0x4e(%rsi), %xmm5
    lea    -0x40(%rsi), %rsi
    palignr    $14, %xmm2, %xmm1
    palignr    $14, %xmm3, %xmm2
    palignr    $14, %xmm4, %xmm3
    palignr    $14, %xmm5, %xmm4

    movaps    %xmm1, -0x10(%rdi)
    movaps    %xmm5, %xmm1

    movaps    %xmm2, -0x20(%rdi)
    lea    -0x40(%rdi), %rdi

    movaps    %xmm3, 0x10(%rdi)
    jb    L(shl_14_bwd_end)
    movaps    %xmm4, (%rdi)
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_14_bwd_end):
    movaps    %xmm4, (%rdi)
    lea    64(%rdx), %rdx
    movdqu    %xmm0, (%r8)
    BRANCH_TO_JMPTBL_ENTRY(L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_15):
    lea    (L(shl_15_loop_L1)-L(shl_15))(%r9), %r9
    cmp    %rcx, %rdx
    movaps    -0x0f(%rsi), %xmm1
    jb    L(L15_fwd)
    lea    (L(shl_15_loop_L2)-L(shl_15_loop_L1))(%r9), %r9
L(L15_fwd):
    lea    -64(%rdx), %rdx
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_15_loop_L2):
    prefetchnta 0x1c0(%rsi)
L(shl_15_loop_L1):
    sub    $64, %rdx
    movaps    0x01(%rsi), %xmm2
    movaps    0x11(%rsi), %xmm3
    movaps    0x21(%rsi), %xmm4
    movaps    0x31(%rsi), %xmm5
    movdqa    %xmm5, %xmm6
    palignr    $15, %xmm4, %xmm5
    lea    64(%rsi), %rsi
    palignr    $15, %xmm3, %xmm4
    palignr    $15, %xmm2, %xmm3
    lea    64(%rdi), %rdi
    palignr    $15, %xmm1, %xmm2
    movdqa    %xmm6, %xmm1
    movdqa    %xmm2, -0x40(%rdi)
    movaps    %xmm3, -0x30(%rdi)
    jb    L(shl_15_end)
    movaps    %xmm4, -0x20(%rdi)
    movaps    %xmm5, -0x10(%rdi)
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_15_end):
    movaps    %xmm4, -0x20(%rdi)
    lea    64(%rdx), %rdx
    movaps    %xmm5, -0x10(%rdi)
    add    %rdx, %rdi
    movdqu    %xmm0, (%r8)
    add    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY(L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(shl_15_bwd):
    lea    (L(shl_15_bwd_loop_L1)-L(shl_15_bwd))(%r9), %r9
    cmp    %rcx, %rdx
    movaps    -0x0f(%rsi), %xmm1
    jb    L(L15_bwd)
    lea    (L(shl_15_bwd_loop_L2)-L(shl_15_bwd_loop_L1))(%r9), %r9
L(L15_bwd):
    lea    -64(%rdx), %rdx
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_15_bwd_loop_L2):
    prefetchnta -0x1c0(%rsi)
L(shl_15_bwd_loop_L1):
    movaps    -0x1f(%rsi), %xmm2
    sub    $0x40, %rdx
    movaps    -0x2f(%rsi), %xmm3
    movaps    -0x3f(%rsi), %xmm4
    movaps    -0x4f(%rsi), %xmm5
    lea    -0x40(%rsi), %rsi
    palignr    $15, %xmm2, %xmm1
    palignr    $15, %xmm3, %xmm2
    palignr    $15, %xmm4, %xmm3
    palignr    $15, %xmm5, %xmm4

    movaps    %xmm1, -0x10(%rdi)
    movaps    %xmm5, %xmm1

    movaps    %xmm2, -0x20(%rdi)
    lea    -0x40(%rdi), %rdi

    movaps    %xmm3, 0x10(%rdi)
    jb    L(shl_15_bwd_end)
    movaps    %xmm4, (%rdi)
    _CET_NOTRACK jmp *%r9
    ud2
L(shl_15_bwd_end):
    movaps    %xmm4, (%rdi)
    lea    64(%rdx), %rdx
    movdqu    %xmm0, (%r8)
    BRANCH_TO_JMPTBL_ENTRY(L(table_less_80bytes), %rdx, 4)

    .p2align 4
L(write_72bytes):
    movdqu    -72(%rsi), %xmm0
    movdqu    -56(%rsi), %xmm1
    mov    -40(%rsi), %r8
    mov    -32(%rsi), %r9
    mov    -24(%rsi), %r10
    mov    -16(%rsi), %r11
    mov    -8(%rsi), %rcx
    movdqu     %xmm0, -72(%rdi)
    movdqu     %xmm1, -56(%rdi)
    mov     %r8, -40(%rdi)
    mov     %r9, -32(%rdi)
    mov     %r10, -24(%rdi)
    mov     %r11, -16(%rdi)
    mov     %rcx, -8(%rdi)
    ret

    .p2align 4
L(write_64bytes):
    movdqu    -64(%rsi), %xmm0
    mov    -48(%rsi), %rcx
    mov    -40(%rsi), %r8
    mov    -32(%rsi), %r9
    mov    -24(%rsi), %r10
    mov    -16(%rsi), %r11
    mov    -8(%rsi), %rdx
    movdqu     %xmm0, -64(%rdi)
    mov     %rcx, -48(%rdi)
    mov     %r8, -40(%rdi)
    mov     %r9, -32(%rdi)
    mov     %r10, -24(%rdi)
    mov     %r11, -16(%rdi)
    mov     %rdx, -8(%rdi)
    ret

    .p2align 4
L(write_56bytes):
    movdqu    -56(%rsi), %xmm0
    mov    -40(%rsi), %r8
    mov    -32(%rsi), %r9
    mov    -24(%rsi), %r10
    mov    -16(%rsi), %r11
    mov    -8(%rsi), %rcx
    movdqu     %xmm0, -56(%rdi)
    mov     %r8, -40(%rdi)
    mov     %r9, -32(%rdi)
    mov     %r10, -24(%rdi)
    mov     %r11, -16(%rdi)
    mov     %rcx, -8(%rdi)
    ret

    .p2align 4
L(write_48bytes):
    mov    -48(%rsi), %rcx
    mov    -40(%rsi), %r8
    mov    -32(%rsi), %r9
    mov    -24(%rsi), %r10
    mov    -16(%rsi), %r11
    mov    -8(%rsi), %rdx
    mov     %rcx, -48(%rdi)
    mov     %r8, -40(%rdi)
    mov     %r9, -32(%rdi)
    mov     %r10, -24(%rdi)
    mov     %r11, -16(%rdi)
    mov     %rdx, -8(%rdi)
    ret

    .p2align 4
L(write_40bytes):
    mov    -40(%rsi), %r8
    mov    -32(%rsi), %r9
    mov    -24(%rsi), %r10
    mov    -16(%rsi), %r11
    mov    -8(%rsi), %rdx
    mov     %r8, -40(%rdi)
    mov     %r9, -32(%rdi)
    mov     %r10, -24(%rdi)
    mov     %r11, -16(%rdi)
    mov     %rdx, -8(%rdi)
    ret

    .p2align 4
L(write_32bytes):
    mov    -32(%rsi), %r9
    mov    -24(%rsi), %r10
    mov    -16(%rsi), %r11
    mov    -8(%rsi), %rdx
    mov     %r9, -32(%rdi)
    mov     %r10, -24(%rdi)
    mov     %r11, -16(%rdi)
    mov     %rdx, -8(%rdi)
    ret

    .p2align 4
L(write_24bytes):
    mov    -24(%rsi), %r10
    mov    -16(%rsi), %r11
    mov    -8(%rsi), %rdx
    mov     %r10, -24(%rdi)
    mov     %r11, -16(%rdi)
    mov     %rdx, -8(%rdi)
    ret

    .p2align 4
L(write_16bytes):
    mov    -16(%rsi), %r11
    mov    -8(%rsi), %rdx
    mov     %r11, -16(%rdi)
    mov     %rdx, -8(%rdi)
    ret

    .p2align 4
L(write_8bytes):
    mov    -8(%rsi), %rdx
    mov     %rdx, -8(%rdi)
L(write_0bytes):
    ret

    .p2align 4
L(write_73bytes):
    movdqu    -73(%rsi), %xmm0
    movdqu    -57(%rsi), %xmm1
    mov    -41(%rsi), %rcx
    mov    -33(%rsi), %r9
    mov    -25(%rsi), %r10
    mov    -17(%rsi), %r11
    mov    -9(%rsi), %r8
    mov    -4(%rsi), %edx
    movdqu     %xmm0, -73(%rdi)
    movdqu     %xmm1, -57(%rdi)
    mov     %rcx, -41(%rdi)
    mov     %r9, -33(%rdi)
    mov     %r10, -25(%rdi)
    mov     %r11, -17(%rdi)
    mov     %r8, -9(%rdi)
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_65bytes):
    movdqu    -65(%rsi), %xmm0
    movdqu    -49(%rsi), %xmm1
    mov    -33(%rsi), %r9
    mov    -25(%rsi), %r10
    mov    -17(%rsi), %r11
    mov    -9(%rsi), %rcx
    mov    -4(%rsi), %edx
    movdqu     %xmm0, -65(%rdi)
    movdqu     %xmm1, -49(%rdi)
    mov     %r9, -33(%rdi)
    mov     %r10, -25(%rdi)
    mov     %r11, -17(%rdi)
    mov     %rcx, -9(%rdi)
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_57bytes):
    movdqu    -57(%rsi), %xmm0
    mov    -41(%rsi), %r8
    mov    -33(%rsi), %r9
    mov    -25(%rsi), %r10
    mov    -17(%rsi), %r11
    mov    -9(%rsi), %rcx
    mov    -4(%rsi), %edx
    movdqu     %xmm0, -57(%rdi)
    mov     %r8, -41(%rdi)
    mov     %r9, -33(%rdi)
    mov     %r10, -25(%rdi)
    mov     %r11, -17(%rdi)
    mov     %rcx, -9(%rdi)
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_49bytes):
    movdqu    -49(%rsi), %xmm0
    mov    -33(%rsi), %r9
    mov    -25(%rsi), %r10
    mov    -17(%rsi), %r11
    mov    -9(%rsi), %rcx
    mov    -4(%rsi), %edx
    movdqu     %xmm0, -49(%rdi)
    mov     %r9, -33(%rdi)
    mov     %r10, -25(%rdi)
    mov     %r11, -17(%rdi)
    mov     %rcx, -9(%rdi)
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_41bytes):
    mov    -41(%rsi), %r8
    mov    -33(%rsi), %r9
    mov    -25(%rsi), %r10
    mov    -17(%rsi), %r11
    mov    -9(%rsi), %rcx
    mov    -1(%rsi), %dl
    mov     %r8, -41(%rdi)
    mov     %r9, -33(%rdi)
    mov     %r10, -25(%rdi)
    mov     %r11, -17(%rdi)
    mov     %rcx, -9(%rdi)
    mov     %dl, -1(%rdi)
    ret

    .p2align 4
L(write_33bytes):
    mov    -33(%rsi), %r9
    mov    -25(%rsi), %r10
    mov    -17(%rsi), %r11
    mov    -9(%rsi), %rcx
    mov    -1(%rsi), %dl
    mov     %r9, -33(%rdi)
    mov     %r10, -25(%rdi)
    mov     %r11, -17(%rdi)
    mov     %rcx, -9(%rdi)
    mov     %dl, -1(%rdi)
    ret

    .p2align 4
L(write_25bytes):
    mov    -25(%rsi), %r10
    mov    -17(%rsi), %r11
    mov    -9(%rsi), %rcx
    mov    -1(%rsi), %dl
    mov     %r10, -25(%rdi)
    mov     %r11, -17(%rdi)
    mov     %rcx, -9(%rdi)
    mov     %dl, -1(%rdi)
    ret

    .p2align 4
L(write_17bytes):
    mov    -17(%rsi), %r11
    mov    -9(%rsi), %rcx
    mov    -4(%rsi), %edx
    mov     %r11, -17(%rdi)
    mov     %rcx, -9(%rdi)
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_9bytes):
    mov    -9(%rsi), %rcx
    mov    -4(%rsi), %edx
    mov     %rcx, -9(%rdi)
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_1bytes):
    mov    -1(%rsi), %dl
    mov     %dl, -1(%rdi)
    ret

    .p2align 4
L(write_74bytes):
    movdqu    -74(%rsi), %xmm0
    movdqu    -58(%rsi), %xmm1
    mov    -42(%rsi), %r8
    mov    -34(%rsi), %r9
    mov    -26(%rsi), %r10
    mov    -18(%rsi), %r11
    mov    -10(%rsi), %rcx
    mov    -4(%rsi), %edx
    movdqu     %xmm0, -74(%rdi)
    movdqu     %xmm1, -58(%rdi)
    mov     %r8, -42(%rdi)
    mov     %r9, -34(%rdi)
    mov     %r10, -26(%rdi)
    mov     %r11, -18(%rdi)
    mov     %rcx, -10(%rdi)
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_66bytes):
    movdqu    -66(%rsi), %xmm0
    movdqu    -50(%rsi), %xmm1
    mov    -42(%rsi), %r8
    mov    -34(%rsi), %r9
    mov    -26(%rsi), %r10
    mov    -18(%rsi), %r11
    mov    -10(%rsi), %rcx
    mov    -4(%rsi), %edx
    movdqu     %xmm0, -66(%rdi)
    movdqu     %xmm1, -50(%rdi)
    mov     %r8, -42(%rdi)
    mov     %r9, -34(%rdi)
    mov     %r10, -26(%rdi)
    mov     %r11, -18(%rdi)
    mov     %rcx, -10(%rdi)
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_58bytes):
    movdqu    -58(%rsi), %xmm1
    mov    -42(%rsi), %r8
    mov    -34(%rsi), %r9
    mov    -26(%rsi), %r10
    mov    -18(%rsi), %r11
    mov    -10(%rsi), %rcx
    mov    -4(%rsi), %edx
    movdqu     %xmm1, -58(%rdi)
    mov     %r8, -42(%rdi)
    mov     %r9, -34(%rdi)
    mov     %r10, -26(%rdi)
    mov     %r11, -18(%rdi)
    mov     %rcx, -10(%rdi)
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_50bytes):
    movdqu    -50(%rsi), %xmm0
    mov    -34(%rsi), %r9
    mov    -26(%rsi), %r10
    mov    -18(%rsi), %r11
    mov    -10(%rsi), %rcx
    mov    -4(%rsi), %edx
    movdqu     %xmm0, -50(%rdi)
    mov     %r9, -34(%rdi)
    mov     %r10, -26(%rdi)
    mov     %r11, -18(%rdi)
    mov     %rcx, -10(%rdi)
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_42bytes):
    mov    -42(%rsi), %r8
    mov    -34(%rsi), %r9
    mov    -26(%rsi), %r10
    mov    -18(%rsi), %r11
    mov    -10(%rsi), %rcx
    mov    -4(%rsi), %edx
    mov     %r8, -42(%rdi)
    mov     %r9, -34(%rdi)
    mov     %r10, -26(%rdi)
    mov     %r11, -18(%rdi)
    mov     %rcx, -10(%rdi)
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_34bytes):
    mov    -34(%rsi), %r9
    mov    -26(%rsi), %r10
    mov    -18(%rsi), %r11
    mov    -10(%rsi), %rcx
    mov    -4(%rsi), %edx
    mov     %r9, -34(%rdi)
    mov     %r10, -26(%rdi)
    mov     %r11, -18(%rdi)
    mov     %rcx, -10(%rdi)
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_26bytes):
    mov    -26(%rsi), %r10
    mov    -18(%rsi), %r11
    mov    -10(%rsi), %rcx
    mov    -4(%rsi), %edx
    mov     %r10, -26(%rdi)
    mov     %r11, -18(%rdi)
    mov     %rcx, -10(%rdi)
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_18bytes):
    mov    -18(%rsi), %r11
    mov    -10(%rsi), %rcx
    mov    -4(%rsi), %edx
    mov     %r11, -18(%rdi)
    mov     %rcx, -10(%rdi)
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_10bytes):
    mov    -10(%rsi), %rcx
    mov    -4(%rsi), %edx
    mov     %rcx, -10(%rdi)
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_2bytes):
    mov    -2(%rsi), %dx
    mov     %dx, -2(%rdi)
    ret

    .p2align 4
L(write_75bytes):
    movdqu    -75(%rsi), %xmm0
    movdqu    -59(%rsi), %xmm1
    mov    -43(%rsi), %r8
    mov    -35(%rsi), %r9
    mov    -27(%rsi), %r10
    mov    -19(%rsi), %r11
    mov    -11(%rsi), %rcx
    mov    -4(%rsi), %edx
    movdqu     %xmm0, -75(%rdi)
    movdqu     %xmm1, -59(%rdi)
    mov     %r8, -43(%rdi)
    mov     %r9, -35(%rdi)
    mov     %r10, -27(%rdi)
    mov     %r11, -19(%rdi)
    mov     %rcx, -11(%rdi)
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_67bytes):
    movdqu    -67(%rsi), %xmm0
    movdqu    -59(%rsi), %xmm1
    mov    -43(%rsi), %r8
    mov    -35(%rsi), %r9
    mov    -27(%rsi), %r10
    mov    -19(%rsi), %r11
    mov    -11(%rsi), %rcx
    mov    -4(%rsi), %edx
    movdqu     %xmm0, -67(%rdi)
    movdqu     %xmm1, -59(%rdi)
    mov     %r8, -43(%rdi)
    mov     %r9, -35(%rdi)
    mov     %r10, -27(%rdi)
    mov     %r11, -19(%rdi)
    mov     %rcx, -11(%rdi)
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_59bytes):
    movdqu    -59(%rsi), %xmm0
    mov    -43(%rsi), %r8
    mov    -35(%rsi), %r9
    mov    -27(%rsi), %r10
    mov    -19(%rsi), %r11
    mov    -11(%rsi), %rcx
    mov    -4(%rsi), %edx
    movdqu     %xmm0, -59(%rdi)
    mov     %r8, -43(%rdi)
    mov     %r9, -35(%rdi)
    mov     %r10, -27(%rdi)
    mov     %r11, -19(%rdi)
    mov     %rcx, -11(%rdi)
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_51bytes):
    movdqu    -51(%rsi), %xmm0
    mov    -35(%rsi), %r9
    mov    -27(%rsi), %r10
    mov    -19(%rsi), %r11
    mov    -11(%rsi), %rcx
    mov    -4(%rsi), %edx
    movdqu     %xmm0, -51(%rdi)
    mov     %r9, -35(%rdi)
    mov     %r10, -27(%rdi)
    mov     %r11, -19(%rdi)
    mov     %rcx, -11(%rdi)
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_43bytes):
    mov    -43(%rsi), %r8
    mov    -35(%rsi), %r9
    mov    -27(%rsi), %r10
    mov    -19(%rsi), %r11
    mov    -11(%rsi), %rcx
    mov    -4(%rsi), %edx
    mov     %r8, -43(%rdi)
    mov     %r9, -35(%rdi)
    mov     %r10, -27(%rdi)
    mov     %r11, -19(%rdi)
    mov     %rcx, -11(%rdi)
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_35bytes):
    mov    -35(%rsi), %r9
    mov    -27(%rsi), %r10
    mov    -19(%rsi), %r11
    mov    -11(%rsi), %rcx
    mov    -4(%rsi), %edx
    mov     %r9, -35(%rdi)
    mov     %r10, -27(%rdi)
    mov     %r11, -19(%rdi)
    mov     %rcx, -11(%rdi)
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_27bytes):
    mov    -27(%rsi), %r10
    mov    -19(%rsi), %r11
    mov    -11(%rsi), %rcx
    mov    -4(%rsi), %edx
    mov     %r10, -27(%rdi)
    mov     %r11, -19(%rdi)
    mov     %rcx, -11(%rdi)
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_19bytes):
    mov    -19(%rsi), %r11
    mov    -11(%rsi), %rcx
    mov    -4(%rsi), %edx
    mov     %r11, -19(%rdi)
    mov     %rcx, -11(%rdi)
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_11bytes):
    mov    -11(%rsi), %rcx
    mov    -4(%rsi), %edx
    mov     %rcx, -11(%rdi)
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_3bytes):
    mov    -3(%rsi), %dx
    mov    -2(%rsi), %cx
    mov     %dx, -3(%rdi)
    mov     %cx, -2(%rdi)
    ret

    .p2align 4
L(write_76bytes):
    movdqu    -76(%rsi), %xmm0
    movdqu    -60(%rsi), %xmm1
    mov    -44(%rsi), %r8
    mov    -36(%rsi), %r9
    mov    -28(%rsi), %r10
    mov    -20(%rsi), %r11
    mov    -12(%rsi), %rcx
    mov    -4(%rsi), %edx
    movdqu     %xmm0, -76(%rdi)
    movdqu     %xmm1, -60(%rdi)
    mov     %r8, -44(%rdi)
    mov     %r9, -36(%rdi)
    mov     %r10, -28(%rdi)
    mov     %r11, -20(%rdi)
    mov     %rcx, -12(%rdi)
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_68bytes):
    movdqu    -68(%rsi), %xmm0
    movdqu    -52(%rsi), %xmm1
    mov    -36(%rsi), %r9
    mov    -28(%rsi), %r10
    mov    -20(%rsi), %r11
    mov    -12(%rsi), %rcx
    mov    -4(%rsi), %edx
    movdqu     %xmm0, -68(%rdi)
    movdqu     %xmm1, -52(%rdi)
    mov     %r9, -36(%rdi)
    mov     %r10, -28(%rdi)
    mov     %r11, -20(%rdi)
    mov     %rcx, -12(%rdi)
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_60bytes):
    movdqu    -60(%rsi), %xmm0
    mov    -44(%rsi), %r8
    mov    -36(%rsi), %r9
    mov    -28(%rsi), %r10
    mov    -20(%rsi), %r11
    mov    -12(%rsi), %rcx
    mov    -4(%rsi), %edx
    movdqu     %xmm0, -60(%rdi)
    mov     %r8, -44(%rdi)
    mov     %r9, -36(%rdi)
    mov     %r10, -28(%rdi)
    mov     %r11, -20(%rdi)
    mov     %rcx, -12(%rdi)
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_52bytes):
    movdqu    -52(%rsi), %xmm0
    mov    -36(%rsi), %r9
    mov    -28(%rsi), %r10
    mov    -20(%rsi), %r11
    mov    -12(%rsi), %rcx
    mov    -4(%rsi), %edx
    movdqu     %xmm0, -52(%rdi)
    mov     %r9, -36(%rdi)
    mov     %r10, -28(%rdi)
    mov     %r11, -20(%rdi)
    mov     %rcx, -12(%rdi)
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_44bytes):
    mov    -44(%rsi), %r8
    mov    -36(%rsi), %r9
    mov    -28(%rsi), %r10
    mov    -20(%rsi), %r11
    mov    -12(%rsi), %rcx
    mov    -4(%rsi), %edx
    mov     %r8, -44(%rdi)
    mov     %r9, -36(%rdi)
    mov     %r10, -28(%rdi)
    mov     %r11, -20(%rdi)
    mov     %rcx, -12(%rdi)
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_36bytes):
    mov    -36(%rsi), %r9
    mov    -28(%rsi), %r10
    mov    -20(%rsi), %r11
    mov    -12(%rsi), %rcx
    mov    -4(%rsi), %edx
    mov     %r9, -36(%rdi)
    mov     %r10, -28(%rdi)
    mov     %r11, -20(%rdi)
    mov     %rcx, -12(%rdi)
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_28bytes):
    mov    -28(%rsi), %r10
    mov    -20(%rsi), %r11
    mov    -12(%rsi), %rcx
    mov    -4(%rsi), %edx
    mov     %r10, -28(%rdi)
    mov     %r11, -20(%rdi)
    mov     %rcx, -12(%rdi)
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_20bytes):
    mov    -20(%rsi), %r11
    mov    -12(%rsi), %rcx
    mov    -4(%rsi), %edx
    mov     %r11, -20(%rdi)
    mov     %rcx, -12(%rdi)
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_12bytes):
    mov    -12(%rsi), %rcx
    mov    -4(%rsi), %edx
    mov     %rcx, -12(%rdi)
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_4bytes):
    mov    -4(%rsi), %edx
    mov     %edx, -4(%rdi)
    ret

    .p2align 4
L(write_77bytes):
    movdqu    -77(%rsi), %xmm0
    movdqu    -61(%rsi), %xmm1
    mov    -45(%rsi), %r8
    mov    -37(%rsi), %r9
    mov    -29(%rsi), %r10
    mov    -21(%rsi), %r11
    mov    -13(%rsi), %rcx
    mov    -8(%rsi), %rdx
    movdqu     %xmm0, -77(%rdi)
    movdqu     %xmm1, -61(%rdi)
    mov     %r8, -45(%rdi)
    mov     %r9, -37(%rdi)
    mov     %r10, -29(%rdi)
    mov     %r11, -21(%rdi)
    mov     %rcx, -13(%rdi)
    mov     %rdx, -8(%rdi)
    ret

    .p2align 4
L(write_69bytes):
    movdqu    -69(%rsi), %xmm0
    movdqu    -53(%rsi), %xmm1
    mov    -37(%rsi), %r9
    mov    -29(%rsi), %r10
    mov    -21(%rsi), %r11
    mov    -13(%rsi), %rcx
    mov    -8(%rsi), %rdx
    movdqu     %xmm0, -69(%rdi)
    movdqu     %xmm1, -53(%rdi)
    mov     %r9, -37(%rdi)
    mov     %r10, -29(%rdi)
    mov     %r11, -21(%rdi)
    mov     %rcx, -13(%rdi)
    mov     %rdx, -8(%rdi)
    ret

    .p2align 4
L(write_61bytes):
    movdqu    -61(%rsi), %xmm0
    mov    -45(%rsi), %r8
    mov    -37(%rsi), %r9
    mov    -29(%rsi), %r10
    mov    -21(%rsi), %r11
    mov    -13(%rsi), %rcx
    mov    -8(%rsi), %rdx
    movdqu     %xmm0, -61(%rdi)
    mov     %r8, -45(%rdi)
    mov     %r9, -37(%rdi)
    mov     %r10, -29(%rdi)
    mov     %r11, -21(%rdi)
    mov     %rcx, -13(%rdi)
    mov     %rdx, -8(%rdi)
    ret

    .p2align 4
L(write_53bytes):
    movdqu    -53(%rsi), %xmm0
    mov    -45(%rsi), %r8
    mov    -37(%rsi), %r9
    mov    -29(%rsi), %r10
    mov    -21(%rsi), %r11
    mov    -13(%rsi), %rcx
    mov    -8(%rsi), %rdx
    movdqu     %xmm0, -53(%rdi)
    mov     %r9, -37(%rdi)
    mov     %r10, -29(%rdi)
    mov     %r11, -21(%rdi)
    mov     %rcx, -13(%rdi)
    mov     %rdx, -8(%rdi)
    ret

    .p2align 4
L(write_45bytes):
    mov    -45(%rsi), %r8
    mov    -37(%rsi), %r9
    mov    -29(%rsi), %r10
    mov    -21(%rsi), %r11
    mov    -13(%rsi), %rcx
    mov    -8(%rsi), %rdx
    mov     %r8, -45(%rdi)
    mov     %r9, -37(%rdi)
    mov     %r10, -29(%rdi)
    mov     %r11, -21(%rdi)
    mov     %rcx, -13(%rdi)
    mov     %rdx, -8(%rdi)
    ret

    .p2align 4
L(write_37bytes):
    mov    -37(%rsi), %r9
    mov    -29(%rsi), %r10
    mov    -21(%rsi), %r11
    mov    -13(%rsi), %rcx
    mov    -8(%rsi), %rdx
    mov     %r9, -37(%rdi)
    mov     %r10, -29(%rdi)
    mov     %r11, -21(%rdi)
    mov     %rcx, -13(%rdi)
    mov     %rdx, -8(%rdi)
    ret

    .p2align 4
L(write_29bytes):
    mov    -29(%rsi), %r10
    mov    -21(%rsi), %r11
    mov    -13(%rsi), %rcx
    mov    -8(%rsi), %rdx
    mov     %r10, -29(%rdi)
    mov     %r11, -21(%rdi)
    mov     %rcx, -13(%rdi)
    mov     %rdx, -8(%rdi)
    ret

    .p2align 4
L(write_21bytes):
    mov    -21(%rsi), %r11
    mov    -13(%rsi), %rcx
    mov    -8(%rsi), %rdx
    mov     %r11, -21(%rdi)
    mov     %rcx, -13(%rdi)
    mov     %rdx, -8(%rdi)
    ret

    .p2align 4
L(write_13bytes):
    mov    -13(%rsi), %rcx
    mov    -8(%rsi), %rdx
    mov     %rcx, -13(%rdi)
    mov     %rdx, -8(%rdi)
    ret

    .p2align 4
L(write_5bytes):
    mov    -5(%rsi), %edx
    mov    -4(%rsi), %ecx
    mov     %edx, -5(%rdi)
    mov     %ecx, -4(%rdi)
    ret

    .p2align 4
L(write_78bytes):
    movdqu    -78(%rsi), %xmm0
    movdqu    -62(%rsi), %xmm1
    mov    -46(%rsi), %r8
    mov    -38(%rsi), %r9
    mov    -30(%rsi), %r10
    mov    -22(%rsi), %r11
    mov    -14(%rsi), %rcx
    mov    -8(%rsi), %rdx
    movdqu     %xmm0, -78(%rdi)
    movdqu     %xmm1, -62(%rdi)
    mov     %r8, -46(%rdi)
    mov     %r9, -38(%rdi)
    mov     %r10, -30(%rdi)
    mov     %r11, -22(%rdi)
    mov     %rcx, -14(%rdi)
    mov     %rdx, -8(%rdi)
    ret

    .p2align 4
L(write_70bytes):
    movdqu    -70(%rsi), %xmm0
    movdqu    -54(%rsi), %xmm1
    mov    -38(%rsi), %r9
    mov    -30(%rsi), %r10
    mov    -22(%rsi), %r11
    mov    -14(%rsi), %rcx
    mov    -8(%rsi), %rdx
    movdqu     %xmm0, -70(%rdi)
    movdqu     %xmm1, -54(%rdi)
    mov     %r9, -38(%rdi)
    mov     %r10, -30(%rdi)
    mov     %r11, -22(%rdi)
    mov     %rcx, -14(%rdi)
    mov     %rdx, -8(%rdi)
    ret

    .p2align 4
L(write_62bytes):
    movdqu    -62(%rsi), %xmm0
    mov    -46(%rsi), %r8
    mov    -38(%rsi), %r9
    mov    -30(%rsi), %r10
    mov    -22(%rsi), %r11
    mov    -14(%rsi), %rcx
    mov    -8(%rsi), %rdx
    movdqu     %xmm0, -62(%rdi)
    mov     %r8, -46(%rdi)
    mov     %r9, -38(%rdi)
    mov     %r10, -30(%rdi)
    mov     %r11, -22(%rdi)
    mov     %rcx, -14(%rdi)
    mov     %rdx, -8(%rdi)
    ret

    .p2align 4
L(write_54bytes):
    movdqu    -54(%rsi), %xmm0
    mov    -38(%rsi), %r9
    mov    -30(%rsi), %r10
    mov    -22(%rsi), %r11
    mov    -14(%rsi), %rcx
    mov    -8(%rsi), %rdx
    movdqu     %xmm0, -54(%rdi)
    mov     %r9, -38(%rdi)
    mov     %r10, -30(%rdi)
    mov     %r11, -22(%rdi)
    mov     %rcx, -14(%rdi)
    mov     %rdx, -8(%rdi)
    ret

    .p2align 4
L(write_46bytes):
    mov    -46(%rsi), %r8
    mov    -38(%rsi), %r9
    mov    -30(%rsi), %r10
    mov    -22(%rsi), %r11
    mov    -14(%rsi), %rcx
    mov    -8(%rsi), %rdx
    mov     %r8, -46(%rdi)
    mov     %r9, -38(%rdi)
    mov     %r10, -30(%rdi)
    mov     %r11, -22(%rdi)
    mov     %rcx, -14(%rdi)
    mov     %rdx, -8(%rdi)
    ret

    .p2align 4
L(write_38bytes):
    mov    -38(%rsi), %r9
    mov    -30(%rsi), %r10
    mov    -22(%rsi), %r11
    mov    -14(%rsi), %rcx
    mov    -8(%rsi), %rdx
    mov     %r9, -38(%rdi)
    mov     %r10, -30(%rdi)
    mov     %r11, -22(%rdi)
    mov     %rcx, -14(%rdi)
    mov     %rdx, -8(%rdi)
    ret

    .p2align 4
L(write_30bytes):
    mov    -30(%rsi), %r10
    mov    -22(%rsi), %r11
    mov    -14(%rsi), %rcx
    mov    -8(%rsi), %rdx
    mov     %r10, -30(%rdi)
    mov     %r11, -22(%rdi)
    mov     %rcx, -14(%rdi)
    mov     %rdx, -8(%rdi)
    ret

    .p2align 4
L(write_22bytes):
    mov    -22(%rsi), %r11
    mov    -14(%rsi), %rcx
    mov    -8(%rsi), %rdx
    mov     %r11, -22(%rdi)
    mov     %rcx, -14(%rdi)
    mov     %rdx, -8(%rdi)
    ret

    .p2align 4
L(write_14bytes):
    mov    -14(%rsi), %rcx
    mov    -8(%rsi), %rdx
    mov     %rcx, -14(%rdi)
    mov     %rdx, -8(%rdi)
    ret

    .p2align 4
L(write_6bytes):
    mov    -6(%rsi), %edx
    mov    -4(%rsi), %ecx
    mov     %edx, -6(%rdi)
    mov     %ecx, -4(%rdi)
    ret

    .p2align 4
L(write_79bytes):
    movdqu    -79(%rsi), %xmm0
    movdqu    -63(%rsi), %xmm1
    mov    -47(%rsi), %r8
    mov    -39(%rsi), %r9
    mov    -31(%rsi), %r10
    mov    -23(%rsi), %r11
    mov    -15(%rsi), %rcx
    mov    -8(%rsi), %rdx
    movdqu     %xmm0, -79(%rdi)
    movdqu     %xmm1, -63(%rdi)
    mov     %r8, -47(%rdi)
    mov     %r9, -39(%rdi)
    mov     %r10, -31(%rdi)
    mov     %r11, -23(%rdi)
    mov     %rcx, -15(%rdi)
    mov     %rdx, -8(%rdi)
    ret

    .p2align 4
L(write_71bytes):
    movdqu    -71(%rsi), %xmm0
    movdqu    -55(%rsi), %xmm1
    mov    -39(%rsi), %r9
    mov    -31(%rsi), %r10
    mov    -23(%rsi), %r11
    mov    -15(%rsi), %rcx
    mov    -8(%rsi), %rdx
    movdqu     %xmm0, -71(%rdi)
    movdqu     %xmm1, -55(%rdi)
    mov     %r9, -39(%rdi)
    mov     %r10, -31(%rdi)
    mov     %r11, -23(%rdi)
    mov     %rcx, -15(%rdi)
    mov     %rdx, -8(%rdi)
    ret

    .p2align 4
L(write_63bytes):
    movdqu    -63(%rsi), %xmm0
    mov    -47(%rsi), %r8
    mov    -39(%rsi), %r9
    mov    -31(%rsi), %r10
    mov    -23(%rsi), %r11
    mov    -15(%rsi), %rcx
    mov    -8(%rsi), %rdx
    movdqu     %xmm0, -63(%rdi)
    mov     %r8, -47(%rdi)
    mov     %r9, -39(%rdi)
    mov     %r10, -31(%rdi)
    mov     %r11, -23(%rdi)
    mov     %rcx, -15(%rdi)
    mov     %rdx, -8(%rdi)
    ret

    .p2align 4
L(write_55bytes):
    movdqu    -55(%rsi), %xmm0
    mov    -39(%rsi), %r9
    mov    -31(%rsi), %r10
    mov    -23(%rsi), %r11
    mov    -15(%rsi), %rcx
    mov    -8(%rsi), %rdx
    movdqu     %xmm0, -55(%rdi)
    mov     %r9, -39(%rdi)
    mov     %r10, -31(%rdi)
    mov     %r11, -23(%rdi)
    mov     %rcx, -15(%rdi)
    mov     %rdx, -8(%rdi)
    ret

    .p2align 4
L(write_47bytes):
    mov    -47(%rsi), %r8
    mov    -39(%rsi), %r9
    mov    -31(%rsi), %r10
    mov    -23(%rsi), %r11
    mov    -15(%rsi), %rcx
    mov    -8(%rsi), %rdx
    mov     %r8, -47(%rdi)
    mov     %r9, -39(%rdi)
    mov     %r10, -31(%rdi)
    mov     %r11, -23(%rdi)
    mov     %rcx, -15(%rdi)
    mov     %rdx, -8(%rdi)
    ret

    .p2align 4
L(write_39bytes):
    mov    -39(%rsi), %r9
    mov    -31(%rsi), %r10
    mov    -23(%rsi), %r11
    mov    -15(%rsi), %rcx
    mov    -8(%rsi), %rdx
    mov     %r9, -39(%rdi)
    mov     %r10, -31(%rdi)
    mov     %r11, -23(%rdi)
    mov     %rcx, -15(%rdi)
    mov     %rdx, -8(%rdi)
    ret

    .p2align 4
L(write_31bytes):
    mov    -31(%rsi), %r10
    mov    -23(%rsi), %r11
    mov    -15(%rsi), %rcx
    mov    -8(%rsi), %rdx
    mov     %r10, -31(%rdi)
    mov     %r11, -23(%rdi)
    mov     %rcx, -15(%rdi)
    mov     %rdx, -8(%rdi)
    ret

    .p2align 4
L(write_23bytes):
    mov    -23(%rsi), %r11
    mov    -15(%rsi), %rcx
    mov    -8(%rsi), %rdx
    mov     %r11, -23(%rdi)
    mov     %rcx, -15(%rdi)
    mov     %rdx, -8(%rdi)
    ret

    .p2align 4
L(write_15bytes):
    mov    -15(%rsi), %rcx
    mov    -8(%rsi), %rdx
    mov     %rcx, -15(%rdi)
    mov     %rdx, -8(%rdi)
    ret

    .p2align 4
L(write_7bytes):
    mov    -7(%rsi), %edx
    mov    -4(%rsi), %ecx
    mov     %edx, -7(%rdi)
    mov     %ecx, -4(%rdi)
    ret

    .p2align 4
L(large_page_fwd):
    movdqu    (%rsi), %xmm1
    lea    16(%rsi), %rsi
    movdqu    %xmm0, (%r8)
    movntdq    %xmm1, (%rdi)
    lea    16(%rdi), %rdi
    lea    -0x90(%rdx), %rdx
#ifdef USE_AS_MEMMOVE
    mov    %rsi, %r9
    sub    %rdi, %r9
    cmp    %rdx, %r9
    jae    L(memmove_is_memcpy_fwd)
    shl    $2, %rcx
    cmp    %rcx, %rdx
    jb    L(ll_cache_copy_fwd_start)
L(memmove_is_memcpy_fwd):
#endif
L(large_page_loop):
    movdqu    (%rsi), %xmm0
    movdqu    0x10(%rsi), %xmm1
    movdqu    0x20(%rsi), %xmm2
    movdqu    0x30(%rsi), %xmm3
    movdqu    0x40(%rsi), %xmm4
    movdqu    0x50(%rsi), %xmm5
    movdqu    0x60(%rsi), %xmm6
    movdqu    0x70(%rsi), %xmm7
    lea    0x80(%rsi), %rsi

    sub    $0x80, %rdx
    movntdq    %xmm0, (%rdi)
    movntdq    %xmm1, 0x10(%rdi)
    movntdq    %xmm2, 0x20(%rdi)
    movntdq    %xmm3, 0x30(%rdi)
    movntdq    %xmm4, 0x40(%rdi)
    movntdq    %xmm5, 0x50(%rdi)
    movntdq    %xmm6, 0x60(%rdi)
    movntdq    %xmm7, 0x70(%rdi)
    lea    0x80(%rdi), %rdi
    jae    L(large_page_loop)
    cmp    $-0x40, %rdx
    lea    0x80(%rdx), %rdx
    jl    L(large_page_less_64bytes)

    movdqu    (%rsi), %xmm0
    movdqu    0x10(%rsi), %xmm1
    movdqu    0x20(%rsi), %xmm2
    movdqu    0x30(%rsi), %xmm3
    lea    0x40(%rsi), %rsi

    movntdq    %xmm0, (%rdi)
    movntdq    %xmm1, 0x10(%rdi)
    movntdq    %xmm2, 0x20(%rdi)
    movntdq    %xmm3, 0x30(%rdi)
    lea    0x40(%rdi), %rdi
    sub    $0x40, %rdx
L(large_page_less_64bytes):
    add    %rdx, %rsi
    add    %rdx, %rdi
    sfence
    BRANCH_TO_JMPTBL_ENTRY (L(table_less_80bytes), %rdx, 4)

#ifdef USE_AS_MEMMOVE
    .p2align 4
L(ll_cache_copy_fwd_start):
    prefetcht0 0x1c0(%rsi)
    prefetcht0 0x200(%rsi)
    movdqu    (%rsi), %xmm0
    movdqu    0x10(%rsi), %xmm1
    movdqu    0x20(%rsi), %xmm2
    movdqu    0x30(%rsi), %xmm3
    movdqu    0x40(%rsi), %xmm4
    movdqu    0x50(%rsi), %xmm5
    movdqu    0x60(%rsi), %xmm6
    movdqu    0x70(%rsi), %xmm7
    lea    0x80(%rsi), %rsi

    sub    $0x80, %rdx
    movaps    %xmm0, (%rdi)
    movaps    %xmm1, 0x10(%rdi)
    movaps    %xmm2, 0x20(%rdi)
    movaps    %xmm3, 0x30(%rdi)
    movaps    %xmm4, 0x40(%rdi)
    movaps    %xmm5, 0x50(%rdi)
    movaps    %xmm6, 0x60(%rdi)
    movaps    %xmm7, 0x70(%rdi)
    lea    0x80(%rdi), %rdi
    jae    L(ll_cache_copy_fwd_start)
    cmp    $-0x40, %rdx
    lea    0x80(%rdx), %rdx
    jl    L(large_page_ll_less_fwd_64bytes)

    movdqu    (%rsi), %xmm0
    movdqu    0x10(%rsi), %xmm1
    movdqu    0x20(%rsi), %xmm2
    movdqu    0x30(%rsi), %xmm3
    lea    0x40(%rsi), %rsi

    movaps    %xmm0, (%rdi)
    movaps    %xmm1, 0x10(%rdi)
    movaps    %xmm2, 0x20(%rdi)
    movaps    %xmm3, 0x30(%rdi)
    lea    0x40(%rdi), %rdi
    sub    $0x40, %rdx
L(large_page_ll_less_fwd_64bytes):
    add    %rdx, %rsi
    add    %rdx, %rdi
    BRANCH_TO_JMPTBL_ENTRY (L(table_less_80bytes), %rdx, 4)

#endif
    .p2align 4
L(large_page_bwd):
    movdqu    -0x10(%rsi), %xmm1
    lea    -16(%rsi), %rsi
    movdqu    %xmm0, (%r8)
    movdqa    %xmm1, -0x10(%rdi)
    lea    -16(%rdi), %rdi
    lea    -0x90(%rdx), %rdx
#ifdef USE_AS_MEMMOVE
    mov    %rdi, %r9
    sub    %rsi, %r9
    cmp    %rdx, %r9
    jae    L(memmove_is_memcpy_bwd)
    cmp    %rcx, %r9
    jb    L(ll_cache_copy_bwd_start)
L(memmove_is_memcpy_bwd):
#endif
L(large_page_bwd_loop):
    movdqu    -0x10(%rsi), %xmm0
    movdqu    -0x20(%rsi), %xmm1
    movdqu    -0x30(%rsi), %xmm2
    movdqu    -0x40(%rsi), %xmm3
    movdqu    -0x50(%rsi), %xmm4
    movdqu    -0x60(%rsi), %xmm5
    movdqu    -0x70(%rsi), %xmm6
    movdqu    -0x80(%rsi), %xmm7
    lea    -0x80(%rsi), %rsi

    sub    $0x80, %rdx
    movntdq    %xmm0, -0x10(%rdi)
    movntdq    %xmm1, -0x20(%rdi)
    movntdq    %xmm2, -0x30(%rdi)
    movntdq    %xmm3, -0x40(%rdi)
    movntdq    %xmm4, -0x50(%rdi)
    movntdq    %xmm5, -0x60(%rdi)
    movntdq    %xmm6, -0x70(%rdi)
    movntdq    %xmm7, -0x80(%rdi)
    lea    -0x80(%rdi), %rdi
    jae    L(large_page_bwd_loop)
    cmp    $-0x40, %rdx
    lea    0x80(%rdx), %rdx
    jl    L(large_page_less_bwd_64bytes)

    movdqu    -0x10(%rsi), %xmm0
    movdqu    -0x20(%rsi), %xmm1
    movdqu    -0x30(%rsi), %xmm2
    movdqu    -0x40(%rsi), %xmm3
    lea    -0x40(%rsi), %rsi

    movntdq    %xmm0, -0x10(%rdi)
    movntdq    %xmm1, -0x20(%rdi)
    movntdq    %xmm2, -0x30(%rdi)
    movntdq    %xmm3, -0x40(%rdi)
    lea    -0x40(%rdi), %rdi
    sub    $0x40, %rdx
L(large_page_less_bwd_64bytes):
    sfence
    BRANCH_TO_JMPTBL_ENTRY (L(table_less_80bytes), %rdx, 4)

#ifdef USE_AS_MEMMOVE
    .p2align 4
L(ll_cache_copy_bwd_start):
    prefetcht0 -0x1c0(%rsi)
    prefetcht0 -0x200(%rsi)
    movdqu    -0x10(%rsi), %xmm0
    movdqu    -0x20(%rsi), %xmm1
    movdqu    -0x30(%rsi), %xmm2
    movdqu    -0x40(%rsi), %xmm3
    movdqu    -0x50(%rsi), %xmm4
    movdqu    -0x60(%rsi), %xmm5
    movdqu    -0x70(%rsi), %xmm6
    movdqu    -0x80(%rsi), %xmm7
    lea    -0x80(%rsi), %rsi

    sub    $0x80, %rdx
    movaps    %xmm0, -0x10(%rdi)
    movaps    %xmm1, -0x20(%rdi)
    movaps    %xmm2, -0x30(%rdi)
    movaps    %xmm3, -0x40(%rdi)
    movaps    %xmm4, -0x50(%rdi)
    movaps    %xmm5, -0x60(%rdi)
    movaps    %xmm6, -0x70(%rdi)
    movaps    %xmm7, -0x80(%rdi)
    lea    -0x80(%rdi), %rdi
    jae    L(ll_cache_copy_bwd_start)
    cmp    $-0x40, %rdx
    lea    0x80(%rdx), %rdx
    jl    L(large_page_ll_less_bwd_64bytes)

    movdqu    -0x10(%rsi), %xmm0
    movdqu    -0x20(%rsi), %xmm1
    movdqu    -0x30(%rsi), %xmm2
    movdqu    -0x40(%rsi), %xmm3
    lea    -0x40(%rsi), %rsi

    movaps    %xmm0, -0x10(%rdi)
    movaps    %xmm1, -0x20(%rdi)
    movaps    %xmm2, -0x30(%rdi)
    movaps    %xmm3, -0x40(%rdi)
    lea    -0x40(%rdi), %rdi
    sub    $0x40, %rdx
L(large_page_ll_less_bwd_64bytes):
    BRANCH_TO_JMPTBL_ENTRY (L(table_less_80bytes), %rdx, 4)
#endif

END (MEMCPY)

    .section .rodata.ssse3,"a",@progbits
    .p2align 3
L(table_less_80bytes):
    .int    JMPTBL (L(write_0bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_1bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_2bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_3bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_4bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_5bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_6bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_7bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_8bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_9bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_10bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_11bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_12bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_13bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_14bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_15bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_16bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_17bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_18bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_19bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_20bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_21bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_22bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_23bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_24bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_25bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_26bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_27bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_28bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_29bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_30bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_31bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_32bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_33bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_34bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_35bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_36bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_37bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_38bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_39bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_40bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_41bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_42bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_43bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_44bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_45bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_46bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_47bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_48bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_49bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_50bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_51bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_52bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_53bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_54bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_55bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_56bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_57bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_58bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_59bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_60bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_61bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_62bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_63bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_64bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_65bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_66bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_67bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_68bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_69bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_70bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_71bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_72bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_73bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_74bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_75bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_76bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_77bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_78bytes), L(table_less_80bytes))
    .int    JMPTBL (L(write_79bytes), L(table_less_80bytes))

    .p2align 3
L(shl_table):
    .int    JMPTBL (L(shl_0), L(shl_table))
    .int    JMPTBL (L(shl_1), L(shl_table))
    .int    JMPTBL (L(shl_2), L(shl_table))
    .int    JMPTBL (L(shl_3), L(shl_table))
    .int    JMPTBL (L(shl_4), L(shl_table))
    .int    JMPTBL (L(shl_5), L(shl_table))
    .int    JMPTBL (L(shl_6), L(shl_table))
    .int    JMPTBL (L(shl_7), L(shl_table))
    .int    JMPTBL (L(shl_8), L(shl_table))
    .int    JMPTBL (L(shl_9), L(shl_table))
    .int    JMPTBL (L(shl_10), L(shl_table))
    .int    JMPTBL (L(shl_11), L(shl_table))
    .int    JMPTBL (L(shl_12), L(shl_table))
    .int    JMPTBL (L(shl_13), L(shl_table))
    .int    JMPTBL (L(shl_14), L(shl_table))
    .int    JMPTBL (L(shl_15), L(shl_table))

    .p2align 3
L(shl_table_bwd):
    .int    JMPTBL (L(shl_0_bwd), L(shl_table_bwd))
    .int    JMPTBL (L(shl_1_bwd), L(shl_table_bwd))
    .int    JMPTBL (L(shl_2_bwd), L(shl_table_bwd))
    .int    JMPTBL (L(shl_3_bwd), L(shl_table_bwd))
    .int    JMPTBL (L(shl_4_bwd), L(shl_table_bwd))
    .int    JMPTBL (L(shl_5_bwd), L(shl_table_bwd))
    .int    JMPTBL (L(shl_6_bwd), L(shl_table_bwd))
    .int    JMPTBL (L(shl_7_bwd), L(shl_table_bwd))
    .int    JMPTBL (L(shl_8_bwd), L(shl_table_bwd))
    .int    JMPTBL (L(shl_9_bwd), L(shl_table_bwd))
    .int    JMPTBL (L(shl_10_bwd), L(shl_table_bwd))
    .int    JMPTBL (L(shl_11_bwd), L(shl_table_bwd))
    .int    JMPTBL (L(shl_12_bwd), L(shl_table_bwd))
    .int    JMPTBL (L(shl_13_bwd), L(shl_table_bwd))
    .int    JMPTBL (L(shl_14_bwd), L(shl_table_bwd))
    .int    JMPTBL (L(shl_15_bwd), L(shl_table_bwd))

#endif
