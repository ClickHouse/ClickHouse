/* memcpy with SSSE3 and REP string
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
# define MEMCPY        __memcpy_ssse3_back
# define MEMCPY_CHK    __memcpy_chk_ssse3_back
# define MEMPCPY    __mempcpy_ssse3_back
# define MEMPCPY_CHK    __mempcpy_chk_ssse3_back
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
    je    L(bwd_write_0bytes)
    cmp    $144, %rdx
    jae    L(copy_backward)
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_bwd), %rdx, 4)
L(copy_forward):
#endif
L(start):
    cmp    $144, %rdx
    jae    L(144bytesormore)

L(fwd_write_less32bytes):
#ifndef USE_AS_MEMMOVE
    cmp    %dil, %sil
    jbe    L(bk_write)
#endif
    add    %rdx, %rsi
    add    %rdx, %rdi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_fwd), %rdx, 4)
#ifndef USE_AS_MEMMOVE
L(bk_write):

    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_bwd), %rdx, 4)
#endif

    .p2align 4
L(144bytesormore):

#ifndef USE_AS_MEMMOVE
    cmp    %dil, %sil
    jle    L(copy_backward)
#endif
    movdqu    (%rsi), %xmm0
    mov    %rdi, %r8
    and    $-16, %rdi
    add    $16, %rdi
    mov    %rdi, %r9
    sub    %r8, %r9
    sub    %r9, %rdx
    add    %r9, %rsi
    mov    %rsi, %r9
    and    $0xf, %r9
    jz    L(shl_0)
#ifdef DATA_CACHE_SIZE
    mov    $DATA_CACHE_SIZE, %RCX_LP
#else
    mov    __x86_data_cache_size(%rip), %RCX_LP
#endif
    cmp    %rcx, %rdx
    jae    L(gobble_mem_fwd)
    lea        L(shl_table_fwd)(%rip), %r11
    sub    $0x80, %rdx
    movslq    (%r11, %r9, 4), %r9
    add    %r11, %r9
    _CET_NOTRACK jmp *%r9
    ud2

    .p2align 4
L(copy_backward):
#ifdef DATA_CACHE_SIZE
    mov    $DATA_CACHE_SIZE, %RCX_LP
#else
    mov    __x86_data_cache_size(%rip), %RCX_LP
#endif
    shl    $1, %rcx
    cmp    %rcx, %rdx
    ja    L(gobble_mem_bwd)

    add    %rdx, %rdi
    add    %rdx, %rsi
    movdqu    -16(%rsi), %xmm0
    lea    -16(%rdi), %r8
    mov    %rdi, %r9
    and    $0xf, %r9
    xor    %r9, %rdi
    sub    %r9, %rsi
    sub    %r9, %rdx
    mov    %rsi, %r9
    and    $0xf, %r9
    jz    L(shl_0_bwd)
    lea        L(shl_table_bwd)(%rip), %r11
    sub    $0x80, %rdx
    movslq    (%r11, %r9, 4), %r9
    add    %r11, %r9
    _CET_NOTRACK jmp *%r9
    ud2

    .p2align 4
L(shl_0):

    mov    %rdx, %r9
    shr    $8, %r9
    add    %rdx, %r9
#ifdef DATA_CACHE_SIZE
    cmp    $DATA_CACHE_SIZE_HALF, %R9_LP
#else
    cmp    __x86_data_cache_size_half(%rip), %R9_LP
#endif
    jae    L(gobble_mem_fwd)
    sub    $0x80, %rdx
    .p2align 4
L(shl_0_loop):
    movdqa    (%rsi), %xmm1
    movdqa    %xmm1, (%rdi)
    movaps    0x10(%rsi), %xmm2
    movaps    %xmm2, 0x10(%rdi)
    movaps    0x20(%rsi), %xmm3
    movaps    %xmm3, 0x20(%rdi)
    movaps    0x30(%rsi), %xmm4
    movaps    %xmm4, 0x30(%rdi)
    movaps    0x40(%rsi), %xmm1
    movaps    %xmm1, 0x40(%rdi)
    movaps    0x50(%rsi), %xmm2
    movaps    %xmm2, 0x50(%rdi)
    movaps    0x60(%rsi), %xmm3
    movaps    %xmm3, 0x60(%rdi)
    movaps    0x70(%rsi), %xmm4
    movaps    %xmm4, 0x70(%rdi)
    sub    $0x80, %rdx
    lea    0x80(%rsi), %rsi
    lea    0x80(%rdi), %rdi
    jae    L(shl_0_loop)
    movdqu    %xmm0, (%r8)
    add    $0x80, %rdx
    add    %rdx, %rsi
    add    %rdx, %rdi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_fwd), %rdx, 4)

    .p2align 4
L(shl_0_bwd):
    sub    $0x80, %rdx
L(copy_backward_loop):
    movaps    -0x10(%rsi), %xmm1
    movaps    %xmm1, -0x10(%rdi)
    movaps    -0x20(%rsi), %xmm2
    movaps    %xmm2, -0x20(%rdi)
    movaps    -0x30(%rsi), %xmm3
    movaps    %xmm3, -0x30(%rdi)
    movaps    -0x40(%rsi), %xmm4
    movaps    %xmm4, -0x40(%rdi)
    movaps    -0x50(%rsi), %xmm5
    movaps    %xmm5, -0x50(%rdi)
    movaps    -0x60(%rsi), %xmm5
    movaps    %xmm5, -0x60(%rdi)
    movaps    -0x70(%rsi), %xmm5
    movaps    %xmm5, -0x70(%rdi)
    movaps    -0x80(%rsi), %xmm5
    movaps    %xmm5, -0x80(%rdi)
    sub    $0x80, %rdx
    lea    -0x80(%rdi), %rdi
    lea    -0x80(%rsi), %rsi
    jae    L(copy_backward_loop)

    movdqu    %xmm0, (%r8)
    add    $0x80, %rdx
    sub    %rdx, %rdi
    sub    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_bwd), %rdx, 4)

    .p2align 4
L(shl_1):
    sub    $0x80, %rdx
    movaps    -0x01(%rsi), %xmm1
    movaps    0x0f(%rsi), %xmm2
    movaps    0x1f(%rsi), %xmm3
    movaps    0x2f(%rsi), %xmm4
    movaps    0x3f(%rsi), %xmm5
    movaps    0x4f(%rsi), %xmm6
    movaps    0x5f(%rsi), %xmm7
    movaps    0x6f(%rsi), %xmm8
    movaps    0x7f(%rsi), %xmm9
    lea    0x80(%rsi), %rsi
    palignr    $1, %xmm8, %xmm9
    movaps    %xmm9, 0x70(%rdi)
    palignr    $1, %xmm7, %xmm8
    movaps    %xmm8, 0x60(%rdi)
    palignr    $1, %xmm6, %xmm7
    movaps    %xmm7, 0x50(%rdi)
    palignr    $1, %xmm5, %xmm6
    movaps    %xmm6, 0x40(%rdi)
    palignr    $1, %xmm4, %xmm5
    movaps    %xmm5, 0x30(%rdi)
    palignr    $1, %xmm3, %xmm4
    movaps    %xmm4, 0x20(%rdi)
    palignr    $1, %xmm2, %xmm3
    movaps    %xmm3, 0x10(%rdi)
    palignr    $1, %xmm1, %xmm2
    movaps    %xmm2, (%rdi)
    lea    0x80(%rdi), %rdi
    jae    L(shl_1)
    movdqu    %xmm0, (%r8)
    add    $0x80, %rdx
    add    %rdx, %rdi
    add    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_fwd), %rdx, 4)

    .p2align 4
L(shl_1_bwd):
    movaps    -0x01(%rsi), %xmm1

    movaps    -0x11(%rsi), %xmm2
    palignr    $1, %xmm2, %xmm1
    movaps    %xmm1, -0x10(%rdi)

    movaps    -0x21(%rsi), %xmm3
    palignr    $1, %xmm3, %xmm2
    movaps    %xmm2, -0x20(%rdi)

    movaps    -0x31(%rsi), %xmm4
    palignr    $1, %xmm4, %xmm3
    movaps    %xmm3, -0x30(%rdi)

    movaps    -0x41(%rsi), %xmm5
    palignr    $1, %xmm5, %xmm4
    movaps    %xmm4, -0x40(%rdi)

    movaps    -0x51(%rsi), %xmm6
    palignr    $1, %xmm6, %xmm5
    movaps    %xmm5, -0x50(%rdi)

    movaps    -0x61(%rsi), %xmm7
    palignr    $1, %xmm7, %xmm6
    movaps    %xmm6, -0x60(%rdi)

    movaps    -0x71(%rsi), %xmm8
    palignr    $1, %xmm8, %xmm7
    movaps    %xmm7, -0x70(%rdi)

    movaps    -0x81(%rsi), %xmm9
    palignr    $1, %xmm9, %xmm8
    movaps    %xmm8, -0x80(%rdi)

    sub    $0x80, %rdx
    lea    -0x80(%rdi), %rdi
    lea    -0x80(%rsi), %rsi
    jae    L(shl_1_bwd)
    movdqu    %xmm0, (%r8)
    add    $0x80, %rdx
    sub    %rdx, %rdi
    sub    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_bwd), %rdx, 4)

    .p2align 4
L(shl_2):
    sub    $0x80, %rdx
    movaps    -0x02(%rsi), %xmm1
    movaps    0x0e(%rsi), %xmm2
    movaps    0x1e(%rsi), %xmm3
    movaps    0x2e(%rsi), %xmm4
    movaps    0x3e(%rsi), %xmm5
    movaps    0x4e(%rsi), %xmm6
    movaps    0x5e(%rsi), %xmm7
    movaps    0x6e(%rsi), %xmm8
    movaps    0x7e(%rsi), %xmm9
    lea    0x80(%rsi), %rsi
    palignr    $2, %xmm8, %xmm9
    movaps    %xmm9, 0x70(%rdi)
    palignr    $2, %xmm7, %xmm8
    movaps    %xmm8, 0x60(%rdi)
    palignr    $2, %xmm6, %xmm7
    movaps    %xmm7, 0x50(%rdi)
    palignr    $2, %xmm5, %xmm6
    movaps    %xmm6, 0x40(%rdi)
    palignr    $2, %xmm4, %xmm5
    movaps    %xmm5, 0x30(%rdi)
    palignr    $2, %xmm3, %xmm4
    movaps    %xmm4, 0x20(%rdi)
    palignr    $2, %xmm2, %xmm3
    movaps    %xmm3, 0x10(%rdi)
    palignr    $2, %xmm1, %xmm2
    movaps    %xmm2, (%rdi)
    lea    0x80(%rdi), %rdi
    jae    L(shl_2)
    movdqu    %xmm0, (%r8)
    add    $0x80, %rdx
    add    %rdx, %rdi
    add    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_fwd), %rdx, 4)

    .p2align 4
L(shl_2_bwd):
    movaps    -0x02(%rsi), %xmm1

    movaps    -0x12(%rsi), %xmm2
    palignr    $2, %xmm2, %xmm1
    movaps    %xmm1, -0x10(%rdi)

    movaps    -0x22(%rsi), %xmm3
    palignr    $2, %xmm3, %xmm2
    movaps    %xmm2, -0x20(%rdi)

    movaps    -0x32(%rsi), %xmm4
    palignr    $2, %xmm4, %xmm3
    movaps    %xmm3, -0x30(%rdi)

    movaps    -0x42(%rsi), %xmm5
    palignr    $2, %xmm5, %xmm4
    movaps    %xmm4, -0x40(%rdi)

    movaps    -0x52(%rsi), %xmm6
    palignr    $2, %xmm6, %xmm5
    movaps    %xmm5, -0x50(%rdi)

    movaps    -0x62(%rsi), %xmm7
    palignr    $2, %xmm7, %xmm6
    movaps    %xmm6, -0x60(%rdi)

    movaps    -0x72(%rsi), %xmm8
    palignr    $2, %xmm8, %xmm7
    movaps    %xmm7, -0x70(%rdi)

    movaps    -0x82(%rsi), %xmm9
    palignr    $2, %xmm9, %xmm8
    movaps    %xmm8, -0x80(%rdi)

    sub    $0x80, %rdx
    lea    -0x80(%rdi), %rdi
    lea    -0x80(%rsi), %rsi
    jae    L(shl_2_bwd)
    movdqu    %xmm0, (%r8)
    add    $0x80, %rdx
    sub    %rdx, %rdi
    sub    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_bwd), %rdx, 4)

    .p2align 4
L(shl_3):
    sub    $0x80, %rdx
    movaps -0x03(%rsi), %xmm1
    movaps    0x0d(%rsi), %xmm2
    movaps    0x1d(%rsi), %xmm3
    movaps    0x2d(%rsi), %xmm4
    movaps    0x3d(%rsi), %xmm5
    movaps    0x4d(%rsi), %xmm6
    movaps    0x5d(%rsi), %xmm7
    movaps    0x6d(%rsi), %xmm8
    movaps    0x7d(%rsi), %xmm9
    lea    0x80(%rsi), %rsi
    palignr    $3, %xmm8, %xmm9
    movaps    %xmm9, 0x70(%rdi)
    palignr    $3, %xmm7, %xmm8
    movaps    %xmm8, 0x60(%rdi)
    palignr    $3, %xmm6, %xmm7
    movaps    %xmm7, 0x50(%rdi)
    palignr    $3, %xmm5, %xmm6
    movaps    %xmm6, 0x40(%rdi)
    palignr    $3, %xmm4, %xmm5
    movaps    %xmm5, 0x30(%rdi)
    palignr    $3, %xmm3, %xmm4
    movaps    %xmm4, 0x20(%rdi)
    palignr    $3, %xmm2, %xmm3
    movaps    %xmm3, 0x10(%rdi)
    palignr    $3, %xmm1, %xmm2
    movaps    %xmm2, (%rdi)
    lea    0x80(%rdi), %rdi
    jae    L(shl_3)
    movdqu    %xmm0, (%r8)
    add    $0x80, %rdx
    add    %rdx, %rdi
    add    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_fwd), %rdx, 4)

    .p2align 4
L(shl_3_bwd):
    movaps    -0x03(%rsi), %xmm1

    movaps    -0x13(%rsi), %xmm2
    palignr    $3, %xmm2, %xmm1
    movaps    %xmm1, -0x10(%rdi)

    movaps    -0x23(%rsi), %xmm3
    palignr    $3, %xmm3, %xmm2
    movaps    %xmm2, -0x20(%rdi)

    movaps    -0x33(%rsi), %xmm4
    palignr    $3, %xmm4, %xmm3
    movaps    %xmm3, -0x30(%rdi)

    movaps    -0x43(%rsi), %xmm5
    palignr    $3, %xmm5, %xmm4
    movaps    %xmm4, -0x40(%rdi)

    movaps    -0x53(%rsi), %xmm6
    palignr    $3, %xmm6, %xmm5
    movaps    %xmm5, -0x50(%rdi)

    movaps    -0x63(%rsi), %xmm7
    palignr    $3, %xmm7, %xmm6
    movaps    %xmm6, -0x60(%rdi)

    movaps    -0x73(%rsi), %xmm8
    palignr    $3, %xmm8, %xmm7
    movaps    %xmm7, -0x70(%rdi)

    movaps    -0x83(%rsi), %xmm9
    palignr    $3, %xmm9, %xmm8
    movaps    %xmm8, -0x80(%rdi)

    sub    $0x80, %rdx
    lea    -0x80(%rdi), %rdi
    lea    -0x80(%rsi), %rsi
    jae    L(shl_3_bwd)
    movdqu    %xmm0, (%r8)
    add    $0x80, %rdx
    sub    %rdx, %rdi
    sub    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_bwd), %rdx, 4)

    .p2align 4
L(shl_4):
    sub    $0x80, %rdx
    movaps    -0x04(%rsi), %xmm1
    movaps    0x0c(%rsi), %xmm2
    movaps    0x1c(%rsi), %xmm3
    movaps    0x2c(%rsi), %xmm4
    movaps    0x3c(%rsi), %xmm5
    movaps    0x4c(%rsi), %xmm6
    movaps    0x5c(%rsi), %xmm7
    movaps    0x6c(%rsi), %xmm8
    movaps    0x7c(%rsi), %xmm9
    lea    0x80(%rsi), %rsi
    palignr    $4, %xmm8, %xmm9
    movaps    %xmm9, 0x70(%rdi)
    palignr    $4, %xmm7, %xmm8
    movaps    %xmm8, 0x60(%rdi)
    palignr    $4, %xmm6, %xmm7
    movaps    %xmm7, 0x50(%rdi)
    palignr    $4, %xmm5, %xmm6
    movaps    %xmm6, 0x40(%rdi)
    palignr    $4, %xmm4, %xmm5
    movaps    %xmm5, 0x30(%rdi)
    palignr    $4, %xmm3, %xmm4
    movaps    %xmm4, 0x20(%rdi)
    palignr    $4, %xmm2, %xmm3
    movaps    %xmm3, 0x10(%rdi)
    palignr    $4, %xmm1, %xmm2
    movaps    %xmm2, (%rdi)
    lea    0x80(%rdi), %rdi
    jae    L(shl_4)
    movdqu    %xmm0, (%r8)
    add    $0x80, %rdx
    add    %rdx, %rdi
    add    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_fwd), %rdx, 4)

    .p2align 4
L(shl_4_bwd):
    movaps    -0x04(%rsi), %xmm1

    movaps    -0x14(%rsi), %xmm2
    palignr    $4, %xmm2, %xmm1
    movaps    %xmm1, -0x10(%rdi)

    movaps    -0x24(%rsi), %xmm3
    palignr    $4, %xmm3, %xmm2
    movaps    %xmm2, -0x20(%rdi)

    movaps    -0x34(%rsi), %xmm4
    palignr    $4, %xmm4, %xmm3
    movaps    %xmm3, -0x30(%rdi)

    movaps    -0x44(%rsi), %xmm5
    palignr    $4, %xmm5, %xmm4
    movaps    %xmm4, -0x40(%rdi)

    movaps    -0x54(%rsi), %xmm6
    palignr    $4, %xmm6, %xmm5
    movaps    %xmm5, -0x50(%rdi)

    movaps    -0x64(%rsi), %xmm7
    palignr    $4, %xmm7, %xmm6
    movaps    %xmm6, -0x60(%rdi)

    movaps    -0x74(%rsi), %xmm8
    palignr    $4, %xmm8, %xmm7
    movaps    %xmm7, -0x70(%rdi)

    movaps    -0x84(%rsi), %xmm9
    palignr    $4, %xmm9, %xmm8
    movaps    %xmm8, -0x80(%rdi)

    sub    $0x80, %rdx
    lea    -0x80(%rdi), %rdi
    lea    -0x80(%rsi), %rsi
    jae    L(shl_4_bwd)
    movdqu    %xmm0, (%r8)
    add    $0x80, %rdx
    sub    %rdx, %rdi
    sub    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_bwd), %rdx, 4)

    .p2align 4
L(shl_5):
    sub    $0x80, %rdx
    movaps    -0x05(%rsi), %xmm1
    movaps    0x0b(%rsi), %xmm2
    movaps    0x1b(%rsi), %xmm3
    movaps    0x2b(%rsi), %xmm4
    movaps    0x3b(%rsi), %xmm5
    movaps    0x4b(%rsi), %xmm6
    movaps    0x5b(%rsi), %xmm7
    movaps    0x6b(%rsi), %xmm8
    movaps    0x7b(%rsi), %xmm9
    lea    0x80(%rsi), %rsi
    palignr    $5, %xmm8, %xmm9
    movaps    %xmm9, 0x70(%rdi)
    palignr    $5, %xmm7, %xmm8
    movaps    %xmm8, 0x60(%rdi)
    palignr    $5, %xmm6, %xmm7
    movaps    %xmm7, 0x50(%rdi)
    palignr    $5, %xmm5, %xmm6
    movaps    %xmm6, 0x40(%rdi)
    palignr    $5, %xmm4, %xmm5
    movaps    %xmm5, 0x30(%rdi)
    palignr    $5, %xmm3, %xmm4
    movaps    %xmm4, 0x20(%rdi)
    palignr    $5, %xmm2, %xmm3
    movaps    %xmm3, 0x10(%rdi)
    palignr    $5, %xmm1, %xmm2
    movaps    %xmm2, (%rdi)
    lea    0x80(%rdi), %rdi
    jae    L(shl_5)
    movdqu    %xmm0, (%r8)
    add    $0x80, %rdx
    add    %rdx, %rdi
    add    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_fwd), %rdx, 4)

    .p2align 4
L(shl_5_bwd):
    movaps    -0x05(%rsi), %xmm1

    movaps    -0x15(%rsi), %xmm2
    palignr    $5, %xmm2, %xmm1
    movaps    %xmm1, -0x10(%rdi)

    movaps    -0x25(%rsi), %xmm3
    palignr    $5, %xmm3, %xmm2
    movaps    %xmm2, -0x20(%rdi)

    movaps    -0x35(%rsi), %xmm4
    palignr    $5, %xmm4, %xmm3
    movaps    %xmm3, -0x30(%rdi)

    movaps    -0x45(%rsi), %xmm5
    palignr    $5, %xmm5, %xmm4
    movaps    %xmm4, -0x40(%rdi)

    movaps    -0x55(%rsi), %xmm6
    palignr    $5, %xmm6, %xmm5
    movaps    %xmm5, -0x50(%rdi)

    movaps    -0x65(%rsi), %xmm7
    palignr    $5, %xmm7, %xmm6
    movaps    %xmm6, -0x60(%rdi)

    movaps    -0x75(%rsi), %xmm8
    palignr    $5, %xmm8, %xmm7
    movaps    %xmm7, -0x70(%rdi)

    movaps    -0x85(%rsi), %xmm9
    palignr    $5, %xmm9, %xmm8
    movaps    %xmm8, -0x80(%rdi)

    sub    $0x80, %rdx
    lea    -0x80(%rdi), %rdi
    lea    -0x80(%rsi), %rsi
    jae    L(shl_5_bwd)
    movdqu    %xmm0, (%r8)
    add    $0x80, %rdx
    sub    %rdx, %rdi
    sub    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_bwd), %rdx, 4)

    .p2align 4
L(shl_6):
    sub    $0x80, %rdx
    movaps    -0x06(%rsi), %xmm1
    movaps    0x0a(%rsi), %xmm2
    movaps    0x1a(%rsi), %xmm3
    movaps    0x2a(%rsi), %xmm4
    movaps    0x3a(%rsi), %xmm5
    movaps    0x4a(%rsi), %xmm6
    movaps    0x5a(%rsi), %xmm7
    movaps    0x6a(%rsi), %xmm8
    movaps    0x7a(%rsi), %xmm9
    lea    0x80(%rsi), %rsi
    palignr    $6, %xmm8, %xmm9
    movaps    %xmm9, 0x70(%rdi)
    palignr    $6, %xmm7, %xmm8
    movaps    %xmm8, 0x60(%rdi)
    palignr    $6, %xmm6, %xmm7
    movaps    %xmm7, 0x50(%rdi)
    palignr    $6, %xmm5, %xmm6
    movaps    %xmm6, 0x40(%rdi)
    palignr    $6, %xmm4, %xmm5
    movaps    %xmm5, 0x30(%rdi)
    palignr    $6, %xmm3, %xmm4
    movaps    %xmm4, 0x20(%rdi)
    palignr    $6, %xmm2, %xmm3
    movaps    %xmm3, 0x10(%rdi)
    palignr    $6, %xmm1, %xmm2
    movaps    %xmm2, (%rdi)
    lea    0x80(%rdi), %rdi
    jae    L(shl_6)
    movdqu    %xmm0, (%r8)
    add    $0x80, %rdx
    add    %rdx, %rdi
    add    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_fwd), %rdx, 4)

    .p2align 4
L(shl_6_bwd):
    movaps    -0x06(%rsi), %xmm1

    movaps    -0x16(%rsi), %xmm2
    palignr    $6, %xmm2, %xmm1
    movaps    %xmm1, -0x10(%rdi)

    movaps    -0x26(%rsi), %xmm3
    palignr    $6, %xmm3, %xmm2
    movaps    %xmm2, -0x20(%rdi)

    movaps    -0x36(%rsi), %xmm4
    palignr    $6, %xmm4, %xmm3
    movaps    %xmm3, -0x30(%rdi)

    movaps    -0x46(%rsi), %xmm5
    palignr    $6, %xmm5, %xmm4
    movaps    %xmm4, -0x40(%rdi)

    movaps    -0x56(%rsi), %xmm6
    palignr    $6, %xmm6, %xmm5
    movaps    %xmm5, -0x50(%rdi)

    movaps    -0x66(%rsi), %xmm7
    palignr    $6, %xmm7, %xmm6
    movaps    %xmm6, -0x60(%rdi)

    movaps    -0x76(%rsi), %xmm8
    palignr    $6, %xmm8, %xmm7
    movaps    %xmm7, -0x70(%rdi)

    movaps    -0x86(%rsi), %xmm9
    palignr    $6, %xmm9, %xmm8
    movaps    %xmm8, -0x80(%rdi)

    sub    $0x80, %rdx
    lea    -0x80(%rdi), %rdi
    lea    -0x80(%rsi), %rsi
    jae    L(shl_6_bwd)
    movdqu    %xmm0, (%r8)
    add    $0x80, %rdx
    sub    %rdx, %rdi
    sub    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_bwd), %rdx, 4)

    .p2align 4
L(shl_7):
    sub    $0x80, %rdx
    movaps    -0x07(%rsi), %xmm1
    movaps    0x09(%rsi), %xmm2
    movaps    0x19(%rsi), %xmm3
    movaps    0x29(%rsi), %xmm4
    movaps    0x39(%rsi), %xmm5
    movaps    0x49(%rsi), %xmm6
    movaps    0x59(%rsi), %xmm7
    movaps    0x69(%rsi), %xmm8
    movaps    0x79(%rsi), %xmm9
    lea    0x80(%rsi), %rsi
    palignr    $7, %xmm8, %xmm9
    movaps    %xmm9, 0x70(%rdi)
    palignr    $7, %xmm7, %xmm8
    movaps    %xmm8, 0x60(%rdi)
    palignr    $7, %xmm6, %xmm7
    movaps    %xmm7, 0x50(%rdi)
    palignr    $7, %xmm5, %xmm6
    movaps    %xmm6, 0x40(%rdi)
    palignr    $7, %xmm4, %xmm5
    movaps    %xmm5, 0x30(%rdi)
    palignr    $7, %xmm3, %xmm4
    movaps    %xmm4, 0x20(%rdi)
    palignr    $7, %xmm2, %xmm3
    movaps    %xmm3, 0x10(%rdi)
    palignr    $7, %xmm1, %xmm2
    movaps    %xmm2, (%rdi)
    lea    0x80(%rdi), %rdi
    jae    L(shl_7)
    movdqu    %xmm0, (%r8)
    add    $0x80, %rdx
    add    %rdx, %rdi
    add    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_fwd), %rdx, 4)

    .p2align 4
L(shl_7_bwd):
    movaps    -0x07(%rsi), %xmm1

    movaps    -0x17(%rsi), %xmm2
    palignr    $7, %xmm2, %xmm1
    movaps    %xmm1, -0x10(%rdi)

    movaps    -0x27(%rsi), %xmm3
    palignr    $7, %xmm3, %xmm2
    movaps    %xmm2, -0x20(%rdi)

    movaps    -0x37(%rsi), %xmm4
    palignr    $7, %xmm4, %xmm3
    movaps    %xmm3, -0x30(%rdi)

    movaps    -0x47(%rsi), %xmm5
    palignr    $7, %xmm5, %xmm4
    movaps    %xmm4, -0x40(%rdi)

    movaps    -0x57(%rsi), %xmm6
    palignr    $7, %xmm6, %xmm5
    movaps    %xmm5, -0x50(%rdi)

    movaps    -0x67(%rsi), %xmm7
    palignr    $7, %xmm7, %xmm6
    movaps    %xmm6, -0x60(%rdi)

    movaps    -0x77(%rsi), %xmm8
    palignr    $7, %xmm8, %xmm7
    movaps    %xmm7, -0x70(%rdi)

    movaps    -0x87(%rsi), %xmm9
    palignr    $7, %xmm9, %xmm8
    movaps    %xmm8, -0x80(%rdi)

    sub    $0x80, %rdx
    lea    -0x80(%rdi), %rdi
    lea    -0x80(%rsi), %rsi
    jae    L(shl_7_bwd)
    movdqu    %xmm0, (%r8)
    add    $0x80, %rdx
    sub    %rdx, %rdi
    sub    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_bwd), %rdx, 4)

    .p2align 4
L(shl_8):
    sub    $0x80, %rdx
    movaps    -0x08(%rsi), %xmm1
    movaps    0x08(%rsi), %xmm2
    movaps    0x18(%rsi), %xmm3
    movaps    0x28(%rsi), %xmm4
    movaps    0x38(%rsi), %xmm5
    movaps    0x48(%rsi), %xmm6
    movaps    0x58(%rsi), %xmm7
    movaps    0x68(%rsi), %xmm8
    movaps    0x78(%rsi), %xmm9
    lea    0x80(%rsi), %rsi
    palignr    $8, %xmm8, %xmm9
    movaps    %xmm9, 0x70(%rdi)
    palignr    $8, %xmm7, %xmm8
    movaps    %xmm8, 0x60(%rdi)
    palignr    $8, %xmm6, %xmm7
    movaps    %xmm7, 0x50(%rdi)
    palignr    $8, %xmm5, %xmm6
    movaps    %xmm6, 0x40(%rdi)
    palignr    $8, %xmm4, %xmm5
    movaps    %xmm5, 0x30(%rdi)
    palignr    $8, %xmm3, %xmm4
    movaps    %xmm4, 0x20(%rdi)
    palignr    $8, %xmm2, %xmm3
    movaps    %xmm3, 0x10(%rdi)
    palignr    $8, %xmm1, %xmm2
    movaps    %xmm2, (%rdi)
    lea    0x80(%rdi), %rdi
    jae    L(shl_8)
    movdqu    %xmm0, (%r8)
    add    $0x80, %rdx
    add    %rdx, %rdi
    add    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_fwd), %rdx, 4)

    .p2align 4
L(shl_8_bwd):
    movaps    -0x08(%rsi), %xmm1

    movaps    -0x18(%rsi), %xmm2
    palignr    $8, %xmm2, %xmm1
    movaps    %xmm1, -0x10(%rdi)

    movaps    -0x28(%rsi), %xmm3
    palignr    $8, %xmm3, %xmm2
    movaps    %xmm2, -0x20(%rdi)

    movaps    -0x38(%rsi), %xmm4
    palignr    $8, %xmm4, %xmm3
    movaps    %xmm3, -0x30(%rdi)

    movaps    -0x48(%rsi), %xmm5
    palignr    $8, %xmm5, %xmm4
    movaps    %xmm4, -0x40(%rdi)

    movaps    -0x58(%rsi), %xmm6
    palignr    $8, %xmm6, %xmm5
    movaps    %xmm5, -0x50(%rdi)

    movaps    -0x68(%rsi), %xmm7
    palignr    $8, %xmm7, %xmm6
    movaps    %xmm6, -0x60(%rdi)

    movaps    -0x78(%rsi), %xmm8
    palignr    $8, %xmm8, %xmm7
    movaps    %xmm7, -0x70(%rdi)

    movaps    -0x88(%rsi), %xmm9
    palignr    $8, %xmm9, %xmm8
    movaps    %xmm8, -0x80(%rdi)

    sub    $0x80, %rdx
    lea    -0x80(%rdi), %rdi
    lea    -0x80(%rsi), %rsi
    jae    L(shl_8_bwd)
L(shl_8_end_bwd):
    movdqu    %xmm0, (%r8)
    add    $0x80, %rdx
    sub    %rdx, %rdi
    sub    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_bwd), %rdx, 4)

    .p2align 4
L(shl_9):
    sub    $0x80, %rdx
    movaps    -0x09(%rsi), %xmm1
    movaps    0x07(%rsi), %xmm2
    movaps    0x17(%rsi), %xmm3
    movaps    0x27(%rsi), %xmm4
    movaps    0x37(%rsi), %xmm5
    movaps    0x47(%rsi), %xmm6
    movaps    0x57(%rsi), %xmm7
    movaps    0x67(%rsi), %xmm8
    movaps    0x77(%rsi), %xmm9
    lea    0x80(%rsi), %rsi
    palignr    $9, %xmm8, %xmm9
    movaps    %xmm9, 0x70(%rdi)
    palignr    $9, %xmm7, %xmm8
    movaps    %xmm8, 0x60(%rdi)
    palignr    $9, %xmm6, %xmm7
    movaps    %xmm7, 0x50(%rdi)
    palignr    $9, %xmm5, %xmm6
    movaps    %xmm6, 0x40(%rdi)
    palignr    $9, %xmm4, %xmm5
    movaps    %xmm5, 0x30(%rdi)
    palignr    $9, %xmm3, %xmm4
    movaps    %xmm4, 0x20(%rdi)
    palignr    $9, %xmm2, %xmm3
    movaps    %xmm3, 0x10(%rdi)
    palignr    $9, %xmm1, %xmm2
    movaps    %xmm2, (%rdi)
    lea    0x80(%rdi), %rdi
    jae    L(shl_9)
    movdqu    %xmm0, (%r8)
    add    $0x80, %rdx
    add    %rdx, %rdi
    add    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_fwd), %rdx, 4)

    .p2align 4
L(shl_9_bwd):
    movaps    -0x09(%rsi), %xmm1

    movaps    -0x19(%rsi), %xmm2
    palignr    $9, %xmm2, %xmm1
    movaps    %xmm1, -0x10(%rdi)

    movaps    -0x29(%rsi), %xmm3
    palignr    $9, %xmm3, %xmm2
    movaps    %xmm2, -0x20(%rdi)

    movaps    -0x39(%rsi), %xmm4
    palignr    $9, %xmm4, %xmm3
    movaps    %xmm3, -0x30(%rdi)

    movaps    -0x49(%rsi), %xmm5
    palignr    $9, %xmm5, %xmm4
    movaps    %xmm4, -0x40(%rdi)

    movaps    -0x59(%rsi), %xmm6
    palignr    $9, %xmm6, %xmm5
    movaps    %xmm5, -0x50(%rdi)

    movaps    -0x69(%rsi), %xmm7
    palignr    $9, %xmm7, %xmm6
    movaps    %xmm6, -0x60(%rdi)

    movaps    -0x79(%rsi), %xmm8
    palignr    $9, %xmm8, %xmm7
    movaps    %xmm7, -0x70(%rdi)

    movaps    -0x89(%rsi), %xmm9
    palignr    $9, %xmm9, %xmm8
    movaps    %xmm8, -0x80(%rdi)

    sub    $0x80, %rdx
    lea    -0x80(%rdi), %rdi
    lea    -0x80(%rsi), %rsi
    jae    L(shl_9_bwd)
    movdqu    %xmm0, (%r8)
    add    $0x80, %rdx
    sub    %rdx, %rdi
    sub    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_bwd), %rdx, 4)

    .p2align 4
L(shl_10):
    sub    $0x80, %rdx
    movaps    -0x0a(%rsi), %xmm1
    movaps    0x06(%rsi), %xmm2
    movaps    0x16(%rsi), %xmm3
    movaps    0x26(%rsi), %xmm4
    movaps    0x36(%rsi), %xmm5
    movaps    0x46(%rsi), %xmm6
    movaps    0x56(%rsi), %xmm7
    movaps    0x66(%rsi), %xmm8
    movaps    0x76(%rsi), %xmm9
    lea    0x80(%rsi), %rsi
    palignr    $10, %xmm8, %xmm9
    movaps    %xmm9, 0x70(%rdi)
    palignr    $10, %xmm7, %xmm8
    movaps    %xmm8, 0x60(%rdi)
    palignr    $10, %xmm6, %xmm7
    movaps    %xmm7, 0x50(%rdi)
    palignr    $10, %xmm5, %xmm6
    movaps    %xmm6, 0x40(%rdi)
    palignr    $10, %xmm4, %xmm5
    movaps    %xmm5, 0x30(%rdi)
    palignr    $10, %xmm3, %xmm4
    movaps    %xmm4, 0x20(%rdi)
    palignr    $10, %xmm2, %xmm3
    movaps    %xmm3, 0x10(%rdi)
    palignr    $10, %xmm1, %xmm2
    movaps    %xmm2, (%rdi)
    lea    0x80(%rdi), %rdi
    jae    L(shl_10)
    movdqu    %xmm0, (%r8)
    add    $0x80, %rdx
    add    %rdx, %rdi
    add    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_fwd), %rdx, 4)

    .p2align 4
L(shl_10_bwd):
    movaps    -0x0a(%rsi), %xmm1

    movaps    -0x1a(%rsi), %xmm2
    palignr    $10, %xmm2, %xmm1
    movaps    %xmm1, -0x10(%rdi)

    movaps    -0x2a(%rsi), %xmm3
    palignr    $10, %xmm3, %xmm2
    movaps    %xmm2, -0x20(%rdi)

    movaps    -0x3a(%rsi), %xmm4
    palignr    $10, %xmm4, %xmm3
    movaps    %xmm3, -0x30(%rdi)

    movaps    -0x4a(%rsi), %xmm5
    palignr    $10, %xmm5, %xmm4
    movaps    %xmm4, -0x40(%rdi)

    movaps    -0x5a(%rsi), %xmm6
    palignr    $10, %xmm6, %xmm5
    movaps    %xmm5, -0x50(%rdi)

    movaps    -0x6a(%rsi), %xmm7
    palignr    $10, %xmm7, %xmm6
    movaps    %xmm6, -0x60(%rdi)

    movaps    -0x7a(%rsi), %xmm8
    palignr    $10, %xmm8, %xmm7
    movaps    %xmm7, -0x70(%rdi)

    movaps    -0x8a(%rsi), %xmm9
    palignr    $10, %xmm9, %xmm8
    movaps    %xmm8, -0x80(%rdi)

    sub    $0x80, %rdx
    lea    -0x80(%rdi), %rdi
    lea    -0x80(%rsi), %rsi
    jae    L(shl_10_bwd)
    movdqu    %xmm0, (%r8)
    add    $0x80, %rdx
    sub    %rdx, %rdi
    sub    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_bwd), %rdx, 4)

    .p2align 4
L(shl_11):
    sub    $0x80, %rdx
    movaps    -0x0b(%rsi), %xmm1
    movaps    0x05(%rsi), %xmm2
    movaps    0x15(%rsi), %xmm3
    movaps    0x25(%rsi), %xmm4
    movaps    0x35(%rsi), %xmm5
    movaps    0x45(%rsi), %xmm6
    movaps    0x55(%rsi), %xmm7
    movaps    0x65(%rsi), %xmm8
    movaps    0x75(%rsi), %xmm9
    lea    0x80(%rsi), %rsi
    palignr    $11, %xmm8, %xmm9
    movaps    %xmm9, 0x70(%rdi)
    palignr    $11, %xmm7, %xmm8
    movaps    %xmm8, 0x60(%rdi)
    palignr    $11, %xmm6, %xmm7
    movaps    %xmm7, 0x50(%rdi)
    palignr    $11, %xmm5, %xmm6
    movaps    %xmm6, 0x40(%rdi)
    palignr    $11, %xmm4, %xmm5
    movaps    %xmm5, 0x30(%rdi)
    palignr    $11, %xmm3, %xmm4
    movaps    %xmm4, 0x20(%rdi)
    palignr    $11, %xmm2, %xmm3
    movaps    %xmm3, 0x10(%rdi)
    palignr    $11, %xmm1, %xmm2
    movaps    %xmm2, (%rdi)
    lea    0x80(%rdi), %rdi
    jae    L(shl_11)
    movdqu    %xmm0, (%r8)
    add    $0x80, %rdx
    add    %rdx, %rdi
    add    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_fwd), %rdx, 4)

    .p2align 4
L(shl_11_bwd):
    movaps    -0x0b(%rsi), %xmm1

    movaps    -0x1b(%rsi), %xmm2
    palignr    $11, %xmm2, %xmm1
    movaps    %xmm1, -0x10(%rdi)

    movaps    -0x2b(%rsi), %xmm3
    palignr    $11, %xmm3, %xmm2
    movaps    %xmm2, -0x20(%rdi)

    movaps    -0x3b(%rsi), %xmm4
    palignr    $11, %xmm4, %xmm3
    movaps    %xmm3, -0x30(%rdi)

    movaps    -0x4b(%rsi), %xmm5
    palignr    $11, %xmm5, %xmm4
    movaps    %xmm4, -0x40(%rdi)

    movaps    -0x5b(%rsi), %xmm6
    palignr    $11, %xmm6, %xmm5
    movaps    %xmm5, -0x50(%rdi)

    movaps    -0x6b(%rsi), %xmm7
    palignr    $11, %xmm7, %xmm6
    movaps    %xmm6, -0x60(%rdi)

    movaps    -0x7b(%rsi), %xmm8
    palignr    $11, %xmm8, %xmm7
    movaps    %xmm7, -0x70(%rdi)

    movaps    -0x8b(%rsi), %xmm9
    palignr    $11, %xmm9, %xmm8
    movaps    %xmm8, -0x80(%rdi)

    sub    $0x80, %rdx
    lea    -0x80(%rdi), %rdi
    lea    -0x80(%rsi), %rsi
    jae    L(shl_11_bwd)
    movdqu    %xmm0, (%r8)
    add    $0x80, %rdx
    sub    %rdx, %rdi
    sub    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_bwd), %rdx, 4)

    .p2align 4
L(shl_12):
    sub    $0x80, %rdx
    movdqa    -0x0c(%rsi), %xmm1
    movaps    0x04(%rsi), %xmm2
    movaps    0x14(%rsi), %xmm3
    movaps    0x24(%rsi), %xmm4
    movaps    0x34(%rsi), %xmm5
    movaps    0x44(%rsi), %xmm6
    movaps    0x54(%rsi), %xmm7
    movaps    0x64(%rsi), %xmm8
    movaps    0x74(%rsi), %xmm9
    lea    0x80(%rsi), %rsi
    palignr    $12, %xmm8, %xmm9
    movaps    %xmm9, 0x70(%rdi)
    palignr    $12, %xmm7, %xmm8
    movaps    %xmm8, 0x60(%rdi)
    palignr    $12, %xmm6, %xmm7
    movaps    %xmm7, 0x50(%rdi)
    palignr    $12, %xmm5, %xmm6
    movaps    %xmm6, 0x40(%rdi)
    palignr    $12, %xmm4, %xmm5
    movaps    %xmm5, 0x30(%rdi)
    palignr    $12, %xmm3, %xmm4
    movaps    %xmm4, 0x20(%rdi)
    palignr    $12, %xmm2, %xmm3
    movaps    %xmm3, 0x10(%rdi)
    palignr    $12, %xmm1, %xmm2
    movaps    %xmm2, (%rdi)

    lea    0x80(%rdi), %rdi
    jae    L(shl_12)
    movdqu    %xmm0, (%r8)
    add    $0x80, %rdx
    add    %rdx, %rdi
    add    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_fwd), %rdx, 4)

    .p2align 4
L(shl_12_bwd):
    movaps    -0x0c(%rsi), %xmm1

    movaps    -0x1c(%rsi), %xmm2
    palignr    $12, %xmm2, %xmm1
    movaps    %xmm1, -0x10(%rdi)

    movaps    -0x2c(%rsi), %xmm3
    palignr    $12, %xmm3, %xmm2
    movaps    %xmm2, -0x20(%rdi)

    movaps    -0x3c(%rsi), %xmm4
    palignr    $12, %xmm4, %xmm3
    movaps    %xmm3, -0x30(%rdi)

    movaps    -0x4c(%rsi), %xmm5
    palignr    $12, %xmm5, %xmm4
    movaps    %xmm4, -0x40(%rdi)

    movaps    -0x5c(%rsi), %xmm6
    palignr    $12, %xmm6, %xmm5
    movaps    %xmm5, -0x50(%rdi)

    movaps    -0x6c(%rsi), %xmm7
    palignr    $12, %xmm7, %xmm6
    movaps    %xmm6, -0x60(%rdi)

    movaps    -0x7c(%rsi), %xmm8
    palignr    $12, %xmm8, %xmm7
    movaps    %xmm7, -0x70(%rdi)

    movaps    -0x8c(%rsi), %xmm9
    palignr    $12, %xmm9, %xmm8
    movaps    %xmm8, -0x80(%rdi)

    sub    $0x80, %rdx
    lea    -0x80(%rdi), %rdi
    lea    -0x80(%rsi), %rsi
    jae    L(shl_12_bwd)
    movdqu    %xmm0, (%r8)
    add    $0x80, %rdx
    sub    %rdx, %rdi
    sub    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_bwd), %rdx, 4)

    .p2align 4
L(shl_13):
    sub    $0x80, %rdx
    movaps    -0x0d(%rsi), %xmm1
    movaps    0x03(%rsi), %xmm2
    movaps    0x13(%rsi), %xmm3
    movaps    0x23(%rsi), %xmm4
    movaps    0x33(%rsi), %xmm5
    movaps    0x43(%rsi), %xmm6
    movaps    0x53(%rsi), %xmm7
    movaps    0x63(%rsi), %xmm8
    movaps    0x73(%rsi), %xmm9
    lea    0x80(%rsi), %rsi
    palignr    $13, %xmm8, %xmm9
    movaps    %xmm9, 0x70(%rdi)
    palignr    $13, %xmm7, %xmm8
    movaps    %xmm8, 0x60(%rdi)
    palignr    $13, %xmm6, %xmm7
    movaps    %xmm7, 0x50(%rdi)
    palignr    $13, %xmm5, %xmm6
    movaps    %xmm6, 0x40(%rdi)
    palignr    $13, %xmm4, %xmm5
    movaps    %xmm5, 0x30(%rdi)
    palignr    $13, %xmm3, %xmm4
    movaps    %xmm4, 0x20(%rdi)
    palignr    $13, %xmm2, %xmm3
    movaps    %xmm3, 0x10(%rdi)
    palignr    $13, %xmm1, %xmm2
    movaps    %xmm2, (%rdi)
    lea    0x80(%rdi), %rdi
    jae    L(shl_13)
    movdqu    %xmm0, (%r8)
    add    $0x80, %rdx
    add    %rdx, %rdi
    add    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_fwd), %rdx, 4)

    .p2align 4
L(shl_13_bwd):
    movaps    -0x0d(%rsi), %xmm1

    movaps    -0x1d(%rsi), %xmm2
    palignr    $13, %xmm2, %xmm1
    movaps    %xmm1, -0x10(%rdi)

    movaps    -0x2d(%rsi), %xmm3
    palignr    $13, %xmm3, %xmm2
    movaps    %xmm2, -0x20(%rdi)

    movaps    -0x3d(%rsi), %xmm4
    palignr    $13, %xmm4, %xmm3
    movaps    %xmm3, -0x30(%rdi)

    movaps    -0x4d(%rsi), %xmm5
    palignr    $13, %xmm5, %xmm4
    movaps    %xmm4, -0x40(%rdi)

    movaps    -0x5d(%rsi), %xmm6
    palignr    $13, %xmm6, %xmm5
    movaps    %xmm5, -0x50(%rdi)

    movaps    -0x6d(%rsi), %xmm7
    palignr    $13, %xmm7, %xmm6
    movaps    %xmm6, -0x60(%rdi)

    movaps    -0x7d(%rsi), %xmm8
    palignr    $13, %xmm8, %xmm7
    movaps    %xmm7, -0x70(%rdi)

    movaps    -0x8d(%rsi), %xmm9
    palignr    $13, %xmm9, %xmm8
    movaps    %xmm8, -0x80(%rdi)

    sub    $0x80, %rdx
    lea    -0x80(%rdi), %rdi
    lea    -0x80(%rsi), %rsi
    jae    L(shl_13_bwd)
    movdqu    %xmm0, (%r8)
    add    $0x80, %rdx
    sub    %rdx, %rdi
    sub    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_bwd), %rdx, 4)

    .p2align 4
L(shl_14):
    sub    $0x80, %rdx
    movaps    -0x0e(%rsi), %xmm1
    movaps    0x02(%rsi), %xmm2
    movaps    0x12(%rsi), %xmm3
    movaps    0x22(%rsi), %xmm4
    movaps    0x32(%rsi), %xmm5
    movaps    0x42(%rsi), %xmm6
    movaps    0x52(%rsi), %xmm7
    movaps    0x62(%rsi), %xmm8
    movaps    0x72(%rsi), %xmm9
    lea    0x80(%rsi), %rsi
    palignr    $14, %xmm8, %xmm9
    movaps    %xmm9, 0x70(%rdi)
    palignr    $14, %xmm7, %xmm8
    movaps    %xmm8, 0x60(%rdi)
    palignr    $14, %xmm6, %xmm7
    movaps    %xmm7, 0x50(%rdi)
    palignr    $14, %xmm5, %xmm6
    movaps    %xmm6, 0x40(%rdi)
    palignr    $14, %xmm4, %xmm5
    movaps    %xmm5, 0x30(%rdi)
    palignr    $14, %xmm3, %xmm4
    movaps    %xmm4, 0x20(%rdi)
    palignr    $14, %xmm2, %xmm3
    movaps    %xmm3, 0x10(%rdi)
    palignr    $14, %xmm1, %xmm2
    movaps    %xmm2, (%rdi)
    lea    0x80(%rdi), %rdi
    jae    L(shl_14)
    movdqu    %xmm0, (%r8)
    add    $0x80, %rdx
    add    %rdx, %rdi
    add    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_fwd), %rdx, 4)

    .p2align 4
L(shl_14_bwd):
    movaps    -0x0e(%rsi), %xmm1

    movaps    -0x1e(%rsi), %xmm2
    palignr    $14, %xmm2, %xmm1
    movaps    %xmm1, -0x10(%rdi)

    movaps    -0x2e(%rsi), %xmm3
    palignr    $14, %xmm3, %xmm2
    movaps    %xmm2, -0x20(%rdi)

    movaps    -0x3e(%rsi), %xmm4
    palignr    $14, %xmm4, %xmm3
    movaps    %xmm3, -0x30(%rdi)

    movaps    -0x4e(%rsi), %xmm5
    palignr    $14, %xmm5, %xmm4
    movaps    %xmm4, -0x40(%rdi)

    movaps    -0x5e(%rsi), %xmm6
    palignr    $14, %xmm6, %xmm5
    movaps    %xmm5, -0x50(%rdi)

    movaps    -0x6e(%rsi), %xmm7
    palignr    $14, %xmm7, %xmm6
    movaps    %xmm6, -0x60(%rdi)

    movaps    -0x7e(%rsi), %xmm8
    palignr    $14, %xmm8, %xmm7
    movaps    %xmm7, -0x70(%rdi)

    movaps    -0x8e(%rsi), %xmm9
    palignr    $14, %xmm9, %xmm8
    movaps    %xmm8, -0x80(%rdi)

    sub    $0x80, %rdx
    lea    -0x80(%rdi), %rdi
    lea    -0x80(%rsi), %rsi
    jae    L(shl_14_bwd)
    movdqu    %xmm0, (%r8)
    add    $0x80, %rdx
    sub    %rdx, %rdi
    sub    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_bwd), %rdx, 4)

    .p2align 4
L(shl_15):
    sub    $0x80, %rdx
    movaps    -0x0f(%rsi), %xmm1
    movaps    0x01(%rsi), %xmm2
    movaps    0x11(%rsi), %xmm3
    movaps    0x21(%rsi), %xmm4
    movaps    0x31(%rsi), %xmm5
    movaps    0x41(%rsi), %xmm6
    movaps    0x51(%rsi), %xmm7
    movaps    0x61(%rsi), %xmm8
    movaps    0x71(%rsi), %xmm9
    lea    0x80(%rsi), %rsi
    palignr    $15, %xmm8, %xmm9
    movaps    %xmm9, 0x70(%rdi)
    palignr    $15, %xmm7, %xmm8
    movaps    %xmm8, 0x60(%rdi)
    palignr    $15, %xmm6, %xmm7
    movaps    %xmm7, 0x50(%rdi)
    palignr    $15, %xmm5, %xmm6
    movaps    %xmm6, 0x40(%rdi)
    palignr    $15, %xmm4, %xmm5
    movaps    %xmm5, 0x30(%rdi)
    palignr    $15, %xmm3, %xmm4
    movaps    %xmm4, 0x20(%rdi)
    palignr    $15, %xmm2, %xmm3
    movaps    %xmm3, 0x10(%rdi)
    palignr    $15, %xmm1, %xmm2
    movaps    %xmm2, (%rdi)
    lea    0x80(%rdi), %rdi
    jae    L(shl_15)
    movdqu    %xmm0, (%r8)
    add    $0x80, %rdx
    add    %rdx, %rdi
    add    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_fwd), %rdx, 4)

    .p2align 4
L(shl_15_bwd):
    movaps    -0x0f(%rsi), %xmm1

    movaps    -0x1f(%rsi), %xmm2
    palignr    $15, %xmm2, %xmm1
    movaps    %xmm1, -0x10(%rdi)

    movaps    -0x2f(%rsi), %xmm3
    palignr    $15, %xmm3, %xmm2
    movaps    %xmm2, -0x20(%rdi)

    movaps    -0x3f(%rsi), %xmm4
    palignr    $15, %xmm4, %xmm3
    movaps    %xmm3, -0x30(%rdi)

    movaps    -0x4f(%rsi), %xmm5
    palignr    $15, %xmm5, %xmm4
    movaps    %xmm4, -0x40(%rdi)

    movaps    -0x5f(%rsi), %xmm6
    palignr    $15, %xmm6, %xmm5
    movaps    %xmm5, -0x50(%rdi)

    movaps    -0x6f(%rsi), %xmm7
    palignr    $15, %xmm7, %xmm6
    movaps    %xmm6, -0x60(%rdi)

    movaps    -0x7f(%rsi), %xmm8
    palignr    $15, %xmm8, %xmm7
    movaps    %xmm7, -0x70(%rdi)

    movaps    -0x8f(%rsi), %xmm9
    palignr    $15, %xmm9, %xmm8
    movaps    %xmm8, -0x80(%rdi)

    sub    $0x80, %rdx
    lea    -0x80(%rdi), %rdi
    lea    -0x80(%rsi), %rsi
    jae    L(shl_15_bwd)
    movdqu    %xmm0, (%r8)
    add    $0x80, %rdx
    sub    %rdx, %rdi
    sub    %rdx, %rsi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_bwd), %rdx, 4)

    .p2align 4
L(gobble_mem_fwd):
    movdqu    (%rsi), %xmm1
    movdqu    %xmm0, (%r8)
    movdqa    %xmm1, (%rdi)
    sub    $16, %rdx
    add    $16, %rsi
    add    $16, %rdi

#ifdef SHARED_CACHE_SIZE_HALF
    mov    $SHARED_CACHE_SIZE_HALF, %RCX_LP
#else
    mov    __x86_shared_cache_size_half(%rip), %RCX_LP
#endif
#ifdef USE_AS_MEMMOVE
    mov    %rsi, %r9
    sub    %rdi, %r9
    cmp    %rdx, %r9
    jae    L(memmove_is_memcpy_fwd)
    cmp    %rcx, %r9
    jbe    L(ll_cache_copy_fwd_start)
L(memmove_is_memcpy_fwd):
#endif
    cmp    %rcx, %rdx
    ja    L(bigger_in_fwd)
    mov    %rdx, %rcx
L(bigger_in_fwd):
    sub    %rcx, %rdx
    cmp    $0x1000, %rdx
    jbe    L(ll_cache_copy_fwd)

    mov    %rcx, %r9
    shl    $3, %r9
    cmp    %r9, %rdx
    jbe    L(2steps_copy_fwd)
    add    %rcx, %rdx
    xor    %rcx, %rcx
L(2steps_copy_fwd):
    sub    $0x80, %rdx
L(gobble_mem_fwd_loop):
    sub    $0x80, %rdx
    prefetcht0 0x200(%rsi)
    prefetcht0 0x300(%rsi)
    movdqu    (%rsi), %xmm0
    movdqu    0x10(%rsi), %xmm1
    movdqu    0x20(%rsi), %xmm2
    movdqu    0x30(%rsi), %xmm3
    movdqu    0x40(%rsi), %xmm4
    movdqu    0x50(%rsi), %xmm5
    movdqu    0x60(%rsi), %xmm6
    movdqu    0x70(%rsi), %xmm7
    lfence
    movntdq    %xmm0, (%rdi)
    movntdq    %xmm1, 0x10(%rdi)
    movntdq    %xmm2, 0x20(%rdi)
    movntdq    %xmm3, 0x30(%rdi)
    movntdq    %xmm4, 0x40(%rdi)
    movntdq    %xmm5, 0x50(%rdi)
    movntdq    %xmm6, 0x60(%rdi)
    movntdq    %xmm7, 0x70(%rdi)
    lea    0x80(%rsi), %rsi
    lea    0x80(%rdi), %rdi
    jae    L(gobble_mem_fwd_loop)
    sfence
    cmp    $0x80, %rcx
    jb    L(gobble_mem_fwd_end)
    add    $0x80, %rdx
L(ll_cache_copy_fwd):
    add    %rcx, %rdx
L(ll_cache_copy_fwd_start):
    sub    $0x80, %rdx
L(gobble_ll_loop_fwd):
    prefetchnta 0x1c0(%rsi)
    prefetchnta 0x280(%rsi)
    prefetchnta 0x1c0(%rdi)
    prefetchnta 0x280(%rdi)
    sub    $0x80, %rdx
    movdqu    (%rsi), %xmm0
    movdqu    0x10(%rsi), %xmm1
    movdqu    0x20(%rsi), %xmm2
    movdqu    0x30(%rsi), %xmm3
    movdqu    0x40(%rsi), %xmm4
    movdqu    0x50(%rsi), %xmm5
    movdqu    0x60(%rsi), %xmm6
    movdqu    0x70(%rsi), %xmm7
    movdqa    %xmm0, (%rdi)
    movdqa    %xmm1, 0x10(%rdi)
    movdqa    %xmm2, 0x20(%rdi)
    movdqa    %xmm3, 0x30(%rdi)
    movdqa    %xmm4, 0x40(%rdi)
    movdqa    %xmm5, 0x50(%rdi)
    movdqa    %xmm6, 0x60(%rdi)
    movdqa    %xmm7, 0x70(%rdi)
    lea    0x80(%rsi), %rsi
    lea    0x80(%rdi), %rdi
    jae    L(gobble_ll_loop_fwd)
L(gobble_mem_fwd_end):
    add    $0x80, %rdx
    add    %rdx, %rsi
    add    %rdx, %rdi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_fwd), %rdx, 4)

    .p2align 4
L(gobble_mem_bwd):
    add    %rdx, %rsi
    add    %rdx, %rdi

    movdqu    -16(%rsi), %xmm0
    lea    -16(%rdi), %r8
    mov    %rdi, %r9
    and    $-16, %rdi
    sub    %rdi, %r9
    sub    %r9, %rsi
    sub    %r9, %rdx


#ifdef SHARED_CACHE_SIZE_HALF
    mov    $SHARED_CACHE_SIZE_HALF, %RCX_LP
#else
    mov    __x86_shared_cache_size_half(%rip), %RCX_LP
#endif
#ifdef USE_AS_MEMMOVE
    mov    %rdi, %r9
    sub    %rsi, %r9
    cmp    %rdx, %r9
    jae    L(memmove_is_memcpy_bwd)
    cmp    %rcx, %r9
    jbe    L(ll_cache_copy_bwd_start)
L(memmove_is_memcpy_bwd):
#endif
    cmp    %rcx, %rdx
    ja    L(bigger)
    mov    %rdx, %rcx
L(bigger):
    sub    %rcx, %rdx
    cmp    $0x1000, %rdx
    jbe    L(ll_cache_copy)

    mov    %rcx, %r9
    shl    $3, %r9
    cmp    %r9, %rdx
    jbe    L(2steps_copy)
    add    %rcx, %rdx
    xor    %rcx, %rcx
L(2steps_copy):
    sub    $0x80, %rdx
L(gobble_mem_bwd_loop):
    sub    $0x80, %rdx
    prefetcht0 -0x200(%rsi)
    prefetcht0 -0x300(%rsi)
    movdqu    -0x10(%rsi), %xmm1
    movdqu    -0x20(%rsi), %xmm2
    movdqu    -0x30(%rsi), %xmm3
    movdqu    -0x40(%rsi), %xmm4
    movdqu    -0x50(%rsi), %xmm5
    movdqu    -0x60(%rsi), %xmm6
    movdqu    -0x70(%rsi), %xmm7
    movdqu    -0x80(%rsi), %xmm8
    lfence
    movntdq    %xmm1, -0x10(%rdi)
    movntdq    %xmm2, -0x20(%rdi)
    movntdq    %xmm3, -0x30(%rdi)
    movntdq    %xmm4, -0x40(%rdi)
    movntdq    %xmm5, -0x50(%rdi)
    movntdq    %xmm6, -0x60(%rdi)
    movntdq    %xmm7, -0x70(%rdi)
    movntdq    %xmm8, -0x80(%rdi)
    lea    -0x80(%rsi), %rsi
    lea    -0x80(%rdi), %rdi
    jae    L(gobble_mem_bwd_loop)
    sfence
    cmp    $0x80, %rcx
    jb    L(gobble_mem_bwd_end)
    add    $0x80, %rdx
L(ll_cache_copy):
    add    %rcx, %rdx
L(ll_cache_copy_bwd_start):
    sub    $0x80, %rdx
L(gobble_ll_loop):
    prefetchnta -0x1c0(%rsi)
    prefetchnta -0x280(%rsi)
    prefetchnta -0x1c0(%rdi)
    prefetchnta -0x280(%rdi)
    sub    $0x80, %rdx
    movdqu    -0x10(%rsi), %xmm1
    movdqu    -0x20(%rsi), %xmm2
    movdqu    -0x30(%rsi), %xmm3
    movdqu    -0x40(%rsi), %xmm4
    movdqu    -0x50(%rsi), %xmm5
    movdqu    -0x60(%rsi), %xmm6
    movdqu    -0x70(%rsi), %xmm7
    movdqu    -0x80(%rsi), %xmm8
    movdqa    %xmm1, -0x10(%rdi)
    movdqa    %xmm2, -0x20(%rdi)
    movdqa    %xmm3, -0x30(%rdi)
    movdqa    %xmm4, -0x40(%rdi)
    movdqa    %xmm5, -0x50(%rdi)
    movdqa    %xmm6, -0x60(%rdi)
    movdqa    %xmm7, -0x70(%rdi)
    movdqa    %xmm8, -0x80(%rdi)
    lea    -0x80(%rsi), %rsi
    lea    -0x80(%rdi), %rdi
    jae    L(gobble_ll_loop)
L(gobble_mem_bwd_end):
    movdqu    %xmm0, (%r8)
    add    $0x80, %rdx
    sub    %rdx, %rsi
    sub    %rdx, %rdi
    BRANCH_TO_JMPTBL_ENTRY (L(table_144_bytes_bwd), %rdx, 4)

    .p2align 4
L(fwd_write_128bytes):
    lddqu    -128(%rsi), %xmm0
    movdqu    %xmm0, -128(%rdi)
L(fwd_write_112bytes):
    lddqu    -112(%rsi), %xmm0
    movdqu    %xmm0, -112(%rdi)
L(fwd_write_96bytes):
    lddqu    -96(%rsi), %xmm0
    movdqu    %xmm0, -96(%rdi)
L(fwd_write_80bytes):
    lddqu    -80(%rsi), %xmm0
    movdqu    %xmm0, -80(%rdi)
L(fwd_write_64bytes):
    lddqu    -64(%rsi), %xmm0
    movdqu    %xmm0, -64(%rdi)
L(fwd_write_48bytes):
    lddqu    -48(%rsi), %xmm0
    movdqu    %xmm0, -48(%rdi)
L(fwd_write_32bytes):
    lddqu    -32(%rsi), %xmm0
    movdqu    %xmm0, -32(%rdi)
L(fwd_write_16bytes):
    lddqu    -16(%rsi), %xmm0
    movdqu    %xmm0, -16(%rdi)
L(fwd_write_0bytes):
    ret


    .p2align 4
L(fwd_write_143bytes):
    lddqu    -143(%rsi), %xmm0
    movdqu    %xmm0, -143(%rdi)
L(fwd_write_127bytes):
    lddqu    -127(%rsi), %xmm0
    movdqu    %xmm0, -127(%rdi)
L(fwd_write_111bytes):
    lddqu    -111(%rsi), %xmm0
    movdqu    %xmm0, -111(%rdi)
L(fwd_write_95bytes):
    lddqu    -95(%rsi), %xmm0
    movdqu    %xmm0, -95(%rdi)
L(fwd_write_79bytes):
    lddqu    -79(%rsi), %xmm0
    movdqu    %xmm0, -79(%rdi)
L(fwd_write_63bytes):
    lddqu    -63(%rsi), %xmm0
    movdqu    %xmm0, -63(%rdi)
L(fwd_write_47bytes):
    lddqu    -47(%rsi), %xmm0
    movdqu    %xmm0, -47(%rdi)
L(fwd_write_31bytes):
    lddqu    -31(%rsi), %xmm0
    lddqu    -16(%rsi), %xmm1
    movdqu    %xmm0, -31(%rdi)
    movdqu    %xmm1, -16(%rdi)
    ret

    .p2align 4
L(fwd_write_15bytes):
    mov    -15(%rsi), %rdx
    mov    -8(%rsi), %rcx
    mov    %rdx, -15(%rdi)
    mov    %rcx, -8(%rdi)
    ret

    .p2align 4
L(fwd_write_142bytes):
    lddqu    -142(%rsi), %xmm0
    movdqu    %xmm0, -142(%rdi)
L(fwd_write_126bytes):
    lddqu    -126(%rsi), %xmm0
    movdqu    %xmm0, -126(%rdi)
L(fwd_write_110bytes):
    lddqu    -110(%rsi), %xmm0
    movdqu    %xmm0, -110(%rdi)
L(fwd_write_94bytes):
    lddqu    -94(%rsi), %xmm0
    movdqu    %xmm0, -94(%rdi)
L(fwd_write_78bytes):
    lddqu    -78(%rsi), %xmm0
    movdqu    %xmm0, -78(%rdi)
L(fwd_write_62bytes):
    lddqu    -62(%rsi), %xmm0
    movdqu    %xmm0, -62(%rdi)
L(fwd_write_46bytes):
    lddqu    -46(%rsi), %xmm0
    movdqu    %xmm0, -46(%rdi)
L(fwd_write_30bytes):
    lddqu    -30(%rsi), %xmm0
    lddqu    -16(%rsi), %xmm1
    movdqu    %xmm0, -30(%rdi)
    movdqu    %xmm1, -16(%rdi)
    ret

    .p2align 4
L(fwd_write_14bytes):
    mov    -14(%rsi), %rdx
    mov    -8(%rsi), %rcx
    mov    %rdx, -14(%rdi)
    mov    %rcx, -8(%rdi)
    ret

    .p2align 4
L(fwd_write_141bytes):
    lddqu    -141(%rsi), %xmm0
    movdqu    %xmm0, -141(%rdi)
L(fwd_write_125bytes):
    lddqu    -125(%rsi), %xmm0
    movdqu    %xmm0, -125(%rdi)
L(fwd_write_109bytes):
    lddqu    -109(%rsi), %xmm0
    movdqu    %xmm0, -109(%rdi)
L(fwd_write_93bytes):
    lddqu    -93(%rsi), %xmm0
    movdqu    %xmm0, -93(%rdi)
L(fwd_write_77bytes):
    lddqu    -77(%rsi), %xmm0
    movdqu    %xmm0, -77(%rdi)
L(fwd_write_61bytes):
    lddqu    -61(%rsi), %xmm0
    movdqu    %xmm0, -61(%rdi)
L(fwd_write_45bytes):
    lddqu    -45(%rsi), %xmm0
    movdqu    %xmm0, -45(%rdi)
L(fwd_write_29bytes):
    lddqu    -29(%rsi), %xmm0
    lddqu    -16(%rsi), %xmm1
    movdqu    %xmm0, -29(%rdi)
    movdqu    %xmm1, -16(%rdi)
    ret

    .p2align 4
L(fwd_write_13bytes):
    mov    -13(%rsi), %rdx
    mov    -8(%rsi), %rcx
    mov    %rdx, -13(%rdi)
    mov    %rcx, -8(%rdi)
    ret

    .p2align 4
L(fwd_write_140bytes):
    lddqu    -140(%rsi), %xmm0
    movdqu    %xmm0, -140(%rdi)
L(fwd_write_124bytes):
    lddqu    -124(%rsi), %xmm0
    movdqu    %xmm0, -124(%rdi)
L(fwd_write_108bytes):
    lddqu    -108(%rsi), %xmm0
    movdqu    %xmm0, -108(%rdi)
L(fwd_write_92bytes):
    lddqu    -92(%rsi), %xmm0
    movdqu    %xmm0, -92(%rdi)
L(fwd_write_76bytes):
    lddqu    -76(%rsi), %xmm0
    movdqu    %xmm0, -76(%rdi)
L(fwd_write_60bytes):
    lddqu    -60(%rsi), %xmm0
    movdqu    %xmm0, -60(%rdi)
L(fwd_write_44bytes):
    lddqu    -44(%rsi), %xmm0
    movdqu    %xmm0, -44(%rdi)
L(fwd_write_28bytes):
    lddqu    -28(%rsi), %xmm0
    lddqu    -16(%rsi), %xmm1
    movdqu    %xmm0, -28(%rdi)
    movdqu    %xmm1, -16(%rdi)
    ret

    .p2align 4
L(fwd_write_12bytes):
    mov    -12(%rsi), %rdx
    mov    -4(%rsi), %ecx
    mov    %rdx, -12(%rdi)
    mov    %ecx, -4(%rdi)
    ret

    .p2align 4
L(fwd_write_139bytes):
    lddqu    -139(%rsi), %xmm0
    movdqu    %xmm0, -139(%rdi)
L(fwd_write_123bytes):
    lddqu    -123(%rsi), %xmm0
    movdqu    %xmm0, -123(%rdi)
L(fwd_write_107bytes):
    lddqu    -107(%rsi), %xmm0
    movdqu    %xmm0, -107(%rdi)
L(fwd_write_91bytes):
    lddqu    -91(%rsi), %xmm0
    movdqu    %xmm0, -91(%rdi)
L(fwd_write_75bytes):
    lddqu    -75(%rsi), %xmm0
    movdqu    %xmm0, -75(%rdi)
L(fwd_write_59bytes):
    lddqu    -59(%rsi), %xmm0
    movdqu    %xmm0, -59(%rdi)
L(fwd_write_43bytes):
    lddqu    -43(%rsi), %xmm0
    movdqu    %xmm0, -43(%rdi)
L(fwd_write_27bytes):
    lddqu    -27(%rsi), %xmm0
    lddqu    -16(%rsi), %xmm1
    movdqu    %xmm0, -27(%rdi)
    movdqu    %xmm1, -16(%rdi)
    ret

    .p2align 4
L(fwd_write_11bytes):
    mov    -11(%rsi), %rdx
    mov    -4(%rsi), %ecx
    mov    %rdx, -11(%rdi)
    mov    %ecx, -4(%rdi)
    ret

    .p2align 4
L(fwd_write_138bytes):
    lddqu    -138(%rsi), %xmm0
    movdqu    %xmm0, -138(%rdi)
L(fwd_write_122bytes):
    lddqu    -122(%rsi), %xmm0
    movdqu    %xmm0, -122(%rdi)
L(fwd_write_106bytes):
    lddqu    -106(%rsi), %xmm0
    movdqu    %xmm0, -106(%rdi)
L(fwd_write_90bytes):
    lddqu    -90(%rsi), %xmm0
    movdqu    %xmm0, -90(%rdi)
L(fwd_write_74bytes):
    lddqu    -74(%rsi), %xmm0
    movdqu    %xmm0, -74(%rdi)
L(fwd_write_58bytes):
    lddqu    -58(%rsi), %xmm0
    movdqu    %xmm0, -58(%rdi)
L(fwd_write_42bytes):
    lddqu    -42(%rsi), %xmm0
    movdqu    %xmm0, -42(%rdi)
L(fwd_write_26bytes):
    lddqu    -26(%rsi), %xmm0
    lddqu    -16(%rsi), %xmm1
    movdqu    %xmm0, -26(%rdi)
    movdqu    %xmm1, -16(%rdi)
    ret

    .p2align 4
L(fwd_write_10bytes):
    mov    -10(%rsi), %rdx
    mov    -4(%rsi), %ecx
    mov    %rdx, -10(%rdi)
    mov    %ecx, -4(%rdi)
    ret

    .p2align 4
L(fwd_write_137bytes):
    lddqu    -137(%rsi), %xmm0
    movdqu    %xmm0, -137(%rdi)
L(fwd_write_121bytes):
    lddqu    -121(%rsi), %xmm0
    movdqu    %xmm0, -121(%rdi)
L(fwd_write_105bytes):
    lddqu    -105(%rsi), %xmm0
    movdqu    %xmm0, -105(%rdi)
L(fwd_write_89bytes):
    lddqu    -89(%rsi), %xmm0
    movdqu    %xmm0, -89(%rdi)
L(fwd_write_73bytes):
    lddqu    -73(%rsi), %xmm0
    movdqu    %xmm0, -73(%rdi)
L(fwd_write_57bytes):
    lddqu    -57(%rsi), %xmm0
    movdqu    %xmm0, -57(%rdi)
L(fwd_write_41bytes):
    lddqu    -41(%rsi), %xmm0
    movdqu    %xmm0, -41(%rdi)
L(fwd_write_25bytes):
    lddqu    -25(%rsi), %xmm0
    lddqu    -16(%rsi), %xmm1
    movdqu    %xmm0, -25(%rdi)
    movdqu    %xmm1, -16(%rdi)
    ret

    .p2align 4
L(fwd_write_9bytes):
    mov    -9(%rsi), %rdx
    mov    -4(%rsi), %ecx
    mov    %rdx, -9(%rdi)
    mov    %ecx, -4(%rdi)
    ret

    .p2align 4
L(fwd_write_136bytes):
    lddqu    -136(%rsi), %xmm0
    movdqu    %xmm0, -136(%rdi)
L(fwd_write_120bytes):
    lddqu    -120(%rsi), %xmm0
    movdqu    %xmm0, -120(%rdi)
L(fwd_write_104bytes):
    lddqu    -104(%rsi), %xmm0
    movdqu    %xmm0, -104(%rdi)
L(fwd_write_88bytes):
    lddqu    -88(%rsi), %xmm0
    movdqu    %xmm0, -88(%rdi)
L(fwd_write_72bytes):
    lddqu    -72(%rsi), %xmm0
    movdqu    %xmm0, -72(%rdi)
L(fwd_write_56bytes):
    lddqu    -56(%rsi), %xmm0
    movdqu    %xmm0, -56(%rdi)
L(fwd_write_40bytes):
    lddqu    -40(%rsi), %xmm0
    movdqu    %xmm0, -40(%rdi)
L(fwd_write_24bytes):
    lddqu    -24(%rsi), %xmm0
    lddqu    -16(%rsi), %xmm1
    movdqu    %xmm0, -24(%rdi)
    movdqu    %xmm1, -16(%rdi)
    ret

    .p2align 4
L(fwd_write_8bytes):
    mov    -8(%rsi), %rdx
    mov    %rdx, -8(%rdi)
    ret

    .p2align 4
L(fwd_write_135bytes):
    lddqu    -135(%rsi), %xmm0
    movdqu    %xmm0, -135(%rdi)
L(fwd_write_119bytes):
    lddqu    -119(%rsi), %xmm0
    movdqu    %xmm0, -119(%rdi)
L(fwd_write_103bytes):
    lddqu    -103(%rsi), %xmm0
    movdqu    %xmm0, -103(%rdi)
L(fwd_write_87bytes):
    lddqu    -87(%rsi), %xmm0
    movdqu    %xmm0, -87(%rdi)
L(fwd_write_71bytes):
    lddqu    -71(%rsi), %xmm0
    movdqu    %xmm0, -71(%rdi)
L(fwd_write_55bytes):
    lddqu    -55(%rsi), %xmm0
    movdqu    %xmm0, -55(%rdi)
L(fwd_write_39bytes):
    lddqu    -39(%rsi), %xmm0
    movdqu    %xmm0, -39(%rdi)
L(fwd_write_23bytes):
    lddqu    -23(%rsi), %xmm0
    lddqu    -16(%rsi), %xmm1
    movdqu    %xmm0, -23(%rdi)
    movdqu    %xmm1, -16(%rdi)
    ret

    .p2align 4
L(fwd_write_7bytes):
    mov    -7(%rsi), %edx
    mov    -4(%rsi), %ecx
    mov    %edx, -7(%rdi)
    mov    %ecx, -4(%rdi)
    ret

    .p2align 4
L(fwd_write_134bytes):
    lddqu    -134(%rsi), %xmm0
    movdqu    %xmm0, -134(%rdi)
L(fwd_write_118bytes):
    lddqu    -118(%rsi), %xmm0
    movdqu    %xmm0, -118(%rdi)
L(fwd_write_102bytes):
    lddqu    -102(%rsi), %xmm0
    movdqu    %xmm0, -102(%rdi)
L(fwd_write_86bytes):
    lddqu    -86(%rsi), %xmm0
    movdqu    %xmm0, -86(%rdi)
L(fwd_write_70bytes):
    lddqu    -70(%rsi), %xmm0
    movdqu    %xmm0, -70(%rdi)
L(fwd_write_54bytes):
    lddqu    -54(%rsi), %xmm0
    movdqu    %xmm0, -54(%rdi)
L(fwd_write_38bytes):
    lddqu    -38(%rsi), %xmm0
    movdqu    %xmm0, -38(%rdi)
L(fwd_write_22bytes):
    lddqu    -22(%rsi), %xmm0
    lddqu    -16(%rsi), %xmm1
    movdqu    %xmm0, -22(%rdi)
    movdqu    %xmm1, -16(%rdi)
    ret

    .p2align 4
L(fwd_write_6bytes):
    mov    -6(%rsi), %edx
    mov    -4(%rsi), %ecx
    mov    %edx, -6(%rdi)
    mov    %ecx, -4(%rdi)
    ret

    .p2align 4
L(fwd_write_133bytes):
    lddqu    -133(%rsi), %xmm0
    movdqu    %xmm0, -133(%rdi)
L(fwd_write_117bytes):
    lddqu    -117(%rsi), %xmm0
    movdqu    %xmm0, -117(%rdi)
L(fwd_write_101bytes):
    lddqu    -101(%rsi), %xmm0
    movdqu    %xmm0, -101(%rdi)
L(fwd_write_85bytes):
    lddqu    -85(%rsi), %xmm0
    movdqu    %xmm0, -85(%rdi)
L(fwd_write_69bytes):
    lddqu    -69(%rsi), %xmm0
    movdqu    %xmm0, -69(%rdi)
L(fwd_write_53bytes):
    lddqu    -53(%rsi), %xmm0
    movdqu    %xmm0, -53(%rdi)
L(fwd_write_37bytes):
    lddqu    -37(%rsi), %xmm0
    movdqu    %xmm0, -37(%rdi)
L(fwd_write_21bytes):
    lddqu    -21(%rsi), %xmm0
    lddqu    -16(%rsi), %xmm1
    movdqu    %xmm0, -21(%rdi)
    movdqu    %xmm1, -16(%rdi)
    ret

    .p2align 4
L(fwd_write_5bytes):
    mov    -5(%rsi), %edx
    mov    -4(%rsi), %ecx
    mov    %edx, -5(%rdi)
    mov    %ecx, -4(%rdi)
    ret

    .p2align 4
L(fwd_write_132bytes):
    lddqu    -132(%rsi), %xmm0
    movdqu    %xmm0, -132(%rdi)
L(fwd_write_116bytes):
    lddqu    -116(%rsi), %xmm0
    movdqu    %xmm0, -116(%rdi)
L(fwd_write_100bytes):
    lddqu    -100(%rsi), %xmm0
    movdqu    %xmm0, -100(%rdi)
L(fwd_write_84bytes):
    lddqu    -84(%rsi), %xmm0
    movdqu    %xmm0, -84(%rdi)
L(fwd_write_68bytes):
    lddqu    -68(%rsi), %xmm0
    movdqu    %xmm0, -68(%rdi)
L(fwd_write_52bytes):
    lddqu    -52(%rsi), %xmm0
    movdqu    %xmm0, -52(%rdi)
L(fwd_write_36bytes):
    lddqu    -36(%rsi), %xmm0
    movdqu    %xmm0, -36(%rdi)
L(fwd_write_20bytes):
    lddqu    -20(%rsi), %xmm0
    lddqu    -16(%rsi), %xmm1
    movdqu    %xmm0, -20(%rdi)
    movdqu    %xmm1, -16(%rdi)
    ret

    .p2align 4
L(fwd_write_4bytes):
    mov    -4(%rsi), %edx
    mov    %edx, -4(%rdi)
    ret

    .p2align 4
L(fwd_write_131bytes):
    lddqu    -131(%rsi), %xmm0
    movdqu    %xmm0, -131(%rdi)
L(fwd_write_115bytes):
    lddqu    -115(%rsi), %xmm0
    movdqu    %xmm0, -115(%rdi)
L(fwd_write_99bytes):
    lddqu    -99(%rsi), %xmm0
    movdqu    %xmm0, -99(%rdi)
L(fwd_write_83bytes):
    lddqu    -83(%rsi), %xmm0
    movdqu    %xmm0, -83(%rdi)
L(fwd_write_67bytes):
    lddqu    -67(%rsi), %xmm0
    movdqu    %xmm0, -67(%rdi)
L(fwd_write_51bytes):
    lddqu    -51(%rsi), %xmm0
    movdqu    %xmm0, -51(%rdi)
L(fwd_write_35bytes):
    lddqu    -35(%rsi), %xmm0
    movdqu    %xmm0, -35(%rdi)
L(fwd_write_19bytes):
    lddqu    -19(%rsi), %xmm0
    lddqu    -16(%rsi), %xmm1
    movdqu    %xmm0, -19(%rdi)
    movdqu    %xmm1, -16(%rdi)
    ret

    .p2align 4
L(fwd_write_3bytes):
    mov    -3(%rsi), %dx
    mov    -2(%rsi), %cx
    mov    %dx, -3(%rdi)
    mov    %cx, -2(%rdi)
    ret

    .p2align 4
L(fwd_write_130bytes):
    lddqu    -130(%rsi), %xmm0
    movdqu    %xmm0, -130(%rdi)
L(fwd_write_114bytes):
    lddqu    -114(%rsi), %xmm0
    movdqu    %xmm0, -114(%rdi)
L(fwd_write_98bytes):
    lddqu    -98(%rsi), %xmm0
    movdqu    %xmm0, -98(%rdi)
L(fwd_write_82bytes):
    lddqu    -82(%rsi), %xmm0
    movdqu    %xmm0, -82(%rdi)
L(fwd_write_66bytes):
    lddqu    -66(%rsi), %xmm0
    movdqu    %xmm0, -66(%rdi)
L(fwd_write_50bytes):
    lddqu    -50(%rsi), %xmm0
    movdqu    %xmm0, -50(%rdi)
L(fwd_write_34bytes):
    lddqu    -34(%rsi), %xmm0
    movdqu    %xmm0, -34(%rdi)
L(fwd_write_18bytes):
    lddqu    -18(%rsi), %xmm0
    lddqu    -16(%rsi), %xmm1
    movdqu    %xmm0, -18(%rdi)
    movdqu    %xmm1, -16(%rdi)
    ret

    .p2align 4
L(fwd_write_2bytes):
    movzwl    -2(%rsi), %edx
    mov    %dx, -2(%rdi)
    ret

    .p2align 4
L(fwd_write_129bytes):
    lddqu    -129(%rsi), %xmm0
    movdqu    %xmm0, -129(%rdi)
L(fwd_write_113bytes):
    lddqu    -113(%rsi), %xmm0
    movdqu    %xmm0, -113(%rdi)
L(fwd_write_97bytes):
    lddqu    -97(%rsi), %xmm0
    movdqu    %xmm0, -97(%rdi)
L(fwd_write_81bytes):
    lddqu    -81(%rsi), %xmm0
    movdqu    %xmm0, -81(%rdi)
L(fwd_write_65bytes):
    lddqu    -65(%rsi), %xmm0
    movdqu    %xmm0, -65(%rdi)
L(fwd_write_49bytes):
    lddqu    -49(%rsi), %xmm0
    movdqu    %xmm0, -49(%rdi)
L(fwd_write_33bytes):
    lddqu    -33(%rsi), %xmm0
    movdqu    %xmm0, -33(%rdi)
L(fwd_write_17bytes):
    lddqu    -17(%rsi), %xmm0
    lddqu    -16(%rsi), %xmm1
    movdqu    %xmm0, -17(%rdi)
    movdqu    %xmm1, -16(%rdi)
    ret

    .p2align 4
L(fwd_write_1bytes):
    movzbl    -1(%rsi), %edx
    mov    %dl, -1(%rdi)
    ret

    .p2align 4
L(bwd_write_128bytes):
    lddqu    112(%rsi), %xmm0
    movdqu    %xmm0, 112(%rdi)
L(bwd_write_112bytes):
    lddqu    96(%rsi), %xmm0
    movdqu    %xmm0, 96(%rdi)
L(bwd_write_96bytes):
    lddqu    80(%rsi), %xmm0
    movdqu    %xmm0, 80(%rdi)
L(bwd_write_80bytes):
    lddqu    64(%rsi), %xmm0
    movdqu    %xmm0, 64(%rdi)
L(bwd_write_64bytes):
    lddqu    48(%rsi), %xmm0
    movdqu    %xmm0, 48(%rdi)
L(bwd_write_48bytes):
    lddqu    32(%rsi), %xmm0
    movdqu    %xmm0, 32(%rdi)
L(bwd_write_32bytes):
    lddqu    16(%rsi), %xmm0
    movdqu    %xmm0, 16(%rdi)
L(bwd_write_16bytes):
    lddqu    (%rsi), %xmm0
    movdqu    %xmm0, (%rdi)
L(bwd_write_0bytes):
    ret

    .p2align 4
L(bwd_write_143bytes):
    lddqu    127(%rsi), %xmm0
    movdqu    %xmm0, 127(%rdi)
L(bwd_write_127bytes):
    lddqu    111(%rsi), %xmm0
    movdqu    %xmm0, 111(%rdi)
L(bwd_write_111bytes):
    lddqu    95(%rsi), %xmm0
    movdqu    %xmm0, 95(%rdi)
L(bwd_write_95bytes):
    lddqu    79(%rsi), %xmm0
    movdqu    %xmm0, 79(%rdi)
L(bwd_write_79bytes):
    lddqu    63(%rsi), %xmm0
    movdqu    %xmm0, 63(%rdi)
L(bwd_write_63bytes):
    lddqu    47(%rsi), %xmm0
    movdqu    %xmm0, 47(%rdi)
L(bwd_write_47bytes):
    lddqu    31(%rsi), %xmm0
    movdqu    %xmm0, 31(%rdi)
L(bwd_write_31bytes):
    lddqu    15(%rsi), %xmm0
    lddqu    (%rsi), %xmm1
    movdqu    %xmm0, 15(%rdi)
    movdqu    %xmm1, (%rdi)
    ret


    .p2align 4
L(bwd_write_15bytes):
    mov    7(%rsi), %rdx
    mov    (%rsi), %rcx
    mov    %rdx, 7(%rdi)
    mov    %rcx, (%rdi)
    ret

    .p2align 4
L(bwd_write_142bytes):
    lddqu    126(%rsi), %xmm0
    movdqu    %xmm0, 126(%rdi)
L(bwd_write_126bytes):
    lddqu    110(%rsi), %xmm0
    movdqu    %xmm0, 110(%rdi)
L(bwd_write_110bytes):
    lddqu    94(%rsi), %xmm0
    movdqu    %xmm0, 94(%rdi)
L(bwd_write_94bytes):
    lddqu    78(%rsi), %xmm0
    movdqu    %xmm0, 78(%rdi)
L(bwd_write_78bytes):
    lddqu    62(%rsi), %xmm0
    movdqu    %xmm0, 62(%rdi)
L(bwd_write_62bytes):
    lddqu    46(%rsi), %xmm0
    movdqu    %xmm0, 46(%rdi)
L(bwd_write_46bytes):
    lddqu    30(%rsi), %xmm0
    movdqu    %xmm0, 30(%rdi)
L(bwd_write_30bytes):
    lddqu    14(%rsi), %xmm0
    lddqu    (%rsi), %xmm1
    movdqu    %xmm0, 14(%rdi)
    movdqu    %xmm1, (%rdi)
    ret

    .p2align 4
L(bwd_write_14bytes):
    mov    6(%rsi), %rdx
    mov    (%rsi), %rcx
    mov    %rdx, 6(%rdi)
    mov    %rcx, (%rdi)
    ret

    .p2align 4
L(bwd_write_141bytes):
    lddqu    125(%rsi), %xmm0
    movdqu    %xmm0, 125(%rdi)
L(bwd_write_125bytes):
    lddqu    109(%rsi), %xmm0
    movdqu    %xmm0, 109(%rdi)
L(bwd_write_109bytes):
    lddqu    93(%rsi), %xmm0
    movdqu    %xmm0, 93(%rdi)
L(bwd_write_93bytes):
    lddqu    77(%rsi), %xmm0
    movdqu    %xmm0, 77(%rdi)
L(bwd_write_77bytes):
    lddqu    61(%rsi), %xmm0
    movdqu    %xmm0, 61(%rdi)
L(bwd_write_61bytes):
    lddqu    45(%rsi), %xmm0
    movdqu    %xmm0, 45(%rdi)
L(bwd_write_45bytes):
    lddqu    29(%rsi), %xmm0
    movdqu    %xmm0, 29(%rdi)
L(bwd_write_29bytes):
    lddqu    13(%rsi), %xmm0
    lddqu    (%rsi), %xmm1
    movdqu    %xmm0, 13(%rdi)
    movdqu    %xmm1, (%rdi)
    ret

    .p2align 4
L(bwd_write_13bytes):
    mov    5(%rsi), %rdx
    mov    (%rsi), %rcx
    mov    %rdx, 5(%rdi)
    mov    %rcx, (%rdi)
    ret

    .p2align 4
L(bwd_write_140bytes):
    lddqu    124(%rsi), %xmm0
    movdqu    %xmm0, 124(%rdi)
L(bwd_write_124bytes):
    lddqu    108(%rsi), %xmm0
    movdqu    %xmm0, 108(%rdi)
L(bwd_write_108bytes):
    lddqu    92(%rsi), %xmm0
    movdqu    %xmm0, 92(%rdi)
L(bwd_write_92bytes):
    lddqu    76(%rsi), %xmm0
    movdqu    %xmm0, 76(%rdi)
L(bwd_write_76bytes):
    lddqu    60(%rsi), %xmm0
    movdqu    %xmm0, 60(%rdi)
L(bwd_write_60bytes):
    lddqu    44(%rsi), %xmm0
    movdqu    %xmm0, 44(%rdi)
L(bwd_write_44bytes):
    lddqu    28(%rsi), %xmm0
    movdqu    %xmm0, 28(%rdi)
L(bwd_write_28bytes):
    lddqu    12(%rsi), %xmm0
    lddqu    (%rsi), %xmm1
    movdqu    %xmm0, 12(%rdi)
    movdqu    %xmm1, (%rdi)
    ret

    .p2align 4
L(bwd_write_12bytes):
    mov    4(%rsi), %rdx
    mov    (%rsi), %rcx
    mov    %rdx, 4(%rdi)
    mov    %rcx, (%rdi)
    ret

    .p2align 4
L(bwd_write_139bytes):
    lddqu    123(%rsi), %xmm0
    movdqu    %xmm0, 123(%rdi)
L(bwd_write_123bytes):
    lddqu    107(%rsi), %xmm0
    movdqu    %xmm0, 107(%rdi)
L(bwd_write_107bytes):
    lddqu    91(%rsi), %xmm0
    movdqu    %xmm0, 91(%rdi)
L(bwd_write_91bytes):
    lddqu    75(%rsi), %xmm0
    movdqu    %xmm0, 75(%rdi)
L(bwd_write_75bytes):
    lddqu    59(%rsi), %xmm0
    movdqu    %xmm0, 59(%rdi)
L(bwd_write_59bytes):
    lddqu    43(%rsi), %xmm0
    movdqu    %xmm0, 43(%rdi)
L(bwd_write_43bytes):
    lddqu    27(%rsi), %xmm0
    movdqu    %xmm0, 27(%rdi)
L(bwd_write_27bytes):
    lddqu    11(%rsi), %xmm0
    lddqu    (%rsi), %xmm1
    movdqu    %xmm0, 11(%rdi)
    movdqu    %xmm1, (%rdi)
    ret

    .p2align 4
L(bwd_write_11bytes):
    mov    3(%rsi), %rdx
    mov    (%rsi), %rcx
    mov    %rdx, 3(%rdi)
    mov    %rcx, (%rdi)
    ret

    .p2align 4
L(bwd_write_138bytes):
    lddqu    122(%rsi), %xmm0
    movdqu    %xmm0, 122(%rdi)
L(bwd_write_122bytes):
    lddqu    106(%rsi), %xmm0
    movdqu    %xmm0, 106(%rdi)
L(bwd_write_106bytes):
    lddqu    90(%rsi), %xmm0
    movdqu    %xmm0, 90(%rdi)
L(bwd_write_90bytes):
    lddqu    74(%rsi), %xmm0
    movdqu    %xmm0, 74(%rdi)
L(bwd_write_74bytes):
    lddqu    58(%rsi), %xmm0
    movdqu    %xmm0, 58(%rdi)
L(bwd_write_58bytes):
    lddqu    42(%rsi), %xmm0
    movdqu    %xmm0, 42(%rdi)
L(bwd_write_42bytes):
    lddqu    26(%rsi), %xmm0
    movdqu    %xmm0, 26(%rdi)
L(bwd_write_26bytes):
    lddqu    10(%rsi), %xmm0
    lddqu    (%rsi), %xmm1
    movdqu    %xmm0, 10(%rdi)
    movdqu    %xmm1, (%rdi)
    ret

    .p2align 4
L(bwd_write_10bytes):
    mov    2(%rsi), %rdx
    mov    (%rsi), %rcx
    mov    %rdx, 2(%rdi)
    mov    %rcx, (%rdi)
    ret

    .p2align 4
L(bwd_write_137bytes):
    lddqu    121(%rsi), %xmm0
    movdqu    %xmm0, 121(%rdi)
L(bwd_write_121bytes):
    lddqu    105(%rsi), %xmm0
    movdqu    %xmm0, 105(%rdi)
L(bwd_write_105bytes):
    lddqu    89(%rsi), %xmm0
    movdqu    %xmm0, 89(%rdi)
L(bwd_write_89bytes):
    lddqu    73(%rsi), %xmm0
    movdqu    %xmm0, 73(%rdi)
L(bwd_write_73bytes):
    lddqu    57(%rsi), %xmm0
    movdqu    %xmm0, 57(%rdi)
L(bwd_write_57bytes):
    lddqu    41(%rsi), %xmm0
    movdqu    %xmm0, 41(%rdi)
L(bwd_write_41bytes):
    lddqu    25(%rsi), %xmm0
    movdqu    %xmm0, 25(%rdi)
L(bwd_write_25bytes):
    lddqu    9(%rsi), %xmm0
    lddqu    (%rsi), %xmm1
    movdqu    %xmm0, 9(%rdi)
    movdqu    %xmm1, (%rdi)
    ret

    .p2align 4
L(bwd_write_9bytes):
    mov    1(%rsi), %rdx
    mov    (%rsi), %rcx
    mov    %rdx, 1(%rdi)
    mov    %rcx, (%rdi)
    ret

    .p2align 4
L(bwd_write_136bytes):
    lddqu    120(%rsi), %xmm0
    movdqu    %xmm0, 120(%rdi)
L(bwd_write_120bytes):
    lddqu    104(%rsi), %xmm0
    movdqu    %xmm0, 104(%rdi)
L(bwd_write_104bytes):
    lddqu    88(%rsi), %xmm0
    movdqu    %xmm0, 88(%rdi)
L(bwd_write_88bytes):
    lddqu    72(%rsi), %xmm0
    movdqu    %xmm0, 72(%rdi)
L(bwd_write_72bytes):
    lddqu    56(%rsi), %xmm0
    movdqu    %xmm0, 56(%rdi)
L(bwd_write_56bytes):
    lddqu    40(%rsi), %xmm0
    movdqu    %xmm0, 40(%rdi)
L(bwd_write_40bytes):
    lddqu    24(%rsi), %xmm0
    movdqu    %xmm0, 24(%rdi)
L(bwd_write_24bytes):
    lddqu    8(%rsi), %xmm0
    lddqu    (%rsi), %xmm1
    movdqu    %xmm0, 8(%rdi)
    movdqu    %xmm1, (%rdi)
    ret

    .p2align 4
L(bwd_write_8bytes):
    mov    (%rsi), %rdx
    mov    %rdx, (%rdi)
    ret

    .p2align 4
L(bwd_write_135bytes):
    lddqu    119(%rsi), %xmm0
    movdqu    %xmm0, 119(%rdi)
L(bwd_write_119bytes):
    lddqu    103(%rsi), %xmm0
    movdqu    %xmm0, 103(%rdi)
L(bwd_write_103bytes):
    lddqu    87(%rsi), %xmm0
    movdqu    %xmm0, 87(%rdi)
L(bwd_write_87bytes):
    lddqu    71(%rsi), %xmm0
    movdqu    %xmm0, 71(%rdi)
L(bwd_write_71bytes):
    lddqu    55(%rsi), %xmm0
    movdqu    %xmm0, 55(%rdi)
L(bwd_write_55bytes):
    lddqu    39(%rsi), %xmm0
    movdqu    %xmm0, 39(%rdi)
L(bwd_write_39bytes):
    lddqu    23(%rsi), %xmm0
    movdqu    %xmm0, 23(%rdi)
L(bwd_write_23bytes):
    lddqu    7(%rsi), %xmm0
    lddqu    (%rsi), %xmm1
    movdqu    %xmm0, 7(%rdi)
    movdqu    %xmm1, (%rdi)
    ret

    .p2align 4
L(bwd_write_7bytes):
    mov    3(%rsi), %edx
    mov    (%rsi), %ecx
    mov    %edx, 3(%rdi)
    mov    %ecx, (%rdi)
    ret

    .p2align 4
L(bwd_write_134bytes):
    lddqu    118(%rsi), %xmm0
    movdqu    %xmm0, 118(%rdi)
L(bwd_write_118bytes):
    lddqu    102(%rsi), %xmm0
    movdqu    %xmm0, 102(%rdi)
L(bwd_write_102bytes):
    lddqu    86(%rsi), %xmm0
    movdqu    %xmm0, 86(%rdi)
L(bwd_write_86bytes):
    lddqu    70(%rsi), %xmm0
    movdqu    %xmm0, 70(%rdi)
L(bwd_write_70bytes):
    lddqu    54(%rsi), %xmm0
    movdqu    %xmm0, 54(%rdi)
L(bwd_write_54bytes):
    lddqu    38(%rsi), %xmm0
    movdqu    %xmm0, 38(%rdi)
L(bwd_write_38bytes):
    lddqu    22(%rsi), %xmm0
    movdqu    %xmm0, 22(%rdi)
L(bwd_write_22bytes):
    lddqu    6(%rsi), %xmm0
    lddqu    (%rsi), %xmm1
    movdqu    %xmm0, 6(%rdi)
    movdqu    %xmm1, (%rdi)
    ret

    .p2align 4
L(bwd_write_6bytes):
    mov    2(%rsi), %edx
    mov    (%rsi), %ecx
    mov    %edx, 2(%rdi)
    mov    %ecx, (%rdi)
    ret

    .p2align 4
L(bwd_write_133bytes):
    lddqu    117(%rsi), %xmm0
    movdqu    %xmm0, 117(%rdi)
L(bwd_write_117bytes):
    lddqu    101(%rsi), %xmm0
    movdqu    %xmm0, 101(%rdi)
L(bwd_write_101bytes):
    lddqu    85(%rsi), %xmm0
    movdqu    %xmm0, 85(%rdi)
L(bwd_write_85bytes):
    lddqu    69(%rsi), %xmm0
    movdqu    %xmm0, 69(%rdi)
L(bwd_write_69bytes):
    lddqu    53(%rsi), %xmm0
    movdqu    %xmm0, 53(%rdi)
L(bwd_write_53bytes):
    lddqu    37(%rsi), %xmm0
    movdqu    %xmm0, 37(%rdi)
L(bwd_write_37bytes):
    lddqu    21(%rsi), %xmm0
    movdqu    %xmm0, 21(%rdi)
L(bwd_write_21bytes):
    lddqu    5(%rsi), %xmm0
    lddqu    (%rsi), %xmm1
    movdqu    %xmm0, 5(%rdi)
    movdqu    %xmm1, (%rdi)
    ret

    .p2align 4
L(bwd_write_5bytes):
    mov    1(%rsi), %edx
    mov    (%rsi), %ecx
    mov    %edx, 1(%rdi)
    mov    %ecx, (%rdi)
    ret

    .p2align 4
L(bwd_write_132bytes):
    lddqu    116(%rsi), %xmm0
    movdqu    %xmm0, 116(%rdi)
L(bwd_write_116bytes):
    lddqu    100(%rsi), %xmm0
    movdqu    %xmm0, 100(%rdi)
L(bwd_write_100bytes):
    lddqu    84(%rsi), %xmm0
    movdqu    %xmm0, 84(%rdi)
L(bwd_write_84bytes):
    lddqu    68(%rsi), %xmm0
    movdqu    %xmm0, 68(%rdi)
L(bwd_write_68bytes):
    lddqu    52(%rsi), %xmm0
    movdqu    %xmm0, 52(%rdi)
L(bwd_write_52bytes):
    lddqu    36(%rsi), %xmm0
    movdqu    %xmm0, 36(%rdi)
L(bwd_write_36bytes):
    lddqu    20(%rsi), %xmm0
    movdqu    %xmm0, 20(%rdi)
L(bwd_write_20bytes):
    lddqu    4(%rsi), %xmm0
    lddqu    (%rsi), %xmm1
    movdqu    %xmm0, 4(%rdi)
    movdqu    %xmm1, (%rdi)
    ret

    .p2align 4
L(bwd_write_4bytes):
    mov    (%rsi), %edx
    mov    %edx, (%rdi)
    ret

    .p2align 4
L(bwd_write_131bytes):
    lddqu    115(%rsi), %xmm0
    movdqu    %xmm0, 115(%rdi)
L(bwd_write_115bytes):
    lddqu    99(%rsi), %xmm0
    movdqu    %xmm0, 99(%rdi)
L(bwd_write_99bytes):
    lddqu    83(%rsi), %xmm0
    movdqu    %xmm0, 83(%rdi)
L(bwd_write_83bytes):
    lddqu    67(%rsi), %xmm0
    movdqu    %xmm0, 67(%rdi)
L(bwd_write_67bytes):
    lddqu    51(%rsi), %xmm0
    movdqu    %xmm0, 51(%rdi)
L(bwd_write_51bytes):
    lddqu    35(%rsi), %xmm0
    movdqu    %xmm0, 35(%rdi)
L(bwd_write_35bytes):
    lddqu    19(%rsi), %xmm0
    movdqu    %xmm0, 19(%rdi)
L(bwd_write_19bytes):
    lddqu    3(%rsi), %xmm0
    lddqu    (%rsi), %xmm1
    movdqu    %xmm0, 3(%rdi)
    movdqu    %xmm1, (%rdi)
    ret

    .p2align 4
L(bwd_write_3bytes):
    mov    1(%rsi), %dx
    mov    (%rsi), %cx
    mov    %dx, 1(%rdi)
    mov    %cx, (%rdi)
    ret

    .p2align 4
L(bwd_write_130bytes):
    lddqu    114(%rsi), %xmm0
    movdqu    %xmm0, 114(%rdi)
L(bwd_write_114bytes):
    lddqu    98(%rsi), %xmm0
    movdqu    %xmm0, 98(%rdi)
L(bwd_write_98bytes):
    lddqu    82(%rsi), %xmm0
    movdqu    %xmm0, 82(%rdi)
L(bwd_write_82bytes):
    lddqu    66(%rsi), %xmm0
    movdqu    %xmm0, 66(%rdi)
L(bwd_write_66bytes):
    lddqu    50(%rsi), %xmm0
    movdqu    %xmm0, 50(%rdi)
L(bwd_write_50bytes):
    lddqu    34(%rsi), %xmm0
    movdqu    %xmm0, 34(%rdi)
L(bwd_write_34bytes):
    lddqu    18(%rsi), %xmm0
    movdqu    %xmm0, 18(%rdi)
L(bwd_write_18bytes):
    lddqu    2(%rsi), %xmm0
    lddqu    (%rsi), %xmm1
    movdqu    %xmm0, 2(%rdi)
    movdqu    %xmm1, (%rdi)
    ret

    .p2align 4
L(bwd_write_2bytes):
    movzwl    (%rsi), %edx
    mov    %dx, (%rdi)
    ret

    .p2align 4
L(bwd_write_129bytes):
    lddqu    113(%rsi), %xmm0
    movdqu    %xmm0, 113(%rdi)
L(bwd_write_113bytes):
    lddqu    97(%rsi), %xmm0
    movdqu    %xmm0, 97(%rdi)
L(bwd_write_97bytes):
    lddqu    81(%rsi), %xmm0
    movdqu    %xmm0, 81(%rdi)
L(bwd_write_81bytes):
    lddqu    65(%rsi), %xmm0
    movdqu    %xmm0, 65(%rdi)
L(bwd_write_65bytes):
    lddqu    49(%rsi), %xmm0
    movdqu    %xmm0, 49(%rdi)
L(bwd_write_49bytes):
    lddqu    33(%rsi), %xmm0
    movdqu    %xmm0, 33(%rdi)
L(bwd_write_33bytes):
    lddqu    17(%rsi), %xmm0
    movdqu    %xmm0, 17(%rdi)
L(bwd_write_17bytes):
    lddqu    1(%rsi), %xmm0
    lddqu    (%rsi), %xmm1
    movdqu    %xmm0, 1(%rdi)
    movdqu    %xmm1, (%rdi)
    ret

    .p2align 4
L(bwd_write_1bytes):
    movzbl    (%rsi), %edx
    mov    %dl, (%rdi)
    ret

END (MEMCPY)

    .section .rodata.ssse3,"a",@progbits
    .p2align 3
L(table_144_bytes_bwd):
    .int    JMPTBL (L(bwd_write_0bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_1bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_2bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_3bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_4bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_5bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_6bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_7bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_8bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_9bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_10bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_11bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_12bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_13bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_14bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_15bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_16bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_17bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_18bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_19bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_20bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_21bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_22bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_23bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_24bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_25bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_26bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_27bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_28bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_29bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_30bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_31bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_32bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_33bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_34bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_35bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_36bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_37bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_38bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_39bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_40bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_41bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_42bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_43bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_44bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_45bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_46bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_47bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_48bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_49bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_50bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_51bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_52bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_53bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_54bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_55bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_56bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_57bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_58bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_59bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_60bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_61bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_62bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_63bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_64bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_65bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_66bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_67bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_68bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_69bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_70bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_71bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_72bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_73bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_74bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_75bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_76bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_77bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_78bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_79bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_80bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_81bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_82bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_83bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_84bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_85bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_86bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_87bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_88bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_89bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_90bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_91bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_92bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_93bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_94bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_95bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_96bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_97bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_98bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_99bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_100bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_101bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_102bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_103bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_104bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_105bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_106bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_107bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_108bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_109bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_110bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_111bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_112bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_113bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_114bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_115bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_116bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_117bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_118bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_119bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_120bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_121bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_122bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_123bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_124bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_125bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_126bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_127bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_128bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_129bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_130bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_131bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_132bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_133bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_134bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_135bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_136bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_137bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_138bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_139bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_140bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_141bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_142bytes), L(table_144_bytes_bwd))
    .int    JMPTBL (L(bwd_write_143bytes), L(table_144_bytes_bwd))

    .p2align 3
L(table_144_bytes_fwd):
    .int    JMPTBL (L(fwd_write_0bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_1bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_2bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_3bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_4bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_5bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_6bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_7bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_8bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_9bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_10bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_11bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_12bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_13bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_14bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_15bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_16bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_17bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_18bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_19bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_20bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_21bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_22bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_23bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_24bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_25bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_26bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_27bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_28bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_29bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_30bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_31bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_32bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_33bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_34bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_35bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_36bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_37bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_38bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_39bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_40bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_41bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_42bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_43bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_44bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_45bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_46bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_47bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_48bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_49bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_50bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_51bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_52bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_53bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_54bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_55bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_56bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_57bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_58bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_59bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_60bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_61bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_62bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_63bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_64bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_65bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_66bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_67bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_68bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_69bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_70bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_71bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_72bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_73bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_74bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_75bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_76bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_77bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_78bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_79bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_80bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_81bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_82bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_83bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_84bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_85bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_86bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_87bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_88bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_89bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_90bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_91bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_92bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_93bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_94bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_95bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_96bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_97bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_98bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_99bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_100bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_101bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_102bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_103bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_104bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_105bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_106bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_107bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_108bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_109bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_110bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_111bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_112bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_113bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_114bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_115bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_116bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_117bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_118bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_119bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_120bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_121bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_122bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_123bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_124bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_125bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_126bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_127bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_128bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_129bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_130bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_131bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_132bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_133bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_134bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_135bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_136bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_137bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_138bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_139bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_140bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_141bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_142bytes), L(table_144_bytes_fwd))
    .int    JMPTBL (L(fwd_write_143bytes), L(table_144_bytes_fwd))

    .p2align 3
L(shl_table_fwd):
    .int    JMPTBL (L(shl_0), L(shl_table_fwd))
    .int    JMPTBL (L(shl_1), L(shl_table_fwd))
    .int    JMPTBL (L(shl_2), L(shl_table_fwd))
    .int    JMPTBL (L(shl_3), L(shl_table_fwd))
    .int    JMPTBL (L(shl_4), L(shl_table_fwd))
    .int    JMPTBL (L(shl_5), L(shl_table_fwd))
    .int    JMPTBL (L(shl_6), L(shl_table_fwd))
    .int    JMPTBL (L(shl_7), L(shl_table_fwd))
    .int    JMPTBL (L(shl_8), L(shl_table_fwd))
    .int    JMPTBL (L(shl_9), L(shl_table_fwd))
    .int    JMPTBL (L(shl_10), L(shl_table_fwd))
    .int    JMPTBL (L(shl_11), L(shl_table_fwd))
    .int    JMPTBL (L(shl_12), L(shl_table_fwd))
    .int    JMPTBL (L(shl_13), L(shl_table_fwd))
    .int    JMPTBL (L(shl_14), L(shl_table_fwd))
    .int    JMPTBL (L(shl_15), L(shl_table_fwd))

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
