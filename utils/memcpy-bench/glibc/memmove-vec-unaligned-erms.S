/* memmove/memcpy/mempcpy with unaligned load/store and rep movsb
   Copyright (C) 2016-2020 Free Software Foundation, Inc.
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

/* memmove/memcpy/mempcpy is implemented as:
   1. Use overlapping load and store to avoid branch.
   2. Load all sources into registers and store them together to avoid
      possible address overlap between source and destination.
   3. If size is 8 * VEC_SIZE or less, load all sources into registers
      and store them together.
   4. If address of destination > address of source, backward copy
      4 * VEC_SIZE at a time with unaligned load and aligned store.
      Load the first 4 * VEC and last VEC before the loop and store
      them after the loop to support overlapping addresses.
   5. Otherwise, forward copy 4 * VEC_SIZE at a time with unaligned
      load and aligned store.  Load the last 4 * VEC and first VEC
      before the loop and store them after the loop to support
      overlapping addresses.
   6. If size >= __x86_shared_non_temporal_threshold and there is no
      overlap between destination and source, use non-temporal store
      instead of aligned store.  */

#include "sysdep.h"

#ifndef MEMCPY_SYMBOL
# define MEMCPY_SYMBOL(p,s)        MEMMOVE_SYMBOL(p, s)
#endif

#ifndef MEMPCPY_SYMBOL
# define MEMPCPY_SYMBOL(p,s)        MEMMOVE_SYMBOL(p, s)
#endif

#ifndef MEMMOVE_CHK_SYMBOL
# define MEMMOVE_CHK_SYMBOL(p,s)    MEMMOVE_SYMBOL(p, s)
#endif

#ifndef VZEROUPPER
# if VEC_SIZE > 16
#  define VZEROUPPER vzeroupper
# else
#  define VZEROUPPER
# endif
#endif

#ifndef PREFETCH
# define PREFETCH(addr) prefetcht0 addr
#endif

/* Assume 64-byte prefetch size.  */
#ifndef PREFETCH_SIZE
# define PREFETCH_SIZE 64
#endif

#define PREFETCHED_LOAD_SIZE (VEC_SIZE * 4)

#if PREFETCH_SIZE == 64
# if PREFETCHED_LOAD_SIZE == PREFETCH_SIZE
#  define PREFETCH_ONE_SET(dir, base, offset) \
    PREFETCH ((offset)base)
# elif PREFETCHED_LOAD_SIZE == 2 * PREFETCH_SIZE
#  define PREFETCH_ONE_SET(dir, base, offset) \
    PREFETCH ((offset)base); \
    PREFETCH ((offset + dir * PREFETCH_SIZE)base)
# elif PREFETCHED_LOAD_SIZE == 4 * PREFETCH_SIZE
#  define PREFETCH_ONE_SET(dir, base, offset) \
    PREFETCH ((offset)base); \
    PREFETCH ((offset + dir * PREFETCH_SIZE)base); \
    PREFETCH ((offset + dir * PREFETCH_SIZE * 2)base); \
    PREFETCH ((offset + dir * PREFETCH_SIZE * 3)base)
# else
#   error Unsupported PREFETCHED_LOAD_SIZE!
# endif
#else
# error Unsupported PREFETCH_SIZE!
#endif

#ifndef SECTION
# error SECTION is not defined!
#endif

    .section SECTION(.text),"ax",@progbits
#if defined SHARED
ENTRY (MEMMOVE_CHK_SYMBOL (__mempcpy_chk, unaligned))
    cmp    %RDX_LP, %RCX_LP
    jb    HIDDEN_JUMPTARGET (__chk_fail)
END (MEMMOVE_CHK_SYMBOL (__mempcpy_chk, unaligned))
#endif

ENTRY (MEMPCPY_SYMBOL (__mempcpy, unaligned))
    mov    %RDI_LP, %RAX_LP
    add    %RDX_LP, %RAX_LP
    jmp    L(start)
END (MEMPCPY_SYMBOL (__mempcpy, unaligned))

#if defined SHARED
ENTRY (MEMMOVE_CHK_SYMBOL (__memmove_chk, unaligned))
    cmp    %RDX_LP, %RCX_LP
    jb    HIDDEN_JUMPTARGET (__chk_fail)
END (MEMMOVE_CHK_SYMBOL (__memmove_chk, unaligned))
#endif

ENTRY (MEMMOVE_SYMBOL (__memmove, unaligned))
    movq    %rdi, %rax
L(start):
# ifdef __ILP32__
    /* Clear the upper 32 bits.  */
    movl    %edx, %edx
# endif
    cmp    $VEC_SIZE, %RDX_LP
    jb    L(less_vec)
    cmp    $(VEC_SIZE * 2), %RDX_LP
    ja    L(more_2x_vec)
#if !defined USE_MULTIARCH
L(last_2x_vec):
#endif
    /* From VEC and to 2 * VEC.  No branch when size == VEC_SIZE.  */
    VMOVU    (%rsi), %VEC(0)
    VMOVU    -VEC_SIZE(%rsi,%rdx), %VEC(1)
    VMOVU    %VEC(0), (%rdi)
    VMOVU    %VEC(1), -VEC_SIZE(%rdi,%rdx)
    VZEROUPPER
#if !defined USE_MULTIARCH
L(nop):
#endif
    ret
#if defined USE_MULTIARCH
END (MEMMOVE_SYMBOL (__memmove, unaligned))

# if VEC_SIZE == 16
ENTRY (__mempcpy_chk_erms)
    cmp    %RDX_LP, %RCX_LP
    jb    HIDDEN_JUMPTARGET (__chk_fail)
END (__mempcpy_chk_erms)

/* Only used to measure performance of REP MOVSB.  */
ENTRY (__mempcpy_erms)
    mov    %RDI_LP, %RAX_LP
    /* Skip zero length.  */
    test    %RDX_LP, %RDX_LP
    jz    2f
    add    %RDX_LP, %RAX_LP
    jmp    L(start_movsb)
END (__mempcpy_erms)

ENTRY (__memmove_chk_erms)
    cmp    %RDX_LP, %RCX_LP
    jb    HIDDEN_JUMPTARGET (__chk_fail)
END (__memmove_chk_erms)

ENTRY (__memmove_erms)
    movq    %rdi, %rax
    /* Skip zero length.  */
    test    %RDX_LP, %RDX_LP
    jz    2f
L(start_movsb):
    mov    %RDX_LP, %RCX_LP
    cmp    %RSI_LP, %RDI_LP
    jb    1f
    /* Source == destination is less common.  */
    je    2f
    lea    (%rsi,%rcx), %RDX_LP
    cmp    %RDX_LP, %RDI_LP
    jb    L(movsb_backward)
1:
    rep movsb
2:
    ret
L(movsb_backward):
    leaq    -1(%rdi,%rcx), %rdi
    leaq    -1(%rsi,%rcx), %rsi
    std
    rep movsb
    cld
    ret
END (__memmove_erms)
strong_alias (__memmove_erms, __memcpy_erms)
strong_alias (__memmove_chk_erms, __memcpy_chk_erms)
# endif

# ifdef SHARED
ENTRY (MEMMOVE_CHK_SYMBOL (__mempcpy_chk, unaligned_erms))
    cmp    %RDX_LP, %RCX_LP
    jb    HIDDEN_JUMPTARGET (__chk_fail)
END (MEMMOVE_CHK_SYMBOL (__mempcpy_chk, unaligned_erms))
# endif

ENTRY (MEMMOVE_SYMBOL (__mempcpy, unaligned_erms))
    mov    %RDI_LP, %RAX_LP
    add    %RDX_LP, %RAX_LP
    jmp    L(start_erms)
END (MEMMOVE_SYMBOL (__mempcpy, unaligned_erms))

# ifdef SHARED
ENTRY (MEMMOVE_CHK_SYMBOL (__memmove_chk, unaligned_erms))
    cmp    %RDX_LP, %RCX_LP
    jb    HIDDEN_JUMPTARGET (__chk_fail)
END (MEMMOVE_CHK_SYMBOL (__memmove_chk, unaligned_erms))
# endif

ENTRY (MEMMOVE_SYMBOL (__memmove, unaligned_erms))
    movq    %rdi, %rax
L(start_erms):
# ifdef __ILP32__
    /* Clear the upper 32 bits.  */
    movl    %edx, %edx
# endif
    cmp    $VEC_SIZE, %RDX_LP
    jb    L(less_vec)
    cmp    $(VEC_SIZE * 2), %RDX_LP
    ja    L(movsb_more_2x_vec)
L(last_2x_vec):
    /* From VEC and to 2 * VEC.  No branch when size == VEC_SIZE. */
    VMOVU    (%rsi), %VEC(0)
    VMOVU    -VEC_SIZE(%rsi,%rdx), %VEC(1)
    VMOVU    %VEC(0), (%rdi)
    VMOVU    %VEC(1), -VEC_SIZE(%rdi,%rdx)
L(return):
    VZEROUPPER
    ret

L(movsb):
    cmp    $SHARED_NON_TEMPORAL_THRESHOLD, %RDX_LP
    jae    L(more_8x_vec)
    cmpq    %rsi, %rdi
    jb    1f
    /* Source == destination is less common.  */
    je    L(nop)
    leaq    (%rsi,%rdx), %r9
    cmpq    %r9, %rdi
    /* Avoid slow backward REP MOVSB.  */
    jb    L(more_8x_vec_backward)
1:
    mov    %RDX_LP, %RCX_LP
    rep movsb
L(nop):
    ret
#endif

L(less_vec):
    /* Less than 1 VEC.  */
#if VEC_SIZE != 16 && VEC_SIZE != 32 && VEC_SIZE != 64
# error Unsupported VEC_SIZE!
#endif
#if VEC_SIZE > 32
    cmpb    $32, %dl
    jae    L(between_32_63)
#endif
#if VEC_SIZE > 16
    cmpb    $16, %dl
    jae    L(between_16_31)
#endif
    cmpb    $8, %dl
    jae    L(between_8_15)
    cmpb    $4, %dl
    jae    L(between_4_7)
    cmpb    $1, %dl
    ja    L(between_2_3)
    jb    1f
    movzbl    (%rsi), %ecx
    movb    %cl, (%rdi)
1:
    ret
#if VEC_SIZE > 32
L(between_32_63):
    /* From 32 to 63.  No branch when size == 32.  */
    vmovdqu    (%rsi), %ymm0
    vmovdqu    -32(%rsi,%rdx), %ymm1
    vmovdqu    %ymm0, (%rdi)
    vmovdqu    %ymm1, -32(%rdi,%rdx)
    VZEROUPPER
    ret
#endif
#if VEC_SIZE > 16
    /* From 16 to 31.  No branch when size == 16.  */
L(between_16_31):
    vmovdqu    (%rsi), %xmm0
    vmovdqu    -16(%rsi,%rdx), %xmm1
    vmovdqu    %xmm0, (%rdi)
    vmovdqu    %xmm1, -16(%rdi,%rdx)
    ret
#endif
L(between_8_15):
    /* From 8 to 15.  No branch when size == 8.  */
    movq    -8(%rsi,%rdx), %rcx
    movq    (%rsi), %rsi
    movq    %rcx, -8(%rdi,%rdx)
    movq    %rsi, (%rdi)
    ret
L(between_4_7):
    /* From 4 to 7.  No branch when size == 4.  */
    movl    -4(%rsi,%rdx), %ecx
    movl    (%rsi), %esi
    movl    %ecx, -4(%rdi,%rdx)
    movl    %esi, (%rdi)
    ret
L(between_2_3):
    /* From 2 to 3.  No branch when size == 2.  */
    movzwl    -2(%rsi,%rdx), %ecx
    movzwl    (%rsi), %esi
    movw    %cx, -2(%rdi,%rdx)
    movw    %si, (%rdi)
    ret

#if defined USE_MULTIARCH
L(movsb_more_2x_vec):
    cmp    $REP_MOSB_THRESHOLD, %RDX_LP
    ja    L(movsb)
#endif
L(more_2x_vec):
    /* More than 2 * VEC and there may be overlap between destination
       and source.  */
    cmpq    $(VEC_SIZE * 8), %rdx
    ja    L(more_8x_vec)
    cmpq    $(VEC_SIZE * 4), %rdx
    jb    L(last_4x_vec)
    /* Copy from 4 * VEC to 8 * VEC, inclusively. */
    VMOVU    (%rsi), %VEC(0)
    VMOVU    VEC_SIZE(%rsi), %VEC(1)
    VMOVU    (VEC_SIZE * 2)(%rsi), %VEC(2)
    VMOVU    (VEC_SIZE * 3)(%rsi), %VEC(3)
    VMOVU    -VEC_SIZE(%rsi,%rdx), %VEC(4)
    VMOVU    -(VEC_SIZE * 2)(%rsi,%rdx), %VEC(5)
    VMOVU    -(VEC_SIZE * 3)(%rsi,%rdx), %VEC(6)
    VMOVU    -(VEC_SIZE * 4)(%rsi,%rdx), %VEC(7)
    VMOVU    %VEC(0), (%rdi)
    VMOVU    %VEC(1), VEC_SIZE(%rdi)
    VMOVU    %VEC(2), (VEC_SIZE * 2)(%rdi)
    VMOVU    %VEC(3), (VEC_SIZE * 3)(%rdi)
    VMOVU    %VEC(4), -VEC_SIZE(%rdi,%rdx)
    VMOVU    %VEC(5), -(VEC_SIZE * 2)(%rdi,%rdx)
    VMOVU    %VEC(6), -(VEC_SIZE * 3)(%rdi,%rdx)
    VMOVU    %VEC(7), -(VEC_SIZE * 4)(%rdi,%rdx)
    VZEROUPPER
    ret
L(last_4x_vec):
    /* Copy from 2 * VEC to 4 * VEC. */
    VMOVU    (%rsi), %VEC(0)
    VMOVU    VEC_SIZE(%rsi), %VEC(1)
    VMOVU    -VEC_SIZE(%rsi,%rdx), %VEC(2)
    VMOVU    -(VEC_SIZE * 2)(%rsi,%rdx), %VEC(3)
    VMOVU    %VEC(0), (%rdi)
    VMOVU    %VEC(1), VEC_SIZE(%rdi)
    VMOVU    %VEC(2), -VEC_SIZE(%rdi,%rdx)
    VMOVU    %VEC(3), -(VEC_SIZE * 2)(%rdi,%rdx)
    VZEROUPPER
    ret

L(more_8x_vec):
    cmpq    %rsi, %rdi
    ja    L(more_8x_vec_backward)
    /* Source == destination is less common.  */
    je    L(nop)
    /* Load the first VEC and last 4 * VEC to support overlapping
       addresses.  */
    VMOVU    (%rsi), %VEC(4)
    VMOVU    -VEC_SIZE(%rsi, %rdx), %VEC(5)
    VMOVU    -(VEC_SIZE * 2)(%rsi, %rdx), %VEC(6)
    VMOVU    -(VEC_SIZE * 3)(%rsi, %rdx), %VEC(7)
    VMOVU    -(VEC_SIZE * 4)(%rsi, %rdx), %VEC(8)
    /* Save start and stop of the destination buffer.  */
    movq    %rdi, %r11
    leaq    -VEC_SIZE(%rdi, %rdx), %rcx
    /* Align destination for aligned stores in the loop.  Compute
       how much destination is misaligned.  */
    movq    %rdi, %r8
    andq    $(VEC_SIZE - 1), %r8
    /* Get the negative of offset for alignment.  */
    subq    $VEC_SIZE, %r8
    /* Adjust source.  */
    subq    %r8, %rsi
    /* Adjust destination which should be aligned now.  */
    subq    %r8, %rdi
    /* Adjust length.  */
    addq    %r8, %rdx
#if (defined USE_MULTIARCH || VEC_SIZE == 16)
    /* Check non-temporal store threshold.  */
    cmp    $SHARED_NON_TEMPORAL_THRESHOLD, %RDX_LP
    ja    L(large_forward)
#endif
L(loop_4x_vec_forward):
    /* Copy 4 * VEC a time forward.  */
    VMOVU    (%rsi), %VEC(0)
    VMOVU    VEC_SIZE(%rsi), %VEC(1)
    VMOVU    (VEC_SIZE * 2)(%rsi), %VEC(2)
    VMOVU    (VEC_SIZE * 3)(%rsi), %VEC(3)
    addq    $(VEC_SIZE * 4), %rsi
    subq    $(VEC_SIZE * 4), %rdx
    VMOVA    %VEC(0), (%rdi)
    VMOVA    %VEC(1), VEC_SIZE(%rdi)
    VMOVA    %VEC(2), (VEC_SIZE * 2)(%rdi)
    VMOVA    %VEC(3), (VEC_SIZE * 3)(%rdi)
    addq    $(VEC_SIZE * 4), %rdi
    cmpq    $(VEC_SIZE * 4), %rdx
    ja    L(loop_4x_vec_forward)
    /* Store the last 4 * VEC.  */
    VMOVU    %VEC(5), (%rcx)
    VMOVU    %VEC(6), -VEC_SIZE(%rcx)
    VMOVU    %VEC(7), -(VEC_SIZE * 2)(%rcx)
    VMOVU    %VEC(8), -(VEC_SIZE * 3)(%rcx)
    /* Store the first VEC.  */
    VMOVU    %VEC(4), (%r11)
    VZEROUPPER
    ret

L(more_8x_vec_backward):
    /* Load the first 4 * VEC and last VEC to support overlapping
       addresses.  */
    VMOVU    (%rsi), %VEC(4)
    VMOVU    VEC_SIZE(%rsi), %VEC(5)
    VMOVU    (VEC_SIZE * 2)(%rsi), %VEC(6)
    VMOVU    (VEC_SIZE * 3)(%rsi), %VEC(7)
    VMOVU    -VEC_SIZE(%rsi,%rdx), %VEC(8)
    /* Save stop of the destination buffer.  */
    leaq    -VEC_SIZE(%rdi, %rdx), %r11
    /* Align destination end for aligned stores in the loop.  Compute
       how much destination end is misaligned.  */
    leaq    -VEC_SIZE(%rsi, %rdx), %rcx
    movq    %r11, %r9
    movq    %r11, %r8
    andq    $(VEC_SIZE - 1), %r8
    /* Adjust source.  */
    subq    %r8, %rcx
    /* Adjust the end of destination which should be aligned now.  */
    subq    %r8, %r9
    /* Adjust length.  */
    subq    %r8, %rdx
#if (defined USE_MULTIARCH || VEC_SIZE == 16)
    /* Check non-temporal store threshold.  */
    cmp    $SHARED_NON_TEMPORAL_THRESHOLD, %RDX_LP
    ja    L(large_backward)
#endif
L(loop_4x_vec_backward):
    /* Copy 4 * VEC a time backward.  */
    VMOVU    (%rcx), %VEC(0)
    VMOVU    -VEC_SIZE(%rcx), %VEC(1)
    VMOVU    -(VEC_SIZE * 2)(%rcx), %VEC(2)
    VMOVU    -(VEC_SIZE * 3)(%rcx), %VEC(3)
    subq    $(VEC_SIZE * 4), %rcx
    subq    $(VEC_SIZE * 4), %rdx
    VMOVA    %VEC(0), (%r9)
    VMOVA    %VEC(1), -VEC_SIZE(%r9)
    VMOVA    %VEC(2), -(VEC_SIZE * 2)(%r9)
    VMOVA    %VEC(3), -(VEC_SIZE * 3)(%r9)
    subq    $(VEC_SIZE * 4), %r9
    cmpq    $(VEC_SIZE * 4), %rdx
    ja    L(loop_4x_vec_backward)
    /* Store the first 4 * VEC.  */
    VMOVU    %VEC(4), (%rdi)
    VMOVU    %VEC(5), VEC_SIZE(%rdi)
    VMOVU    %VEC(6), (VEC_SIZE * 2)(%rdi)
    VMOVU    %VEC(7), (VEC_SIZE * 3)(%rdi)
    /* Store the last VEC.  */
    VMOVU    %VEC(8), (%r11)
    VZEROUPPER
    ret

#if (defined USE_MULTIARCH || VEC_SIZE == 16)
L(large_forward):
    /* Don't use non-temporal store if there is overlap between
       destination and source since destination may be in cache
       when source is loaded.  */
    leaq    (%rdi, %rdx), %r10
    cmpq    %r10, %rsi
    jb    L(loop_4x_vec_forward)
L(loop_large_forward):
    /* Copy 4 * VEC a time forward with non-temporal stores.  */
    PREFETCH_ONE_SET (1, (%rsi), PREFETCHED_LOAD_SIZE * 2)
    PREFETCH_ONE_SET (1, (%rsi), PREFETCHED_LOAD_SIZE * 3)
    VMOVU    (%rsi), %VEC(0)
    VMOVU    VEC_SIZE(%rsi), %VEC(1)
    VMOVU    (VEC_SIZE * 2)(%rsi), %VEC(2)
    VMOVU    (VEC_SIZE * 3)(%rsi), %VEC(3)
    addq    $PREFETCHED_LOAD_SIZE, %rsi
    subq    $PREFETCHED_LOAD_SIZE, %rdx
    VMOVNT    %VEC(0), (%rdi)
    VMOVNT    %VEC(1), VEC_SIZE(%rdi)
    VMOVNT    %VEC(2), (VEC_SIZE * 2)(%rdi)
    VMOVNT    %VEC(3), (VEC_SIZE * 3)(%rdi)
    addq    $PREFETCHED_LOAD_SIZE, %rdi
    cmpq    $PREFETCHED_LOAD_SIZE, %rdx
    ja    L(loop_large_forward)
    sfence
    /* Store the last 4 * VEC.  */
    VMOVU    %VEC(5), (%rcx)
    VMOVU    %VEC(6), -VEC_SIZE(%rcx)
    VMOVU    %VEC(7), -(VEC_SIZE * 2)(%rcx)
    VMOVU    %VEC(8), -(VEC_SIZE * 3)(%rcx)
    /* Store the first VEC.  */
    VMOVU    %VEC(4), (%r11)
    VZEROUPPER
    ret

L(large_backward):
    /* Don't use non-temporal store if there is overlap between
       destination and source since destination may be in cache
       when source is loaded.  */
    leaq    (%rcx, %rdx), %r10
    cmpq    %r10, %r9
    jb    L(loop_4x_vec_backward)
L(loop_large_backward):
    /* Copy 4 * VEC a time backward with non-temporal stores.  */
    PREFETCH_ONE_SET (-1, (%rcx), -PREFETCHED_LOAD_SIZE * 2)
    PREFETCH_ONE_SET (-1, (%rcx), -PREFETCHED_LOAD_SIZE * 3)
    VMOVU    (%rcx), %VEC(0)
    VMOVU    -VEC_SIZE(%rcx), %VEC(1)
    VMOVU    -(VEC_SIZE * 2)(%rcx), %VEC(2)
    VMOVU    -(VEC_SIZE * 3)(%rcx), %VEC(3)
    subq    $PREFETCHED_LOAD_SIZE, %rcx
    subq    $PREFETCHED_LOAD_SIZE, %rdx
    VMOVNT    %VEC(0), (%r9)
    VMOVNT    %VEC(1), -VEC_SIZE(%r9)
    VMOVNT    %VEC(2), -(VEC_SIZE * 2)(%r9)
    VMOVNT    %VEC(3), -(VEC_SIZE * 3)(%r9)
    subq    $PREFETCHED_LOAD_SIZE, %r9
    cmpq    $PREFETCHED_LOAD_SIZE, %rdx
    ja    L(loop_large_backward)
    sfence
    /* Store the first 4 * VEC.  */
    VMOVU    %VEC(4), (%rdi)
    VMOVU    %VEC(5), VEC_SIZE(%rdi)
    VMOVU    %VEC(6), (VEC_SIZE * 2)(%rdi)
    VMOVU    %VEC(7), (VEC_SIZE * 3)(%rdi)
    /* Store the last VEC.  */
    VMOVU    %VEC(8), (%r11)
    VZEROUPPER
    ret
#endif
END (MEMMOVE_SYMBOL (__memmove, unaligned_erms))

#if 1
# ifdef USE_MULTIARCH
strong_alias (MEMMOVE_SYMBOL (__memmove, unaligned_erms),
          MEMMOVE_SYMBOL (__memcpy, unaligned_erms))
#  ifdef SHARED
strong_alias (MEMMOVE_SYMBOL (__memmove_chk, unaligned_erms),
          MEMMOVE_SYMBOL (__memcpy_chk, unaligned_erms))
#  endif
# endif
# ifdef SHARED
strong_alias (MEMMOVE_CHK_SYMBOL (__memmove_chk, unaligned),
          MEMMOVE_CHK_SYMBOL (__memcpy_chk, unaligned))
# endif
#endif
strong_alias (MEMMOVE_SYMBOL (__memmove, unaligned),
          MEMCPY_SYMBOL (__memcpy, unaligned))
