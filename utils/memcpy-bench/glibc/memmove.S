/* Optimized memmove for x86-64.
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

#include "sysdep.h"

#define VEC_SIZE    16
#define VEC(i)        xmm##i
#define PREFETCHNT    prefetchnta
#define VMOVNT        movntdq
/* Use movups and movaps for smaller code sizes.  */
#define VMOVU        movups
#define VMOVA        movaps

#define SECTION(p)        p

#ifdef USE_MULTIARCH
# if 0
#  define MEMCPY_SYMBOL(p,s)        memcpy
# endif
#else
# if defined SHARED
#  define MEMCPY_SYMBOL(p,s)        __memcpy
# else
#  define MEMCPY_SYMBOL(p,s)        memcpy
# endif
#endif
#if !defined USE_MULTIARCH
# define MEMPCPY_SYMBOL(p,s)        __mempcpy
#endif
#ifndef MEMMOVE_SYMBOL
# define MEMMOVE_CHK_SYMBOL(p,s)    p
# define MEMMOVE_SYMBOL(p,s)        memmove
#endif

#include "memmove-vec-unaligned-erms.S"

#ifndef USE_MULTIARCH
libc_hidden_builtin_def (memmove)
# if defined SHARED && IS_IN (libc)
strong_alias (memmove, __memcpy)
libc_hidden_ver (memmove, memcpy)
# endif
libc_hidden_def (__mempcpy)
weak_alias (__mempcpy, mempcpy)
libc_hidden_builtin_def (mempcpy)

# if defined SHARED && IS_IN (libc)
#  undef memcpy
#  include <shlib-compat.h>
versioned_symbol (libc, __memcpy, memcpy, GLIBC_2_14);

#  if SHLIB_COMPAT (libc, GLIBC_2_2_5, GLIBC_2_14)
compat_symbol (libc, memmove, memcpy, GLIBC_2_2_5);
#  endif
# endif
#endif
