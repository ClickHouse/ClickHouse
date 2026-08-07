/* memmove with SSE2.
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

#if 1
# define MEMMOVE_SYMBOL(p,s)    p##_sse2_##s
#else
weak_alias (__mempcpy, mempcpy)
#endif

#include "memmove.S"

#if defined SHARED
# include <shlib-compat.h>
# if SHLIB_COMPAT (libc, GLIBC_2_2_5, GLIBC_2_14)
/* Use __memmove_sse2_unaligned to support overlapping addresses.  */
compat_symbol (libc, __memmove_sse2_unaligned, memcpy, GLIBC_2_2_5);
# endif
#endif
