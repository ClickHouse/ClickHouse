#pragma once

/* Definitions for x86 syntax variations.
    Copyright (C) 1992-2020 Free Software Foundation, Inc.
    This file is part of the GNU C Library.  Its master source is NOT part of
    the C library, however.  The master source lives in the GNU MP Library.

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

#undef ALIGN
#define ALIGN(log) .align 1<<log

#undef L
#define L(body) .L##body
