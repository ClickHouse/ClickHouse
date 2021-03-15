#pragma once

/* Generic asm macros used on many machines.
    Copyright (C) 1991-2020 Free Software Foundation, Inc.
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

#define C_SYMBOL_NAME(name) name
#define HIDDEN_JUMPTARGET(name) 0x0
#define SHARED_CACHE_SIZE_HALF (1024*1024)
#define DATA_CACHE_SIZE_HALF (1024*32/2)
#define DATA_CACHE_SIZE (1024*32)
#define SHARED_NON_TEMPORAL_THRESHOLD (1024*1024*4)
#define REP_MOSB_THRESHOLD 1024

#define USE_MULTIARCH

#define ASM_LINE_SEP ;

#define strong_alias(original, alias)                \
    .globl C_SYMBOL_NAME (alias) ASM_LINE_SEP        \
    C_SYMBOL_NAME (alias) = C_SYMBOL_NAME (original)

#ifndef C_LABEL

/* Define a macro we can use to construct the asm name for a C symbol.  */
# define C_LABEL(name)    name##:

#endif

#ifdef __ASSEMBLER__
/* Mark the end of function named SYM.  This is used on some platforms
    to generate correct debugging information.  */
# ifndef END
#  define END(sym)
# endif

# ifndef JUMPTARGET
#  define JUMPTARGET(sym)    sym
# endif
#endif

/* Macros to generate eh_frame unwind information.  */
#ifdef __ASSEMBLER__
# define cfi_startproc            .cfi_startproc
# define cfi_endproc            .cfi_endproc
# define cfi_def_cfa(reg, off)        .cfi_def_cfa reg, off
# define cfi_def_cfa_register(reg)    .cfi_def_cfa_register reg
# define cfi_def_cfa_offset(off)    .cfi_def_cfa_offset off
# define cfi_adjust_cfa_offset(off)    .cfi_adjust_cfa_offset off
# define cfi_offset(reg, off)        .cfi_offset reg, off
# define cfi_rel_offset(reg, off)    .cfi_rel_offset reg, off
# define cfi_register(r1, r2)        .cfi_register r1, r2
# define cfi_return_column(reg)    .cfi_return_column reg
# define cfi_restore(reg)        .cfi_restore reg
# define cfi_same_value(reg)        .cfi_same_value reg
# define cfi_undefined(reg)        .cfi_undefined reg
# define cfi_remember_state        .cfi_remember_state
# define cfi_restore_state        .cfi_restore_state
# define cfi_window_save        .cfi_window_save
# define cfi_personality(enc, exp)    .cfi_personality enc, exp
# define cfi_lsda(enc, exp)        .cfi_lsda enc, exp

#else /* ! ASSEMBLER */

# define CFI_STRINGIFY(Name) CFI_STRINGIFY2 (Name)
# define CFI_STRINGIFY2(Name) #Name
# define CFI_STARTPROC    ".cfi_startproc"
# define CFI_ENDPROC    ".cfi_endproc"
# define CFI_DEF_CFA(reg, off)    \
    ".cfi_def_cfa " CFI_STRINGIFY(reg) "," CFI_STRINGIFY(off)
# define CFI_DEF_CFA_REGISTER(reg) \
    ".cfi_def_cfa_register " CFI_STRINGIFY(reg)
# define CFI_DEF_CFA_OFFSET(off) \
    ".cfi_def_cfa_offset " CFI_STRINGIFY(off)
# define CFI_ADJUST_CFA_OFFSET(off) \
    ".cfi_adjust_cfa_offset " CFI_STRINGIFY(off)
# define CFI_OFFSET(reg, off) \
    ".cfi_offset " CFI_STRINGIFY(reg) "," CFI_STRINGIFY(off)
# define CFI_REL_OFFSET(reg, off) \
    ".cfi_rel_offset " CFI_STRINGIFY(reg) "," CFI_STRINGIFY(off)
# define CFI_REGISTER(r1, r2) \
    ".cfi_register " CFI_STRINGIFY(r1) "," CFI_STRINGIFY(r2)
# define CFI_RETURN_COLUMN(reg) \
    ".cfi_return_column " CFI_STRINGIFY(reg)
# define CFI_RESTORE(reg) \
    ".cfi_restore " CFI_STRINGIFY(reg)
# define CFI_UNDEFINED(reg) \
    ".cfi_undefined " CFI_STRINGIFY(reg)
# define CFI_REMEMBER_STATE \
    ".cfi_remember_state"
# define CFI_RESTORE_STATE \
    ".cfi_restore_state"
# define CFI_WINDOW_SAVE \
    ".cfi_window_save"
# define CFI_PERSONALITY(enc, exp) \
    ".cfi_personality " CFI_STRINGIFY(enc) "," CFI_STRINGIFY(exp)
# define CFI_LSDA(enc, exp) \
    ".cfi_lsda " CFI_STRINGIFY(enc) "," CFI_STRINGIFY(exp)
#endif

#include "dwarf2.h"
