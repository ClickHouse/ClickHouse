/*-*- mode:unix-assembly; indent-tabs-mode:t; tab-width:8; coding:utf-8     -*-│
│vi: set et ft=asm ts=8 tw=8 fenc=utf-8                                     :vi│
╞══════════════════════════════════════════════════════════════════════════════╡
│ Copyright 2020 Justine Alexandra Roberts Tunney                              │
│                                                                              │
│ Permission to use, copy, modify, and/or distribute this software for         │
│ any purpose with or without fee is hereby granted, provided that the         │
│ above copyright notice and this permission notice appear in all copies.      │
│                                                                              │
│ THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL                │
│ WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED                │
│ WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE             │
│ AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL         │
│ DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR        │
│ PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER               │
│ TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR             │
│ PERFORMANCE OF THIS SOFTWARE.                                                │
╚─────────────────────────────────────────────────────────────────────────────*/

//	Copies memory.
//
//	DEST and SRC must not overlap, unless DEST≤SRC.
//
//	@param	rdi is dest
//	@param	rsi is src
//	@param	rdx is number of bytes
//	@return	original rdi copied to rax
//	@mode	long
//	@asyncsignalsafe
memcpy_jart:	mov	%rdi,%rax
//	𝑠𝑙𝑖𝑑𝑒
	.align	16
	.type	memcpy_jart,@function
	.size	memcpy_jart,.-memcpy_jart
	.globl	memcpy_jart

//	Copies memory w/ minimal impact ABI.
//
//	@param	rdi is dest
//	@param	rsi is src
//	@param	rdx is number of bytes
//	@clob	flags,rcx,xmm3,xmm4
//	@mode	long
MemCpy:	mov	$.Lmemcpytab.size,%ecx
	cmp	%rcx,%rdx
	cmovb	%rdx,%rcx
	jmp	*memcpytab(,%rcx,8)
.Lanchorpoint:
.L16r:	cmp	$1024,%rdx
	jae	.Lerms
.L16:	movdqu	-16(%rsi,%rdx),%xmm4
	mov	$16,%rcx
0:	add	$16,%rcx
	movdqu	-32(%rsi,%rcx),%xmm3
	movdqu	%xmm3,-32(%rdi,%rcx)
	cmp	%rcx,%rdx
	ja	0b
	movdqu	%xmm4,-16(%rdi,%rdx)
	pxor	%xmm4,%xmm4
	pxor	%xmm3,%xmm3
	jmp	.L0
.L8:	push	%rbx
	mov	(%rsi),%rcx
	mov	-8(%rsi,%rdx),%rbx
	mov	%rcx,(%rdi)
	mov	%rbx,-8(%rdi,%rdx)
1:	pop	%rbx
.L0:	ret
.L4:	push	%rbx
	mov	(%rsi),%ecx
	mov	-4(%rsi,%rdx),%ebx
	mov	%ecx,(%rdi)
	mov	%ebx,-4(%rdi,%rdx)
	jmp	1b
.L3:	push	%rbx
	mov	(%rsi),%cx
	mov	-2(%rsi,%rdx),%bx
	mov	%cx,(%rdi)
	mov	%bx,-2(%rdi,%rdx)
	jmp	1b
.L2:	mov	(%rsi),%cx
	mov	%cx,(%rdi)
	jmp	.L0
.L1:	mov	(%rsi),%cl
	mov	%cl,(%rdi)
	jmp	.L0
.Lerms:	cmp	$1024*1024,%rdx
	ja	.Lnts
	push	%rdi
	push	%rsi
	mov	%rdx,%rcx
	rep movsb
	pop	%rsi
	pop	%rdi
	jmp	.L0
.Lnts:	movdqu	(%rsi),%xmm3
	movdqu	%xmm3,(%rdi)
	lea	16(%rdi),%rcx
	and	$-16,%rcx
	sub	%rdi,%rcx
	add	%rcx,%rdi
	add	%rcx,%rsi
	sub	%rcx,%rdx
	mov	$16,%rcx
0:	add	$16,%rcx
	movdqu	-32(%rsi,%rcx),%xmm3
	movntdq	%xmm3,-32(%rdi,%rcx)
	cmp	%rcx,%rdx
	ja	0b
	sfence
	movdqu	-16(%rsi,%rdx),%xmm3
	movdqu	%xmm3,-16(%rdi,%rdx)
	pxor	%xmm3,%xmm3
	jmp	.L0
	.type	MemCpy,@function
	.size	MemCpy,.-MemCpy
	.globl	MemCpy

	.section .rodata
	.align	8
memcpytab:
	.quad	.L0
	.quad	.L1
	.quad	.L2
	.quad	.L3
	.rept	4
	.quad	.L4
	.endr
	.rept	8
	.quad	.L8
	.endr
	.rept	16
	.quad	.L16
	.endr
	.equ	.Lmemcpytab.size,(.-memcpytab)/8
	.quad	.L16r # SSE + ERMS + NTS
	.type	memcpytab,@object
	.previous
