/// This file can be included multiple times with values of the following macros predefined:
/// NAME_PART - to use in function names
/// VEC_SIZE = 16, 32, 64
/// VZEROUPPER = 0, 1
/// UNROLL_COUNT - how many times to unroll the main loop

#if !defined(VEC_SIZE)  /// This is only for readability. NAME_PART macro should be defined before including this file.
    #define VEC_SIZE 32
    #define VZEROUPPER 1
    #define UNROLL_COUNT 4
#endif

#if VEC_SIZE == 16
    #define NAME_PART sse
    #define VEC_REGISTER "xmm"
    #define VEC_MOV_UNALIGNED "movdqu"
    #define VEC_MOV_ALIGNED "movdqa"
    #define VEC_SIZE_MINUS_1 "0x0f"
    #define VEC_SIZEx1 "0x10"
    #define VEC_SIZEx2 "0x20"
    #define VEC_SIZEx3 "0x30"
    #define VEC_SIZEx4 "0x40"
    #define VEC_SIZEx5 "0x50"
    #define VEC_SIZEx6 "0x60"
    #define VEC_SIZEx7 "0x70"
    #define VEC_SIZEx8 "0x80"
    #define VEC_SIZEx9 "0x90"
    #define VEC_SIZEx10 "0xa0"
    #define VEC_SIZEx11 "0xb0"
    #define VEC_SIZEx12 "0xc0"
    #define VEC_SIZEx13 "0xd0"
    #define VEC_SIZEx14 "0xe0"
    #define VEC_SIZEx15 "0xf0"
    #define VEC_SIZEx16 "0x100"
#elif VEC_SIZE == 32
    #define NAME_PART avx
    #define VEC_REGISTER "ymm"
    #define VEC_MOV_UNALIGNED "vmovdqu"
    #define VEC_MOV_ALIGNED "vmovdqa"
    #define VEC_SIZE_MINUS_1 "0x1f"
    #define VEC_SIZEx1 "0x20"
    #define VEC_SIZEx2 "0x40"
    #define VEC_SIZEx3 "0x60"
    #define VEC_SIZEx4 "0x80"
    #define VEC_SIZEx5 "0xa0"
    #define VEC_SIZEx6 "0xc0"
    #define VEC_SIZEx7 "0xe0"
    #define VEC_SIZEx8 "0x100"
    #define VEC_SIZEx9 "0x120"
    #define VEC_SIZEx10 "0x140"
    #define VEC_SIZEx11 "0x160"
    #define VEC_SIZEx12 "0x180"
    #define VEC_SIZEx13 "0x1a0"
    #define VEC_SIZEx14 "0x1c0"
    #define VEC_SIZEx15 "0x1e0"
    #define VEC_SIZEx16 "0x200"
#elif VEC_SIZE == 64
    #define NAME_PART avx512
    #define VEC_REGISTER "zmm"
    #define VEC_MOV_UNALIGNED "vmovdqu64"
    #define VEC_MOV_ALIGNED "vmovdqa64"
    #define VEC_SIZE_MINUS_1 "0x3f"
    #define VEC_SIZEx1 "0x40"
    #define VEC_SIZEx2 "0x80"
    #define VEC_SIZEx3 "0xc0"
    #define VEC_SIZEx4 "0x100"
    #define VEC_SIZEx5 "0x140"
    #define VEC_SIZEx6 "0x180"
    #define VEC_SIZEx7 "0x1c0"
    #define VEC_SIZEx8 "0x200"
    #define VEC_SIZEx9 "0x240"
    #define VEC_SIZEx10 "0x280"
    #define VEC_SIZEx11 "0x2c0"
    #define VEC_SIZEx12 "0x300"
    #define VEC_SIZEx13 "0x340"
    #define VEC_SIZEx14 "0x380"
    #define VEC_SIZEx15 "0x3c0"
    #define VEC_SIZEx16 "0x400"
#endif

#if VZEROUPPER
    #define VZEROUPPER_INSTRUCTION "vzeroupper\n"
#else
    #define VZEROUPPER_INSTRUCTION
#endif

#define CONCAT_INTERNAL(A, B) A ## B
#define CONCAT(A, B) CONCAT_INTERNAL(A, B)

#define NAME_FORWARD_UNROLLED CONCAT(CONCAT(CONCAT(memcpy_medium_forward_unrolled, UNROLL_COUNT), _), NAME_PART)
#define NAME_BACKWARD_UNROLLED CONCAT(CONCAT(CONCAT(memcpy_medium_backward_unrolled, UNROLL_COUNT), _), NAME_PART)
#define NAME_TWOWAY_UNROLLED CONCAT(CONCAT(CONCAT(memcpy_medium_twoway_unrolled, UNROLL_COUNT), _), NAME_PART)

#if UNROLL_COUNT == 1
    #define VEC_SIZExUNROLL VEC_SIZEx1
#elif UNROLL_COUNT == 2
    #define VEC_SIZExUNROLL VEC_SIZEx2
#elif UNROLL_COUNT == 3
    #define VEC_SIZExUNROLL VEC_SIZEx3
#elif UNROLL_COUNT == 4
    #define VEC_SIZExUNROLL VEC_SIZEx4
#elif UNROLL_COUNT == 5
    #define VEC_SIZExUNROLL VEC_SIZEx5
#elif UNROLL_COUNT == 6
    #define VEC_SIZExUNROLL VEC_SIZEx6
#elif UNROLL_COUNT == 7
    #define VEC_SIZExUNROLL VEC_SIZEx7
#elif UNROLL_COUNT == 8
    #define VEC_SIZExUNROLL VEC_SIZEx8
#elif UNROLL_COUNT == 9
    #define VEC_SIZExUNROLL VEC_SIZEx9
#elif UNROLL_COUNT == 10
    #define VEC_SIZExUNROLL VEC_SIZEx10
#elif UNROLL_COUNT == 11
    #define VEC_SIZExUNROLL VEC_SIZEx11
#elif UNROLL_COUNT == 12
    #define VEC_SIZExUNROLL VEC_SIZEx12
#elif UNROLL_COUNT == 13
    #define VEC_SIZExUNROLL VEC_SIZEx13
#elif UNROLL_COUNT == 14
    #define VEC_SIZExUNROLL VEC_SIZEx14
#elif UNROLL_COUNT == 15
    #define VEC_SIZExUNROLL VEC_SIZEx15
#elif UNROLL_COUNT == 16
    #define VEC_SIZExUNROLL VEC_SIZEx16
#endif



void * NAME_FORWARD_UNROLLED(void * __restrict destination, const void * __restrict source, size_t size)
{
    void * __restrict ret = destination;

    __asm__ __volatile__ (
    "mov %[dst], %[ret] \n"

    VEC_MOV_UNALIGNED " (%[src]), %%" VEC_REGISTER "15 \n"
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "15, (%[dst]) \n"

    "lea    -" VEC_SIZEx1 "(%[dst],%[size],1), %%rcx \n"
    "mov    %[dst], %%r8 \n"
    "and    $" VEC_SIZE_MINUS_1 ", %%r8 \n"
    "sub    $" VEC_SIZEx1 ", %%r8 \n"
    "sub    %%r8, %[src] \n"
    "sub    %%r8, %[dst] \n"
    "add    %%r8, %[size] \n"

"1: \n"
    VEC_MOV_UNALIGNED " (%[src]), %%" VEC_REGISTER "0 \n"
#if UNROLL_COUNT >= 2
    VEC_MOV_UNALIGNED " " VEC_SIZEx1 "(%[src]), %%" VEC_REGISTER "1 \n"
#endif
#if UNROLL_COUNT >= 3
    VEC_MOV_UNALIGNED " " VEC_SIZEx2 "(%[src]), %%" VEC_REGISTER "2 \n"
#endif
#if UNROLL_COUNT >= 4
    VEC_MOV_UNALIGNED " " VEC_SIZEx3 "(%[src]), %%" VEC_REGISTER "3 \n"
#endif
#if UNROLL_COUNT >= 5
    VEC_MOV_UNALIGNED " " VEC_SIZEx4 "(%[src]), %%" VEC_REGISTER "4 \n"
#endif
#if UNROLL_COUNT >= 6
    VEC_MOV_UNALIGNED " " VEC_SIZEx5 "(%[src]), %%" VEC_REGISTER "5 \n"
#endif
#if UNROLL_COUNT >= 7
    VEC_MOV_UNALIGNED " " VEC_SIZEx6 "(%[src]), %%" VEC_REGISTER "6 \n"
#endif
#if UNROLL_COUNT >= 8
    VEC_MOV_UNALIGNED " " VEC_SIZEx7 "(%[src]), %%" VEC_REGISTER "7 \n"
#endif
#if UNROLL_COUNT >= 9
    VEC_MOV_UNALIGNED " " VEC_SIZEx8 "(%[src]), %%" VEC_REGISTER "8 \n"
#endif
#if UNROLL_COUNT >= 10
    VEC_MOV_UNALIGNED " " VEC_SIZEx9 "(%[src]), %%" VEC_REGISTER "9 \n"
#endif
#if UNROLL_COUNT >= 11
    VEC_MOV_UNALIGNED " " VEC_SIZEx10 "(%[src]), %%" VEC_REGISTER "10 \n"
#endif
#if UNROLL_COUNT >= 12
    VEC_MOV_UNALIGNED " " VEC_SIZEx11 "(%[src]), %%" VEC_REGISTER "11 \n"
#endif
#if UNROLL_COUNT >= 13
    VEC_MOV_UNALIGNED " " VEC_SIZEx12 "(%[src]), %%" VEC_REGISTER "12 \n"
#endif
#if UNROLL_COUNT >= 14
    VEC_MOV_UNALIGNED " " VEC_SIZEx13 "(%[src]), %%" VEC_REGISTER "13 \n"
#endif
#if UNROLL_COUNT >= 15
    VEC_MOV_UNALIGNED " " VEC_SIZEx14 "(%[src]), %%" VEC_REGISTER "14 \n"
#endif
#if UNROLL_COUNT >= 16
    VEC_MOV_UNALIGNED " " VEC_SIZEx15 "(%[src]), %%" VEC_REGISTER "15 \n"
#endif

    "add    $" VEC_SIZExUNROLL ", %[src] \n"
    "sub    $" VEC_SIZExUNROLL ", %[size] \n"

    VEC_MOV_ALIGNED " %%" VEC_REGISTER "0, (%[dst]) \n"
#if UNROLL_COUNT >= 2
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "1, " VEC_SIZEx1 "(%[dst]) \n"
#endif
#if UNROLL_COUNT >= 3
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "2, " VEC_SIZEx2 "(%[dst]) \n"
#endif
#if UNROLL_COUNT >= 4
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "3, " VEC_SIZEx3 "(%[dst]) \n"
#endif
#if UNROLL_COUNT >= 5
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "4, " VEC_SIZEx4 "(%[dst]) \n"
#endif
#if UNROLL_COUNT >= 6
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "5, " VEC_SIZEx5 "(%[dst]) \n"
#endif
#if UNROLL_COUNT >= 7
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "6, " VEC_SIZEx6 "(%[dst]) \n"
#endif
#if UNROLL_COUNT >= 8
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "7, " VEC_SIZEx7 "(%[dst]) \n"
#endif
#if UNROLL_COUNT >= 9
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "8, " VEC_SIZEx8 "(%[dst]) \n"
#endif
#if UNROLL_COUNT >= 10
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "9, " VEC_SIZEx9 "(%[dst]) \n"
#endif
#if UNROLL_COUNT >= 11
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "10, " VEC_SIZEx10 "(%[dst]) \n"
#endif
#if UNROLL_COUNT >= 12
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "11, " VEC_SIZEx11 "(%[dst]) \n"
#endif
#if UNROLL_COUNT >= 13
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "12, " VEC_SIZEx12 "(%[dst]) \n"
#endif
#if UNROLL_COUNT >= 14
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "13, " VEC_SIZEx13 "(%[dst]) \n"
#endif
#if UNROLL_COUNT >= 15
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "14, " VEC_SIZEx14 "(%[dst]) \n"
#endif
#if UNROLL_COUNT >= 16
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "15, " VEC_SIZEx15 "(%[dst]) \n"
#endif

    "add    $" VEC_SIZExUNROLL ", %[dst] \n"
    "cmp    $" VEC_SIZExUNROLL ", %[size] \n"
    "ja     1b \n"

    VEC_MOV_UNALIGNED " -" VEC_SIZEx1 "(%[src],%[size],1), %%" VEC_REGISTER "0 \n"
#if UNROLL_COUNT >= 2
    VEC_MOV_UNALIGNED " -" VEC_SIZEx2 "(%[src],%[size],1), %%" VEC_REGISTER "1 \n"
#endif
#if UNROLL_COUNT >= 3
    VEC_MOV_UNALIGNED " -" VEC_SIZEx3 "(%[src],%[size],1), %%" VEC_REGISTER "2 \n"
#endif
#if UNROLL_COUNT >= 4
    VEC_MOV_UNALIGNED " -" VEC_SIZEx4 "(%[src],%[size],1), %%" VEC_REGISTER "3 \n"
#endif
#if UNROLL_COUNT >= 5
    VEC_MOV_UNALIGNED " -" VEC_SIZEx5 "(%[src],%[size],1), %%" VEC_REGISTER "4 \n"
#endif
#if UNROLL_COUNT >= 6
    VEC_MOV_UNALIGNED " -" VEC_SIZEx6 "(%[src],%[size],1), %%" VEC_REGISTER "5 \n"
#endif
#if UNROLL_COUNT >= 7
    VEC_MOV_UNALIGNED " -" VEC_SIZEx7 "(%[src],%[size],1), %%" VEC_REGISTER "6 \n"
#endif
#if UNROLL_COUNT >= 8
    VEC_MOV_UNALIGNED " -" VEC_SIZEx8 "(%[src],%[size],1), %%" VEC_REGISTER "7 \n"
#endif
#if UNROLL_COUNT >= 9
    VEC_MOV_UNALIGNED " -" VEC_SIZEx9 "(%[src],%[size],1), %%" VEC_REGISTER "8 \n"
#endif
#if UNROLL_COUNT >= 10
    VEC_MOV_UNALIGNED " -" VEC_SIZEx10 "(%[src],%[size],1), %%" VEC_REGISTER "9 \n"
#endif
#if UNROLL_COUNT >= 11
    VEC_MOV_UNALIGNED " -" VEC_SIZEx11 "(%[src],%[size],1), %%" VEC_REGISTER "10 \n"
#endif
#if UNROLL_COUNT >= 12
    VEC_MOV_UNALIGNED " -" VEC_SIZEx12 "(%[src],%[size],1), %%" VEC_REGISTER "11 \n"
#endif
#if UNROLL_COUNT >= 13
    VEC_MOV_UNALIGNED " -" VEC_SIZEx13 "(%[src],%[size],1), %%" VEC_REGISTER "12 \n"
#endif
#if UNROLL_COUNT >= 14
    VEC_MOV_UNALIGNED " -" VEC_SIZEx14 "(%[src],%[size],1), %%" VEC_REGISTER "13 \n"
#endif
#if UNROLL_COUNT >= 15
    VEC_MOV_UNALIGNED " -" VEC_SIZEx15 "(%[src],%[size],1), %%" VEC_REGISTER "14 \n"
#endif
#if UNROLL_COUNT >= 16
    VEC_MOV_UNALIGNED " -" VEC_SIZEx16 "(%[src],%[size],1), %%" VEC_REGISTER "15 \n"
#endif

    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "0, (%%rcx) \n"
#if UNROLL_COUNT >= 2
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "1, -" VEC_SIZEx1 "(%%rcx) \n"
#endif
#if UNROLL_COUNT >= 3
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "2, -" VEC_SIZEx2 "(%%rcx) \n"
#endif
#if UNROLL_COUNT >= 4
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "3, -" VEC_SIZEx3 "(%%rcx) \n"
#endif
#if UNROLL_COUNT >= 5
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "4, -" VEC_SIZEx4 "(%%rcx) \n"
#endif
#if UNROLL_COUNT >= 6
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "5, -" VEC_SIZEx5 "(%%rcx) \n"
#endif
#if UNROLL_COUNT >= 7
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "6, -" VEC_SIZEx6 "(%%rcx) \n"
#endif
#if UNROLL_COUNT >= 8
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "7, -" VEC_SIZEx7 "(%%rcx) \n"
#endif
#if UNROLL_COUNT >= 9
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "8, -" VEC_SIZEx8 "(%%rcx) \n"
#endif
#if UNROLL_COUNT >= 10
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "9, -" VEC_SIZEx9 "(%%rcx) \n"
#endif
#if UNROLL_COUNT >= 11
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "10, -" VEC_SIZEx10 "(%%rcx) \n"
#endif
#if UNROLL_COUNT >= 12
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "11, -" VEC_SIZEx11 "(%%rcx) \n"
#endif
#if UNROLL_COUNT >= 13
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "12, -" VEC_SIZEx12 "(%%rcx) \n"
#endif
#if UNROLL_COUNT >= 14
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "13, -" VEC_SIZEx13 "(%%rcx) \n"
#endif
#if UNROLL_COUNT >= 15
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "14, -" VEC_SIZEx14 "(%%rcx) \n"
#endif
#if UNROLL_COUNT >= 16
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "15, -" VEC_SIZEx15 "(%%rcx) \n"
#endif

    VZEROUPPER_INSTRUCTION

    : [dst]"+r"(destination), [src]"+r"(source), [size]"+r"(size), [ret]"=rax"(ret)
    :
    : "rcx", "r8", "r11",
      VEC_REGISTER "0", VEC_REGISTER "1", VEC_REGISTER "2", VEC_REGISTER "3",
      VEC_REGISTER "4", VEC_REGISTER "5", VEC_REGISTER "6", VEC_REGISTER "7",
      VEC_REGISTER "8", VEC_REGISTER "9", VEC_REGISTER "10", VEC_REGISTER "11",
      VEC_REGISTER "12", VEC_REGISTER "13", VEC_REGISTER "14", VEC_REGISTER "15",
      "memory");

    return ret;
}


void * NAME_BACKWARD_UNROLLED(void * __restrict destination, const void * __restrict source, size_t size)
{
    void * __restrict ret = destination;

    __asm__ __volatile__ (
    "mov %[dst], %[ret] \n"

    "lea    -" VEC_SIZEx1 "(%[dst],%[size],1), %%r11 \n"
    "lea    -" VEC_SIZEx1 "(%[src],%[size],1), %%rcx \n"

    VEC_MOV_UNALIGNED " (%%rcx), %%" VEC_REGISTER "15 \n"
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "15, (%%r11) \n"

    "mov    %%r11, %%r9 \n"
    "mov    %%r11, %%r8 \n"
    "and    $" VEC_SIZE_MINUS_1 ", %%r8 \n"
    "sub    %%r8, %%rcx \n"
    "sub    %%r8, %%r9 \n"
    "sub    %%r8, %[size] \n"

"1: \n"
    VEC_MOV_UNALIGNED " (%%rcx),%%" VEC_REGISTER "0 \n"
#if UNROLL_COUNT >= 2
    VEC_MOV_UNALIGNED " -" VEC_SIZEx1 "(%%rcx),%%" VEC_REGISTER "1 \n"
#endif
#if UNROLL_COUNT >= 3
    VEC_MOV_UNALIGNED " -" VEC_SIZEx2 "(%%rcx),%%" VEC_REGISTER "2 \n"
#endif
#if UNROLL_COUNT >= 4
    VEC_MOV_UNALIGNED " -" VEC_SIZEx3 "(%%rcx),%%" VEC_REGISTER "3 \n"
#endif
#if UNROLL_COUNT >= 5
    VEC_MOV_UNALIGNED " -" VEC_SIZEx4 "(%%rcx),%%" VEC_REGISTER "4 \n"
#endif
#if UNROLL_COUNT >= 6
    VEC_MOV_UNALIGNED " -" VEC_SIZEx5 "(%%rcx),%%" VEC_REGISTER "5 \n"
#endif
#if UNROLL_COUNT >= 7
    VEC_MOV_UNALIGNED " -" VEC_SIZEx6 "(%%rcx),%%" VEC_REGISTER "6 \n"
#endif
#if UNROLL_COUNT >= 8
    VEC_MOV_UNALIGNED " -" VEC_SIZEx7 "(%%rcx),%%" VEC_REGISTER "7 \n"
#endif
#if UNROLL_COUNT >= 9
    VEC_MOV_UNALIGNED " -" VEC_SIZEx8 "(%%rcx),%%" VEC_REGISTER "8 \n"
#endif
#if UNROLL_COUNT >= 10
    VEC_MOV_UNALIGNED " -" VEC_SIZEx9 "(%%rcx),%%" VEC_REGISTER "9 \n"
#endif
#if UNROLL_COUNT >= 11
    VEC_MOV_UNALIGNED " -" VEC_SIZEx10 "(%%rcx),%%" VEC_REGISTER "10 \n"
#endif
#if UNROLL_COUNT >= 12
    VEC_MOV_UNALIGNED " -" VEC_SIZEx11 "(%%rcx),%%" VEC_REGISTER "11 \n"
#endif
#if UNROLL_COUNT >= 13
    VEC_MOV_UNALIGNED " -" VEC_SIZEx12 "(%%rcx),%%" VEC_REGISTER "12 \n"
#endif
#if UNROLL_COUNT >= 14
    VEC_MOV_UNALIGNED " -" VEC_SIZEx13 "(%%rcx),%%" VEC_REGISTER "13 \n"
#endif
#if UNROLL_COUNT >= 15
    VEC_MOV_UNALIGNED " -" VEC_SIZEx14 "(%%rcx),%%" VEC_REGISTER "14 \n"
#endif
#if UNROLL_COUNT >= 16
    VEC_MOV_UNALIGNED " -" VEC_SIZEx15 "(%%rcx),%%" VEC_REGISTER "15 \n"
#endif

    "sub    $" VEC_SIZExUNROLL ",%%rcx \n"
    "sub    $" VEC_SIZExUNROLL ",%[size] \n"

    VEC_MOV_ALIGNED " %%" VEC_REGISTER "0, (%%r9) \n"
#if UNROLL_COUNT >= 2
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "1, -" VEC_SIZEx1 "(%%r9) \n"
#endif
#if UNROLL_COUNT >= 3
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "2, -" VEC_SIZEx2 "(%%r9) \n"
#endif
#if UNROLL_COUNT >= 4
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "3, -" VEC_SIZEx3 "(%%r9) \n"
#endif
#if UNROLL_COUNT >= 5
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "4, -" VEC_SIZEx4 "(%%r9) \n"
#endif
#if UNROLL_COUNT >= 6
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "5, -" VEC_SIZEx5 "(%%r9) \n"
#endif
#if UNROLL_COUNT >= 7
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "6, -" VEC_SIZEx6 "(%%r9) \n"
#endif
#if UNROLL_COUNT >= 8
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "7, -" VEC_SIZEx7 "(%%r9) \n"
#endif
#if UNROLL_COUNT >= 9
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "8, -" VEC_SIZEx8 "(%%r9) \n"
#endif
#if UNROLL_COUNT >= 10
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "9, -" VEC_SIZEx9 "(%%r9) \n"
#endif
#if UNROLL_COUNT >= 11
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "10, -" VEC_SIZEx10 "(%%r9) \n"
#endif
#if UNROLL_COUNT >= 12
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "11, -" VEC_SIZEx11 "(%%r9) \n"
#endif
#if UNROLL_COUNT >= 13
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "12, -" VEC_SIZEx12 "(%%r9) \n"
#endif
#if UNROLL_COUNT >= 14
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "13, -" VEC_SIZEx13 "(%%r9) \n"
#endif
#if UNROLL_COUNT >= 15
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "14, -" VEC_SIZEx14 "(%%r9) \n"
#endif
#if UNROLL_COUNT >= 16
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "15, -" VEC_SIZEx15 "(%%r9) \n"
#endif

    "sub    $" VEC_SIZExUNROLL ",%%r9 \n"
    "cmp    $" VEC_SIZExUNROLL ",%[size] \n"
    "ja     1b \n"

    VEC_MOV_UNALIGNED " (%[src]), %%" VEC_REGISTER "0 \n"
#if UNROLL_COUNT >= 2
    VEC_MOV_UNALIGNED " " VEC_SIZEx1 "(%[src]), %%" VEC_REGISTER "1 \n"
#endif
#if UNROLL_COUNT >= 3
    VEC_MOV_UNALIGNED " " VEC_SIZEx2 "(%[src]), %%" VEC_REGISTER "2 \n"
#endif
#if UNROLL_COUNT >= 4
    VEC_MOV_UNALIGNED " " VEC_SIZEx3 "(%[src]), %%" VEC_REGISTER "3 \n"
#endif
#if UNROLL_COUNT >= 5
    VEC_MOV_UNALIGNED " " VEC_SIZEx4 "(%[src]), %%" VEC_REGISTER "4 \n"
#endif
#if UNROLL_COUNT >= 6
    VEC_MOV_UNALIGNED " " VEC_SIZEx5 "(%[src]), %%" VEC_REGISTER "5 \n"
#endif
#if UNROLL_COUNT >= 7
    VEC_MOV_UNALIGNED " " VEC_SIZEx6 "(%[src]), %%" VEC_REGISTER "6 \n"
#endif
#if UNROLL_COUNT >= 8
    VEC_MOV_UNALIGNED " " VEC_SIZEx7 "(%[src]), %%" VEC_REGISTER "7 \n"
#endif
#if UNROLL_COUNT >= 9
    VEC_MOV_UNALIGNED " " VEC_SIZEx8 "(%[src]), %%" VEC_REGISTER "8 \n"
#endif
#if UNROLL_COUNT >= 10
    VEC_MOV_UNALIGNED " " VEC_SIZEx9 "(%[src]), %%" VEC_REGISTER "9 \n"
#endif
#if UNROLL_COUNT >= 11
    VEC_MOV_UNALIGNED " " VEC_SIZEx10 "(%[src]), %%" VEC_REGISTER "10 \n"
#endif
#if UNROLL_COUNT >= 12
    VEC_MOV_UNALIGNED " " VEC_SIZEx11 "(%[src]), %%" VEC_REGISTER "11 \n"
#endif
#if UNROLL_COUNT >= 13
    VEC_MOV_UNALIGNED " " VEC_SIZEx12 "(%[src]), %%" VEC_REGISTER "12 \n"
#endif
#if UNROLL_COUNT >= 14
    VEC_MOV_UNALIGNED " " VEC_SIZEx13 "(%[src]), %%" VEC_REGISTER "13 \n"
#endif
#if UNROLL_COUNT >= 15
    VEC_MOV_UNALIGNED " " VEC_SIZEx14 "(%[src]), %%" VEC_REGISTER "14 \n"
#endif
#if UNROLL_COUNT >= 16
    VEC_MOV_UNALIGNED " " VEC_SIZEx15 "(%[src]), %%" VEC_REGISTER "15 \n"
#endif

    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "0, (%[dst]) \n"
#if UNROLL_COUNT >= 2
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "1, " VEC_SIZEx1 "(%[dst]) \n"
#endif
#if UNROLL_COUNT >= 3
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "2, " VEC_SIZEx2 "(%[dst]) \n"
#endif
#if UNROLL_COUNT >= 4
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "3, " VEC_SIZEx3 "(%[dst]) \n"
#endif
#if UNROLL_COUNT >= 5
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "4, " VEC_SIZEx4 "(%[dst]) \n"
#endif
#if UNROLL_COUNT >= 6
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "5, " VEC_SIZEx5 "(%[dst]) \n"
#endif
#if UNROLL_COUNT >= 7
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "6, " VEC_SIZEx6 "(%[dst]) \n"
#endif
#if UNROLL_COUNT >= 8
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "7, " VEC_SIZEx7 "(%[dst]) \n"
#endif
#if UNROLL_COUNT >= 9
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "8, " VEC_SIZEx8 "(%[dst]) \n"
#endif
#if UNROLL_COUNT >= 10
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "9, " VEC_SIZEx9 "(%[dst]) \n"
#endif
#if UNROLL_COUNT >= 11
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "10, " VEC_SIZEx10 "(%[dst]) \n"
#endif
#if UNROLL_COUNT >= 12
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "11, " VEC_SIZEx11 "(%[dst]) \n"
#endif
#if UNROLL_COUNT >= 13
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "12, " VEC_SIZEx12 "(%[dst]) \n"
#endif
#if UNROLL_COUNT >= 14
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "13, " VEC_SIZEx13 "(%[dst]) \n"
#endif
#if UNROLL_COUNT >= 15
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "14, " VEC_SIZEx14 "(%[dst]) \n"
#endif
#if UNROLL_COUNT >= 16
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "15, " VEC_SIZEx15 "(%[dst]) \n"
#endif

    VZEROUPPER_INSTRUCTION

    : [dst]"+r"(destination), [src]"+r"(source), [size]"+r"(size), [ret]"=rax"(ret)
    :
    : "rcx", "r8", "r9", "r11",
      VEC_REGISTER "0", VEC_REGISTER "1", VEC_REGISTER "2", VEC_REGISTER "3",
      VEC_REGISTER "4", VEC_REGISTER "5", VEC_REGISTER "6", VEC_REGISTER "7",
      VEC_REGISTER "8", VEC_REGISTER "9", VEC_REGISTER "10", VEC_REGISTER "11",
      VEC_REGISTER "12", VEC_REGISTER "13", VEC_REGISTER "14", VEC_REGISTER "15",
      "memory");

    return ret;
}


void * NAME_TWOWAY_UNROLLED(void * __restrict destination, const void * __restrict source, size_t size)
{
    if (source < destination)
        return NAME_FORWARD_UNROLLED(destination, source, size);
    else
        return NAME_BACKWARD_UNROLLED(destination, source, size);
}


#undef NAME_PART
#undef VEC_REGISTER
#undef VEC_MOV_UNALIGNED
#undef VEC_MOV_ALIGNED
#undef VEC_SIZE_MINUS_1
#undef VEC_SIZEx1
#undef VEC_SIZEx2
#undef VEC_SIZEx3
#undef VEC_SIZEx4
#undef VEC_SIZEx5
#undef VEC_SIZEx6
#undef VEC_SIZEx7
#undef VEC_SIZEx8
#undef VEC_SIZEx9
#undef VEC_SIZEx10
#undef VEC_SIZEx11
#undef VEC_SIZEx12
#undef VEC_SIZEx13
#undef VEC_SIZEx14
#undef VEC_SIZEx15
#undef VEC_SIZEx16
#undef VEC_SIZExUNROLL
#undef VZEROUPPER_INSTRUCTION
#undef NAME_FORWARD
#undef NAME_BACKWARD
#undef NAME_TWOWAY
#undef CONCAT_INTERNAL
#undef CONCAT
