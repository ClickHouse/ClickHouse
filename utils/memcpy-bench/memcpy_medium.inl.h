/// This file can be included multiple times with values of the following macros predefined:
/// NAME - to use in function names
/// VEC_SIZE = 16, 32, 64
/// VZEROUPPER = 0, 1

#ifndef NAME  /// This is only for readability. NAME macro should be defined before including this file.
    #define NAME avx
    #define VEC_SIZE 32
    #define VZEROUPPER 1
#endif

#if VEC_SIZE == 16
    #define VEC_REGISTER "xmm"
    #define VEC_MOV_UNALIGNED "movdqu"
    #define VEC_MOV_ALIGNED "movdqa"
    #define VEC_SIZE_MINUS_1 "0x0f"
    #define VEC_SIZEx1 "0x10"
    #define VEC_SIZEx2 "0x20"
    #define VEC_SIZEx3 "0x30"
    #define VEC_SIZEx4 "0x40"
#elif VEC_SIZE == 32
    #define VEC_REGISTER "ymm"
    #define VEC_MOV_UNALIGNED "vmovdqu"
    #define VEC_MOV_ALIGNED "vmovdqa"
    #define VEC_SIZE_MINUS_1 "0x1f"
    #define VEC_SIZEx1 "0x20"
    #define VEC_SIZEx2 "0x40"
    #define VEC_SIZEx3 "0x60"
    #define VEC_SIZEx4 "0x80"
#endif

#if VZEROUPPER
    #define VZEROUPPER_INSTRUCTION "vzeroupper\n"
#endif


#define NAME_FORWARD memcpy_medium_ ## NAME ## _forward
#define NAME_BACKWARD memcpy_medium_ ## NAME ## _backward
#define NAME_TWOWAY memcpy_medium_ ## NAME ## _twoway

void * NAME_FORWARD(void * __restrict destination, const void * __restrict source, size_t size)
{
    void * __restrict ret = destination;

    __asm__ __volatile__ (
    "mov %[dst], %[ret] \n"

    VEC_MOV_UNALIGNED " (%[src]), %%" VEC_REGISTER "4 \n"
    VEC_MOV_UNALIGNED " -" VEC_SIZEx1 "(%[src],%[size],1), %%" VEC_REGISTER "5 \n"
    VEC_MOV_UNALIGNED " -" VEC_SIZEx2 "(%[src],%[size],1), %%" VEC_REGISTER "6 \n"
    VEC_MOV_UNALIGNED " -" VEC_SIZEx3 "(%[src],%[size],1), %%" VEC_REGISTER "7 \n"
    VEC_MOV_UNALIGNED " -" VEC_SIZEx4 "(%[src],%[size],1), %%" VEC_REGISTER "8 \n"

    "lea    -" VEC_SIZEx1 "(%[dst],%[size],1), %%rcx \n"
    "mov    %[dst], %%r8 \n"
    "and    $" VEC_SIZE_MINUS_1 ", %%r8 \n"
    "sub    $" VEC_SIZEx1 ", %%r8 \n"
    "sub    %%r8, %[src] \n"
    "sub    %%r8, %[dst] \n"
    "add    %%r8, %[size] \n"

"1: \n"
    VEC_MOV_UNALIGNED " (%[src]), %%" VEC_REGISTER "0 \n"
    VEC_MOV_UNALIGNED " " VEC_SIZEx1 "(%[src]), %%" VEC_REGISTER "1 \n"
    VEC_MOV_UNALIGNED " " VEC_SIZEx2 "(%[src]), %%" VEC_REGISTER "2 \n"
    VEC_MOV_UNALIGNED " " VEC_SIZEx3 "(%[src]), %%" VEC_REGISTER "3 \n"
    "add    $" VEC_SIZEx4 ", %[src]"
    "sub    $" VEC_SIZEx4 ", %[size]"
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "0, (%[dst]) \n"
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "1, " VEC_SIZEx1 "(%[dst]) \n"
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "2, " VEC_SIZEx2 "(%[dst]) \n"
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "3, " VEC_SIZEx3 "(%[dst]) \n"
    "add    $" VEC_SIZEx4 ", %[dst]"
    "cmp    $" VEC_SIZEx4 ", %[size]"
    "ja     1b"

    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "5, (%%rcx) \n"
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "6, -" VEC_SIZEx1 "(%%rcx) \n"
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "7, -" VEC_SIZEx2 "(%%rcx) \n"
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "8, -" VEC_SIZEx3 "(%%rcx) \n"
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "4, (%[ret]) \n"

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


void * NAME_BACKWARD(void * __restrict destination, const void * __restrict source, size_t size)
{
    void * __restrict ret = destination;

    __asm__ __volatile__ (
    "mov %[dst], %[ret]"

    VEC_MOV_UNALIGNED " (%[src]), %%" VEC_REGISTER "4 \n"
    VEC_MOV_UNALIGNED " " VEC_SIZEx1 "(%[src]), %%" VEC_REGISTER "5 \n"
    VEC_MOV_UNALIGNED " " VEC_SIZEx2 "(%[src]), %%" VEC_REGISTER "6 \n"
    VEC_MOV_UNALIGNED " " VEC_SIZEx3 "(%[src]), %%" VEC_REGISTER "7 \n"
    VEC_MOV_UNALIGNED " -" VEC_SIZEx1 "(%[src],%[size],1), %%" VEC_REGISTER "8 \n"

    "lea    -" VEC_SIZEx1 "(%[dst],%[size],1), %%r11 \n"
    "lea    -" VEC_SIZEx1 "(%[src],%[size],1), %%rcx \n"
    "mov    %%r11, %%r9 \n"
    "mov    %%r11, %%r8 \n"
    "and    " VEC_SIZE_MINUS_1 ", %%r8 \n"
    "sub    %%r8, %%rcx \n"
    "sub    %%r8, %%r9 \n"
    "sub    %%r8, %[size] \n"

"1: \n"
    VEC_MOV_UNALIGNED " (%%rcx),%%" VEC_REGISTER "0 \n"
    VEC_MOV_UNALIGNED " -" VEC_SIZEx1 "(%%rcx),%%" VEC_REGISTER "1 \n"
    VEC_MOV_UNALIGNED " -" VEC_SIZEx2 "(%%rcx),%%" VEC_REGISTER "2 \n"
    VEC_MOV_UNALIGNED " -" VEC_SIZEx3 "(%%rcx),%%" VEC_REGISTER "3 \n"
    "sub    $" VEC_SIZEx4 ",%%rcx \n"
    "sub    $" VEC_SIZEx4 ",%[size] \n"
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "0, (%%r9) \n"
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "1, -" VEC_SIZEx1 "(%%r9) \n"
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "2, -" VEC_SIZEx2 "(%%r9) \n"
    VEC_MOV_ALIGNED " %%" VEC_REGISTER "3, -" VEC_SIZEx3 "(%%r9) \n"
    "sub    $" VEC_SIZEx4 ",%%r9 \n"
    "cmp    $" VEC_SIZEx4 ",%[size] \n"
    "ja     1b \n"

    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "4, (%[dst]) \n"
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "5, " VEC_SIZEx1 "(%[dst]) \n"
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "6, " VEC_SIZEx2 "(%[dst]) \n"
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "7, " VEC_SIZEx3 "(%[dst]) \n"
    VEC_MOV_UNALIGNED " %%" VEC_REGISTER "8, (%%r11) \n"

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


void * NAME_TWOWAY(void * __restrict destination, const void * __restrict source, size_t size)
{
    if (source < destination)
        return NAME_FORWARD(destination, source, size);
    else
        return NAME_BACKWARD(destination, source, size);
}


#undef VEC_REGISTER
#undef VEC_MOV_UNALIGNED
#undef VEC_MOV_ALIGNED
#undef VEC_SIZE_MINUS_1
#undef VEC_SIZEx1
#undef VEC_SIZEx2
#undef VEC_SIZEx3
#undef VEC_SIZEx4
#undef VZEROUPPER_INSTRUCTION
