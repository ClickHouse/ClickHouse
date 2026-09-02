#if 1
# define VEC_SIZE    64
# define VEC(i)        zmm##i
# define VMOVNT        vmovntdq
# define VMOVU        vmovdqu64
# define VMOVA        vmovdqa64

# define SECTION(p)        p##.avx512
# define MEMMOVE_SYMBOL(p,s)    p##_avx512_##s

# include "memmove-vec-unaligned-erms.S"
#endif
