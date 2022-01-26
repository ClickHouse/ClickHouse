#ifndef _RLE_BITPACKING_HYBRID_ENCODER_H
#define _RLE_BITPACKING_HYBRID_ENCODER_H

#if defined(__cplusplus)
extern "C" {
#endif

#include <stdint.h>


extern int numberOfLeadingZeros(uint32_t v);


extern int getWidthFromMaxInt(uint32_t bound);


extern int writeUnsignedVarInt(uint32_t value, uint8_t *out);

extern uint32_t readUnsignedVarInt(uint8_t *out, int out_len);

extern int writeIntLittleEndianPaddedOnBitWidth(uint32_t v, int bitWidth, uint8_t *out);

extern uint32_t readIntLittleEndianPaddedOnBitWidth(uint8_t *out, int out_len);

typedef void BITPACKING_PACK8_FUNC(uint32_t *in, uint8_t *out);
typedef void BITPACKING_UNPACK8_FUNC(uint8_t *in, uint32_t *out);

extern void bitPackingbit0Pack8Values(uint32_t *in, uint8_t *out);
extern void bitPackingbit0unPack8Values(uint8_t *in, uint32_t *out);
extern void bitPackingbit1Pack8Values(uint32_t *in, uint8_t *out);
extern void bitPackingbit1unPack8Values(uint8_t *in, uint32_t *out);
extern void bitPackingbit2Pack8Values(uint32_t *in, uint8_t *out);
extern void bitPackingbit2unPack8Values(uint8_t *in, uint32_t *out);
extern void bitPackingbit3Pack8Values(uint32_t *in, uint8_t *out);
extern void bitPackingbit3unPack8Values(uint8_t *in, uint32_t *out);
extern void bitPackingbit4Pack8Values(uint32_t *in, uint8_t *out);
extern void bitPackingbit4unPack8Values(uint8_t *in, uint32_t *out);
extern void bitPackingbit5Pack8Values(uint32_t *in, uint8_t *out);
extern void bitPackingbit5unPack8Values(uint8_t *in, uint32_t *out);
extern void bitPackingbit6Pack8Values(uint32_t *in, uint8_t *out);
extern void bitPackingbit6unPack8Values(uint8_t *in, uint32_t *out);
extern void bitPackingbit7Pack8Values(uint32_t *in, uint8_t *out);
extern void bitPackingbit7unPack8Values(uint8_t *in, uint32_t *out);
extern void bitPackingbit8Pack8Values(uint32_t *in, uint8_t *out);
extern void bitPackingbit8unPack8Values(uint8_t *in, uint32_t *out);
extern void bitPackingbit9Pack8Values(uint32_t *in, uint8_t *out);
extern void bitPackingbit9unPack8Values(uint8_t *in, uint32_t *out);
extern void bitPackingbit10Pack8Values(uint32_t *in, uint8_t *out);
extern void bitPackingbit10unPack8Values(uint8_t *in, uint32_t *out);
extern void bitPackingbit11Pack8Values(uint32_t *in, uint8_t *out);
extern void bitPackingbit11unPack8Values(uint8_t *in, uint32_t *out);
extern void bitPackingbit12Pack8Values(uint32_t *in, uint8_t *out);
extern void bitPackingbit12unPack8Values(uint8_t *in, uint32_t *out);
extern void bitPackingbit13Pack8Values(uint32_t *in, uint8_t *out);
extern void bitPackingbit13unPack8Values(uint8_t *in, uint32_t *out);
extern void bitPackingbit14Pack8Values(uint32_t *in, uint8_t *out);
extern void bitPackingbit14unPack8Values(uint8_t *in, uint32_t *out);
extern void bitPackingbit15Pack8Values(uint32_t *in, uint8_t *out);
extern void bitPackingbit15unPack8Values(uint8_t *in, uint32_t *out);
extern void bitPackingbit16Pack8Values(uint32_t *in, uint8_t *out);
extern void bitPackingbit16unPack8Values(uint8_t *in, uint32_t *out);
extern void bitPackingbit17Pack8Values(uint32_t *in, uint8_t *out);
extern void bitPackingbit17unPack8Values(uint8_t *in, uint32_t *out);
extern void bitPackingbit18Pack8Values(uint32_t *in, uint8_t *out);
extern void bitPackingbit18unPack8Values(uint8_t *in, uint32_t *out);
extern void bitPackingbit19Pack8Values(uint32_t *in, uint8_t *out);
extern void bitPackingbit19unPack8Values(uint8_t *in, uint32_t *out);
extern void bitPackingbit20Pack8Values(uint32_t *in, uint8_t *out);
extern void bitPackingbit20unPack8Values(uint8_t *in, uint32_t *out);
extern void bitPackingbit21Pack8Values(uint32_t *in, uint8_t *out);
extern void bitPackingbit21unPack8Values(uint8_t *in, uint32_t *out);
extern void bitPackingbit22Pack8Values(uint32_t *in, uint8_t *out);
extern void bitPackingbit22unPack8Values(uint8_t *in, uint32_t *out);
extern void bitPackingbit23Pack8Values(uint32_t *in, uint8_t *out);
extern void bitPackingbit23unPack8Values(uint8_t *in, uint32_t *out);
extern void bitPackingbit24Pack8Values(uint32_t *in, uint8_t *out);
extern void bitPackingbit24unPack8Values(uint8_t *in, uint32_t *out);
extern void bitPackingbit25Pack8Values(uint32_t *in, uint8_t *out);
extern void bitPackingbit25unPack8Values(uint8_t *in, uint32_t *out);
extern void bitPackingbit26Pack8Values(uint32_t *in, uint8_t *out);
extern void bitPackingbit26unPack8Values(uint8_t *in, uint32_t *out);
extern void bitPackingbit27Pack8Values(uint32_t *in, uint8_t *out);
extern void bitPackingbit27unPack8Values(uint8_t *in, uint32_t *out);
extern void bitPackingbit28Pack8Values(uint32_t *in, uint8_t *out);
extern void bitPackingbit28unPack8Values(uint8_t *in, uint32_t *out);
extern void bitPackingbit29Pack8Values(uint32_t *in, uint8_t *out);
extern void bitPackingbit29unPack8Values(uint8_t *in, uint32_t *out);
extern void bitPackingbit30Pack8Values(uint32_t *in, uint8_t *out);
extern void bitPackingbit30unPack8Values(uint8_t *in, uint32_t *out);
extern void bitPackingbit31Pack8Values(uint32_t *in, uint8_t *out);
extern void bitPackingbit31unPack8Values(uint8_t *in, uint32_t *out);
extern void bitPackingbit32Pack8Values(uint32_t *in, uint8_t *out);
extern void bitPackingbit32unPack8Values(uint8_t *in, uint32_t *out);

extern BITPACKING_PACK8_FUNC *bitpacking_pack8_funcs[33];
extern BITPACKING_UNPACK8_FUNC *bitpacking_unpack8_funcs[33];

#if defined(__cplusplus)
}
#endif

#endif  /* _RLE_BITPACKING_HYBRID_ENCODER_H */

