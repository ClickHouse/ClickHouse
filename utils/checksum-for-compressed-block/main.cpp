#include <city.h>
#include <iostream>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <Common/hex.h>


/** A tool to easily prove if "Checksum doesn't match: corrupted data"
  *  errors are caused by random bit flips due to hardware issues.
  * It frequently happens due to bad memory on network switches
  *  (at least about a few times a year in a fleet of ~1200 ClickHouse servers).
  *
  * These hardware errors don't cause any data corruption issues in ClickHouse,
  *  because ClickHouse always validate it's own 128 bit checksums
  *  and report errors if checksum doesn't match.
  *
  * Client can simply retry the query.
  * But ops engineers want to validate if the issue is real hardware error or not.
  * If checksum difference is caused by single bit flip,
  *  we can be sure that this is hardware error, because random bit flips
  *  have low probability to happen due to software bugs.
  *
  * Usage:

echo -ne "\x82\x6b\x00\x00\x00\x62\x00\x00\x00\xf2\x3b\x01\x00\x02\xff\xff\xff\xff\x00\x01\x01\x2c\x75\x6e\x69\x71\x49\x66\x28\x44\x65\x76\x69\x63\x65\x49\x44\x48\x61\x73\x68\x2c\x20\x65\x71\x75\x61\x6c\x73\x28\x53\x65\x73\x73\x69\x6f\x6e\x54\x79\x70\x65\x2c\x20\x30\x29\x29\x28\x41\x67\x67\x72\x65\x67\x61\x74\x65\x46\x75\x6e\x63\x74\x69\x6f\x6e\x28\x3f\x00\xf0\x03\x2c\x20\x55\x49\x6e\x74\x36\x34\x2c\x20\x55\x49\x6e\x74\x38\x29\x00\x00" | ./checksum-for-compressed-block-find-bit-flips | grep 8b40502d2ffe5b712b52e03c505ca49f
8b40502d2ffe5b712b52e03c505ca49f        32, 6
  */


std::string flipBit(std::string s, size_t pos)
{
    s[pos / 8] ^= 1 << pos % 8;
    return s;
}


int main(int, char **)
{
    using namespace DB;
    ReadBufferFromFileDescriptor in(STDIN_FILENO);
    std::string str;
    readStringUntilEOF(str, in);

    for (size_t pos = 0; pos < str.size() * 8; ++pos)
    {
        auto flipped = flipBit(str, pos);
        auto checksum = CityHash_v1_0_2::CityHash128(flipped.data(), flipped.size());
        std::cout << getHexUIntLowercase(checksum.first) << getHexUIntLowercase(checksum.second) << "\t" << pos / 8 << ", " << pos % 8 << "\n";
    }

    return 0;
}
