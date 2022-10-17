#pragma once
#include <vector>
#include <Core/Block.h>
#include <Common/Allocator.h>

namespace local_engine
{
int64_t calculateBitSetWidthInBytes(int32_t num_fields);

class CHColumnToSparkRow;
class SparkRowToCHColumn;

class SparkRowInfo
{
    friend CHColumnToSparkRow;
    friend SparkRowToCHColumn;

public:
    explicit SparkRowInfo(DB::Block & block);
    int64_t getNullBitsetWidthInBytes() const;
    void setNullBitsetWidthInBytes(int64_t nullBitsetWidthInBytes);
    int64_t getNumCols() const;
    void setNumCols(int64_t numCols);
    int64_t getNumRows() const;
    void setNumRows(int64_t numRows);
    unsigned char * getBufferAddress() const;
    void setBufferAddress(unsigned char * bufferAddress);
    const std::vector<int64_t> & getOffsets() const;
    const std::vector<int64_t> & getLengths() const;
    int64_t getTotalBytes() const;

private:
    int64_t total_bytes;
    int64_t null_bitset_width_in_bytes;
    int64_t num_cols;
    int64_t num_rows;
    std::vector<int64_t> buffer_cursor;
    uint8_t * buffer_address;
    std::vector<int64_t> offsets;
    std::vector<int64_t> lengths;
};

using SparkRowInfoPtr = std::unique_ptr<local_engine::SparkRowInfo>;

class CHColumnToSparkRow : private Allocator<false>
{
public:
    std::unique_ptr<SparkRowInfo> convertCHColumnToSparkRow(DB::Block & block);
    void freeMem(uint8_t * address, size_t size);
};
}
