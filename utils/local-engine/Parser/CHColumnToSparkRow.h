#pragma once
#include <vector>
#include <Core/Block.h>
#include <Common/Allocator.h>

namespace local_engine
{

class CHColumnToSparkRow;

class SparkRowInfo
{
    friend CHColumnToSparkRow;
public:
    SparkRowInfo(DB::Block& block);
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
    int64_t total_bytes_;
    int64_t nullBitsetWidthInBytes_;
    int64_t num_cols_;
    int64_t num_rows_;
    std::vector<int64_t> buffer_cursor_;
    uint8_t * buffer_address_;
    std::vector<int64_t> offsets_;
    std::vector<int64_t> lengths_;
};

using SparkRowInfoPtr = std::unique_ptr<local_engine::SparkRowInfo>;

class CHColumnToSparkRow : private Allocator<false>
{
public:
    std::unique_ptr<SparkRowInfo> convertCHColumnToSparkRow(DB::Block & block);
    void freeMem(uint8_t * address, size_t size);
};
}

