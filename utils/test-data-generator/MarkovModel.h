#pragma once

#include <Core/Types.h>
#include <Common/HashTable/HashMap.h>
#include <Common/Arena.h>
#include <ext/bit_cast.h>
#include <common/StringRef.h>


namespace DB
{


class MarkovModel
{
private:
    using NGramHash = UInt32;

    struct HistogramElement
    {
        UInt8 byte;
        UInt32 count;
    };

    struct Histogram
    {
        UInt32 total = 0;
        std::vector<HistogramElement> data;

        void add(UInt8 byte)
        {
            ++total;

            for (auto & elem : data)
            {
                if (elem.byte == byte)
                {
                    ++elem.count;
                    return;
                }
            }

            data.emplace_back(HistogramElement{.byte = byte, .count = 1});
        }

        UInt8 sample(UInt32 random) const
        {
            random %= total;

            UInt32 sum = 0;
            for (const auto & elem : data)
            {
                sum += elem.count;
                if (sum > random)
                    return elem.byte;
            }

            __builtin_unreachable();
        }
    };

    using Table = HashMap<NGramHash, Histogram, TrivialHash>;
    Table table;

    size_t n;


    NGramHash hashContext(const char * pos, const char * data, size_t size) const
    {
        if (pos >= data + n)
            return CRC32Hash()(StringRef(pos - n, n));
        else
            return CRC32Hash()(StringRef(data, pos - data));
    }

public:
    explicit MarkovModel(size_t n_) : n(n_) {}
    MarkovModel() {}

    void consume(const char * data, size_t size)
    {
        const char * pos = data;
        const char * end = data + size;

        while (pos < end)
        {
            table[hashContext(pos, data, size)].add(*pos);
            ++pos;
        }

        /// Mark end of string as zero byte.
        table[hashContext(pos, data, size)].add(0);
    }


    template <typename Random>
    size_t generate(char * data, size_t size, Random && random) const
    {
        char * pos = data;
        char * end = data + size;

        while (pos < end)
        {
            auto it = table.find(hashContext(pos, data, size));
            if (table.end() == it)
                return pos - data;

            *pos = it->second.sample(random());

            /// Zero byte marks end of string.
            if (0 == *pos)
                return pos - data;

            ++pos;
        }

        return size;
    }


    /// Allows to add random noise to frequencies.
    template <typename Transform>
    void modifyCounts(Transform && transform)
    {
        for (auto & elem : table)
        {
            UInt32 new_total = 0;
            for (auto & frequency : elem.second.data)
            {
                frequency.count = transform(frequency.count);
                new_total += frequency.count;
            }
            elem.second.total = new_total;
        }
    }


    void write(WriteBuffer & out) const
    {
        writeBinary(UInt8(n), out);
        writeVarUInt(table.size(), out);

        for (const auto & elem : table)
        {
            writeBinary(elem.first, out);
            writeBinary(UInt8(elem.second.data.size()), out);

            for (const auto & frequency : elem.second.data)
            {
                writeBinary(frequency.byte, out);
                writeVarUInt(frequency.count, out);
            }
        }
    }


    void read(ReadBuffer & in)
    {
        table.clear();

        UInt8 read_n = 0;
        readBinary(read_n, in);
        n = read_n;

        size_t read_size = 0;
        readVarUInt(read_size, in);

        for (size_t i = 0; i < read_size; ++i)
        {
            NGramHash key = 0;
            UInt8 historgam_size = 0;
            readBinary(key, in);
            readBinary(historgam_size, in);

            Histogram & histogram = table[key];
            histogram.data.resize(historgam_size);

            for (size_t j = 0; j < historgam_size; ++j)
            {
                readBinary(histogram.data[j].byte, in);
                readVarUInt(histogram.data[j].count, in);
                histogram.total += histogram.data[j].count;
            }
        }
    }
};

}
