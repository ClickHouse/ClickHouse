// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "column_reader.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <exception>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/buffer_builder.h"
#include "arrow/array.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_dict.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/chunked_array.h"
#include "arrow/type.h"
#include "arrow/util/bit_stream_utils.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_writer.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/compression.h"
#include "arrow/util/int_util_internal.h"
#include "arrow/util/logging.h"
#include "arrow/util/rle_encoding.h"
#include "parquet/column_page.h"
#include "Storages/ch_parquet/arrow/encoding.h"
#include "parquet/encryption/encryption_internal.h"
#include "parquet/encryption/internal_file_decryptor.h"
#include "parquet/level_comparison.h"
#include "parquet/level_conversion.h"
#include "parquet/properties.h"
#include "parquet/statistics.h"
#include "parquet/thrift_internal.h"  // IWYU pragma: keep
// Required after "arrow/util/int_util_internal.h" (for OPTIONAL)
#include "parquet/windows_compatibility.h"


#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Common/DateLUTImpl.h>
#include <base/types.h>
#include <Processors/Chunk.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnUnique.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/castColumn.h>
#include <Common/quoteString.h>
#include <algorithm>
#include <arrow/builder.h>
#include <arrow/array.h>


using arrow::MemoryPool;
using arrow::internal::AddWithOverflow;
using arrow::internal::checked_cast;
using arrow::internal::MultiplyWithOverflow;

namespace BitUtil = arrow::BitUtil;

namespace ch_parquet {
using namespace parquet;
using namespace DB;
namespace {
    inline bool HasSpacedValues(const ColumnDescriptor* descr) {
        if (descr->max_repetition_level() > 0) {
            // repeated+flat case
            return !descr->schema_node()->is_required();
        } else {
            // non-repeated+nested case
            // Find if a node forces nulls in the lowest level along the hierarchy
            const schema::Node* node = descr->schema_node().get();
            while (node) {
                if (node->is_optional()) {
                    return true;
                }
                node = node->parent();
            }
            return false;
        }
    }
}  // namespace

LevelDecoder::LevelDecoder() : num_values_remaining_(0) {}

LevelDecoder::~LevelDecoder() {}

int LevelDecoder::SetData(Encoding::type encoding, int16_t max_level,
                          int num_buffered_values, const uint8_t* data,
                          int32_t data_size) {
    max_level_ = max_level;
    int32_t num_bytes = 0;
    encoding_ = encoding;
    num_values_remaining_ = num_buffered_values;
    bit_width_ = BitUtil::Log2(max_level + 1);
    switch (encoding) {
        case Encoding::RLE: {
            if (data_size < 4) {
                throw ParquetException("Received invalid levels (corrupt data page?)");
            }
            num_bytes = ::arrow::util::SafeLoadAs<int32_t>(data);
            if (num_bytes < 0 || num_bytes > data_size - 4) {
                throw ParquetException("Received invalid number of bytes (corrupt data page?)");
            }
            const uint8_t* decoder_data = data + 4;
            if (!rle_decoder_) {
                rle_decoder_.reset(
                    new ::arrow::util::RleDecoder(decoder_data, num_bytes, bit_width_));
            } else {
                rle_decoder_->Reset(decoder_data, num_bytes, bit_width_);
            }
            return 4 + num_bytes;
        }
        case Encoding::BIT_PACKED: {
            int num_bits = 0;
            if (MultiplyWithOverflow(num_buffered_values, bit_width_, &num_bits)) {
                throw ParquetException(
                    "Number of buffered values too large (corrupt data page?)");
            }
            num_bytes = static_cast<int32_t>(BitUtil::BytesForBits(num_bits));
            if (num_bytes < 0 || num_bytes > data_size - 4) {
                throw ParquetException("Received invalid number of bytes (corrupt data page?)");
            }
            if (!bit_packed_decoder_) {
                bit_packed_decoder_.reset(new ::arrow::BitUtil::BitReader(data, num_bytes));
            } else {
                bit_packed_decoder_->Reset(data, num_bytes);
            }
            return num_bytes;
        }
        default:
            throw ParquetException("Unknown encoding type for levels.");
    }
    return -1;
}

void LevelDecoder::SetDataV2(int32_t num_bytes, int16_t max_level,
                             int num_buffered_values, const uint8_t* data) {
    max_level_ = max_level;
    // Repetition and definition levels always uses RLE encoding
    // in the DataPageV2 format.
    if (num_bytes < 0) {
        throw ParquetException("Invalid page header (corrupt data page?)");
    }
    encoding_ = Encoding::RLE;
    num_values_remaining_ = num_buffered_values;
    bit_width_ = BitUtil::Log2(max_level + 1);

    if (!rle_decoder_) {
        rle_decoder_.reset(new ::arrow::util::RleDecoder(data, num_bytes, bit_width_));
    } else {
        rle_decoder_->Reset(data, num_bytes, bit_width_);
    }
}

int LevelDecoder::Decode(int batch_size, int16_t* levels) {
    int num_decoded = 0;

    int num_values = std::min(num_values_remaining_, batch_size);
    if (encoding_ == Encoding::RLE) {
        num_decoded = rle_decoder_->GetBatch(levels, num_values);
    } else {
        num_decoded = bit_packed_decoder_->GetBatch(bit_width_, levels, num_values);
    }
    if (num_decoded > 0) {
        internal::MinMax min_max = internal::FindMinMax(levels, num_decoded);
        if (ARROW_PREDICT_FALSE(min_max.min < 0 || min_max.max > max_level_)) {
            std::stringstream ss;
            ss << "Malformed levels. min: " << min_max.min << " max: " << min_max.max
               << " out of range.  Max Level: " << max_level_;
            throw ParquetException(ss.str());
        }
    }
    num_values_remaining_ -= num_decoded;
    return num_decoded;
}

ReaderProperties default_reader_properties() {
    static ReaderProperties default_reader_properties;
    return default_reader_properties;
}

namespace {

    // Extracts encoded statistics from V1 and V2 data page headers
    template <typename H>
    EncodedStatistics ExtractStatsFromHeader(const H& header) {
        EncodedStatistics page_statistics;
        if (!header.__isset.statistics) {
            return page_statistics;
        }
        const format::Statistics& stats = header.statistics;
        if (stats.__isset.max) {
            page_statistics.set_max(stats.max);
        }
        if (stats.__isset.min) {
            page_statistics.set_min(stats.min);
        }
        if (stats.__isset.null_count) {
            page_statistics.set_null_count(stats.null_count);
        }
        if (stats.__isset.distinct_count) {
            page_statistics.set_distinct_count(stats.distinct_count);
        }
        return page_statistics;
    }

    // ----------------------------------------------------------------------
    // SerializedPageReader deserializes Thrift metadata and pages that have been
    // assembled in a serialized stream for storing in a Parquet files

    // This subclass delimits pages appearing in a serialized stream, each preceded
    // by a serialized Thrift format::PageHeader indicating the type of each page
    // and the page metadata.
    class SerializedPageReader : public PageReader {
    public:
        SerializedPageReader(std::shared_ptr<ArrowInputStream> stream, int64_t total_num_rows,
                             Compression::type codec, ::arrow::MemoryPool* pool,
                             const CryptoContext* crypto_ctx)
            : stream_(std::move(stream)),
            decompression_buffer_(AllocateBuffer(pool, 0)),
            page_ordinal_(0),
            seen_num_rows_(0),
            total_num_rows_(total_num_rows),
            decryption_buffer_(AllocateBuffer(pool, 0)) {
            if (crypto_ctx != nullptr) {
                crypto_ctx_ = *crypto_ctx;
                InitDecryption();
            }
            max_page_header_size_ = kDefaultMaxPageHeaderSize;
            decompressor_ = GetCodec(codec);
        }

        // Implement the PageReader interface
        std::shared_ptr<Page> NextPage() override;

        void set_max_page_header_size(uint32_t size) override { max_page_header_size_ = size; }

    private:
        void UpdateDecryption(const std::shared_ptr<Decryptor>& decryptor, int8_t module_type,
                              const std::string& page_aad);

        void InitDecryption();

        std::shared_ptr<Buffer> DecompressIfNeeded(std::shared_ptr<Buffer> page_buffer,
                                                   int compressed_len, int uncompressed_len,
                                                   int levels_byte_len = 0);

        std::shared_ptr<ArrowInputStream> stream_;

        format::PageHeader current_page_header_;
        std::shared_ptr<Page> current_page_;

        // Compression codec to use.
        std::unique_ptr<::arrow::util::Codec> decompressor_;
        std::shared_ptr<ResizableBuffer> decompression_buffer_;

        // The fields below are used for calculation of AAD (additional authenticated data)
        // suffix which is part of the Parquet Modular Encryption.
        // The AAD suffix for a parquet module is built internally by
        // concatenating different parts some of which include
        // the row group ordinal, column ordinal and page ordinal.
        // Please refer to the encryption specification for more details:
        // https://github.com/apache/parquet-format/blob/encryption/Encryption.md#44-additional-authenticated-data

        // The ordinal fields in the context below are used for AAD suffix calculation.
        CryptoContext crypto_ctx_;
        int16_t page_ordinal_;  // page ordinal does not count the dictionary page

        // Maximum allowed page size
        uint32_t max_page_header_size_;

        // Number of rows read in data pages so far
        int64_t seen_num_rows_;

        // Number of rows in all the data pages
        int64_t total_num_rows_;

        // data_page_aad_ and data_page_header_aad_ contain the AAD for data page and data page
        // header in a single column respectively.
        // While calculating AAD for different pages in a single column the pages AAD is
        // updated by only the page ordinal.
        std::string data_page_aad_;
        std::string data_page_header_aad_;
        // Encryption
        std::shared_ptr<ResizableBuffer> decryption_buffer_;
    };

    void SerializedPageReader::InitDecryption() {
        // Prepare the AAD for quick update later.
        if (crypto_ctx_.data_decryptor != nullptr) {
            DCHECK(!crypto_ctx_.data_decryptor->file_aad().empty());
            data_page_aad_ = encryption::CreateModuleAad(
                crypto_ctx_.data_decryptor->file_aad(), encryption::kDataPage,
                crypto_ctx_.row_group_ordinal, crypto_ctx_.column_ordinal, kNonPageOrdinal);
        }
        if (crypto_ctx_.meta_decryptor != nullptr) {
            DCHECK(!crypto_ctx_.meta_decryptor->file_aad().empty());
            data_page_header_aad_ = encryption::CreateModuleAad(
                crypto_ctx_.meta_decryptor->file_aad(), encryption::kDataPageHeader,
                crypto_ctx_.row_group_ordinal, crypto_ctx_.column_ordinal, kNonPageOrdinal);
        }
    }

    void SerializedPageReader::UpdateDecryption(const std::shared_ptr<Decryptor>& decryptor,
                                                int8_t module_type,
                                                const std::string& page_aad) {
        DCHECK(decryptor != nullptr);
        if (crypto_ctx_.start_decrypt_with_dictionary_page) {
            std::string aad = encryption::CreateModuleAad(
                decryptor->file_aad(), module_type, crypto_ctx_.row_group_ordinal,
                crypto_ctx_.column_ordinal, kNonPageOrdinal);
            decryptor->UpdateAad(aad);
        } else {
            encryption::QuickUpdatePageAad(page_aad, page_ordinal_);
            decryptor->UpdateAad(page_aad);
        }
    }

    std::shared_ptr<Page> SerializedPageReader::NextPage() {
        // Loop here because there may be unhandled page types that we skip until
        // finding a page that we do know what to do with

        while (seen_num_rows_ < total_num_rows_) {
            uint32_t header_size = 0;
            uint32_t allowed_page_size = kDefaultPageHeaderSize;

            // Page headers can be very large because of page statistics
            // We try to deserialize a larger buffer progressively
            // until a maximum allowed header limit
            while (true) {
                PARQUET_ASSIGN_OR_THROW(auto view, stream_->Peek(allowed_page_size));
                if (view.size() == 0) {
                    return std::shared_ptr<Page>(nullptr);
                }

                // This gets used, then set by DeserializeThriftMsg
                header_size = static_cast<uint32_t>(view.size());
                try {
                    if (crypto_ctx_.meta_decryptor != nullptr) {
                        UpdateDecryption(crypto_ctx_.meta_decryptor, encryption::kDictionaryPageHeader,
                                         data_page_header_aad_);
                    }
                    DeserializeThriftMsg(reinterpret_cast<const uint8_t*>(view.data()), &header_size,
                                         &current_page_header_, crypto_ctx_.meta_decryptor);
                    break;
                } catch (std::exception& e) {
                    // Failed to deserialize. Double the allowed page header size and try again
                    std::stringstream ss;
                    ss << e.what();
                    allowed_page_size *= 2;
                    if (allowed_page_size > max_page_header_size_) {
                        ss << "Deserializing page header failed.\n";
                        throw ParquetException(ss.str());
                    }
                }
            }
            // Advance the stream offset
            PARQUET_THROW_NOT_OK(stream_->Advance(header_size));

            int compressed_len = current_page_header_.compressed_page_size;
            int uncompressed_len = current_page_header_.uncompressed_page_size;
            if (compressed_len < 0 || uncompressed_len < 0) {
                throw ParquetException("Invalid page header");
            }

            if (crypto_ctx_.data_decryptor != nullptr) {
                UpdateDecryption(crypto_ctx_.data_decryptor, encryption::kDictionaryPage,
                                 data_page_aad_);
            }

            // Read the compressed data page.
            PARQUET_ASSIGN_OR_THROW(auto page_buffer, stream_->Read(compressed_len));
            if (page_buffer->size() != compressed_len) {
                std::stringstream ss;
                ss << "Page was smaller (" << page_buffer->size() << ") than expected ("
                   << compressed_len << ")";
                ParquetException::EofException(ss.str());
            }

            // Decrypt it if we need to
            if (crypto_ctx_.data_decryptor != nullptr) {
                PARQUET_THROW_NOT_OK(decryption_buffer_->Resize(
                    compressed_len - crypto_ctx_.data_decryptor->CiphertextSizeDelta(), false));
                compressed_len = crypto_ctx_.data_decryptor->Decrypt(
                    page_buffer->data(), compressed_len, decryption_buffer_->mutable_data());

                page_buffer = decryption_buffer_;
            }

            const PageType::type page_type = LoadEnumSafe(&current_page_header_.type);

            if (page_type == PageType::DICTIONARY_PAGE) {
                crypto_ctx_.start_decrypt_with_dictionary_page = false;
                const format::DictionaryPageHeader& dict_header =
                    current_page_header_.dictionary_page_header;

                bool is_sorted = dict_header.__isset.is_sorted ? dict_header.is_sorted : false;
                if (dict_header.num_values < 0) {
                    throw ParquetException("Invalid page header (negative number of values)");
                }

                // Uncompress if needed
                page_buffer =
                    DecompressIfNeeded(std::move(page_buffer), compressed_len, uncompressed_len);

                return std::make_shared<DictionaryPage>(page_buffer, dict_header.num_values,
                                                        LoadEnumSafe(&dict_header.encoding),
                                                        is_sorted);
            } else if (page_type == PageType::DATA_PAGE) {
                ++page_ordinal_;
                const format::DataPageHeader& header = current_page_header_.data_page_header;

                if (header.num_values < 0) {
                    throw ParquetException("Invalid page header (negative number of values)");
                }
                EncodedStatistics page_statistics = ExtractStatsFromHeader(header);
                seen_num_rows_ += header.num_values;

                // Uncompress if needed
                page_buffer =
                    DecompressIfNeeded(std::move(page_buffer), compressed_len, uncompressed_len);

                return std::make_shared<DataPageV1>(page_buffer, header.num_values,
                                                    LoadEnumSafe(&header.encoding),
                                                    LoadEnumSafe(&header.definition_level_encoding),
                                                    LoadEnumSafe(&header.repetition_level_encoding),
                                                    uncompressed_len, page_statistics);
            } else if (page_type == PageType::DATA_PAGE_V2) {
                ++page_ordinal_;
                const format::DataPageHeaderV2& header = current_page_header_.data_page_header_v2;

                if (header.num_values < 0) {
                    throw ParquetException("Invalid page header (negative number of values)");
                }
                if (header.definition_levels_byte_length < 0 ||
                    header.repetition_levels_byte_length < 0) {
                    throw ParquetException("Invalid page header (negative levels byte length)");
                }
                bool is_compressed = header.__isset.is_compressed ? header.is_compressed : false;
                EncodedStatistics page_statistics = ExtractStatsFromHeader(header);
                seen_num_rows_ += header.num_values;

                // Uncompress if needed
                int levels_byte_len;
                if (AddWithOverflow(header.definition_levels_byte_length,
                                    header.repetition_levels_byte_length, &levels_byte_len)) {
                    throw ParquetException("Levels size too large (corrupt file?)");
                }
                // DecompressIfNeeded doesn't take `is_compressed` into account as
                // it's page type-agnostic.
                if (is_compressed) {
                    page_buffer = DecompressIfNeeded(std::move(page_buffer), compressed_len,
                                                     uncompressed_len, levels_byte_len);
                }

                return std::make_shared<DataPageV2>(
                    page_buffer, header.num_values, header.num_nulls, header.num_rows,
                    LoadEnumSafe(&header.encoding), header.definition_levels_byte_length,
                    header.repetition_levels_byte_length, uncompressed_len, is_compressed,
                    page_statistics);
            } else {
                // We don't know what this page type is. We're allowed to skip non-data
                // pages.
                continue;
            }
        }
        return std::shared_ptr<Page>(nullptr);
    }

    std::shared_ptr<Buffer> SerializedPageReader::DecompressIfNeeded(
        std::shared_ptr<Buffer> page_buffer, int compressed_len, int uncompressed_len,
        int levels_byte_len) {
        if (decompressor_ == nullptr) {
            return page_buffer;
        }
        if (compressed_len < levels_byte_len || uncompressed_len < levels_byte_len) {
            throw ParquetException("Invalid page header");
        }

        // Grow the uncompressed buffer if we need to.
        if (uncompressed_len > static_cast<int>(decompression_buffer_->size())) {
            PARQUET_THROW_NOT_OK(decompression_buffer_->Resize(uncompressed_len, false));
        }

        if (levels_byte_len > 0) {
            // First copy the levels as-is
            uint8_t* decompressed = decompression_buffer_->mutable_data();
            memcpy(decompressed, page_buffer->data(), levels_byte_len);
        }

        // Decompress the values
        PARQUET_THROW_NOT_OK(decompressor_->Decompress(
            compressed_len - levels_byte_len, page_buffer->data() + levels_byte_len,
            uncompressed_len - levels_byte_len,
            decompression_buffer_->mutable_data() + levels_byte_len));

        return decompression_buffer_;
    }

}  // namespace
}

namespace parquet
{
    std::unique_ptr<PageReader> PageReader::Open(
        std::shared_ptr<ArrowInputStream> stream,
        int64_t total_num_rows,
        Compression::type codec,
        ::arrow::MemoryPool * pool,
        const CryptoContext * ctx)
    {
        return std::unique_ptr<PageReader>(new SerializedPageReader(std::move(stream), total_num_rows, codec, pool, ctx));
    }
}

namespace ch_parquet
{
using namespace parquet;
using namespace parquet::internal;

namespace {

    // ----------------------------------------------------------------------
    // Impl base class for TypedColumnReader and RecordReader

    // PLAIN_DICTIONARY is deprecated but used to be used as a dictionary index
    // encoding.
    static bool IsDictionaryIndexEncoding(const Encoding::type& e) {
        return e == Encoding::RLE_DICTIONARY || e == Encoding::PLAIN_DICTIONARY;
    }

    template <typename DType>
    class ColumnReaderImplBase {
    public:
        using T = typename DType::c_type;

        ColumnReaderImplBase(const ColumnDescriptor* descr, ::arrow::MemoryPool* pool)
            : descr_(descr),
            max_def_level_(descr->max_definition_level()),
            max_rep_level_(descr->max_repetition_level()),
            num_buffered_values_(0),
            num_decoded_values_(0),
            pool_(pool),
            current_decoder_(nullptr),
            current_encoding_(Encoding::UNKNOWN) {}

        virtual ~ColumnReaderImplBase() = default;

    protected:
        // Read up to batch_size values from the current data page into the
        // pre-allocated memory T*
        //
        // @returns: the number of values read into the out buffer
        int64_t ReadValues(int64_t batch_size, T* out) {
            int64_t num_decoded = current_decoder_->Decode(out, static_cast<int>(batch_size));
            return num_decoded;
        }

        // Read up to batch_size values from the current data page into the
        // pre-allocated memory T*, leaving spaces for null entries according
        // to the def_levels.
        //
        // @returns: the number of values read into the out buffer
        int64_t ReadValuesSpaced(int64_t batch_size, T* out, int64_t null_count,
                                 uint8_t* valid_bits, int64_t valid_bits_offset) {
            return current_decoder_->DecodeSpaced(out, static_cast<int>(batch_size),
                                                  static_cast<int>(null_count), valid_bits,
                                                  valid_bits_offset);
        }

        // Read multiple definition levels into preallocated memory
        //
        // Returns the number of decoded definition levels
        int64_t ReadDefinitionLevels(int64_t batch_size, int16_t* levels) {
            if (max_def_level_ == 0) {
                return 0;
            }
            return definition_level_decoder_.Decode(static_cast<int>(batch_size), levels);
        }

        bool HasNextInternal() {
            // Either there is no data page available yet, or the data page has been
            // exhausted
            if (num_buffered_values_ == 0 || num_decoded_values_ == num_buffered_values_) {
                if (!ReadNewPage() || num_buffered_values_ == 0) {
                    return false;
                }
            }
            return true;
        }

        // Read multiple repetition levels into preallocated memory
        // Returns the number of decoded repetition levels
        int64_t ReadRepetitionLevels(int64_t batch_size, int16_t* levels) {
            if (max_rep_level_ == 0) {
                return 0;
            }
            return repetition_level_decoder_.Decode(static_cast<int>(batch_size), levels);
        }

        // Advance to the next data page
        bool ReadNewPage() {
            // Loop until we find the next data page.
            while (true) {
                current_page_ = pager_->NextPage();
                if (!current_page_) {
                    // EOS
                    return false;
                }

                if (current_page_->type() == PageType::DICTIONARY_PAGE) {
                    ConfigureDictionary(static_cast<const DictionaryPage*>(current_page_.get()));
                    continue;
                } else if (current_page_->type() == PageType::DATA_PAGE) {
                    const auto page = std::static_pointer_cast<DataPageV1>(current_page_);
                    const int64_t levels_byte_size = InitializeLevelDecoders(
                        *page, page->repetition_level_encoding(), page->definition_level_encoding());
                    InitializeDataDecoder(*page, levels_byte_size);
                    return true;
                } else if (current_page_->type() == PageType::DATA_PAGE_V2) {
                    const auto page = std::static_pointer_cast<DataPageV2>(current_page_);
                    int64_t levels_byte_size = InitializeLevelDecodersV2(*page);
                    InitializeDataDecoder(*page, levels_byte_size);
                    return true;
                } else {
                    // We don't know what this page type is. We're allowed to skip non-data
                    // pages.
                    continue;
                }
            }
            return true;
        }

        void ConfigureDictionary(const DictionaryPage* page) {
            int encoding = static_cast<int>(page->encoding());
            if (page->encoding() == Encoding::PLAIN_DICTIONARY ||
                page->encoding() == Encoding::PLAIN) {
                encoding = static_cast<int>(Encoding::RLE_DICTIONARY);
            }

            auto it = decoders_.find(encoding);
            if (it != decoders_.end()) {
                throw ParquetException("Column cannot have more than one dictionary.");
            }

            if (page->encoding() == Encoding::PLAIN_DICTIONARY ||
                page->encoding() == Encoding::PLAIN) {
                auto dictionary = MakeTypedDecoder<DType>(Encoding::PLAIN, descr_);
                dictionary->SetData(page->num_values(), page->data(), page->size());

                // The dictionary is fully decoded during DictionaryDecoder::Init, so the
                // DictionaryPage buffer is no longer required after this step
                //
                // TODO(wesm): investigate whether this all-or-nothing decoding of the
                // dictionary makes sense and whether performance can be improved

                std::unique_ptr<DictDecoder<DType>> decoder = MakeDictDecoder<DType>(descr_, pool_);
                decoder->SetDict(dictionary.get());
                decoders_[encoding] =
                    std::unique_ptr<DecoderType>(dynamic_cast<DecoderType*>(decoder.release()));
            } else {
                ParquetException::NYI("only plain dictionary encoding has been implemented");
            }

            new_dictionary_ = true;
            current_decoder_ = decoders_[encoding].get();
            DCHECK(current_decoder_);
        }

        // Initialize repetition and definition level decoders on the next data page.

        // If the data page includes repetition and definition levels, we
        // initialize the level decoders and return the number of encoded level bytes.
        // The return value helps determine the number of bytes in the encoded data.
        int64_t InitializeLevelDecoders(const DataPage& page,
                                        Encoding::type repetition_level_encoding,
                                        Encoding::type definition_level_encoding) {
            // Read a data page.
            num_buffered_values_ = page.num_values();

            // Have not decoded any values from the data page yet
            num_decoded_values_ = 0;

            const uint8_t* buffer = page.data();
            int32_t levels_byte_size = 0;
            int32_t max_size = page.size();

            // Data page Layout: Repetition Levels - Definition Levels - encoded values.
            // Levels are encoded as rle or bit-packed.
            // Init repetition levels
            if (max_rep_level_ > 0) {
                int32_t rep_levels_bytes = repetition_level_decoder_.SetData(
                    repetition_level_encoding, max_rep_level_,
                    static_cast<int>(num_buffered_values_), buffer, max_size);
                buffer += rep_levels_bytes;
                levels_byte_size += rep_levels_bytes;
                max_size -= rep_levels_bytes;
            }
            // TODO figure a way to set max_def_level_ to 0
            // if the initial value is invalid

            // Init definition levels
            if (max_def_level_ > 0) {
                int32_t def_levels_bytes = definition_level_decoder_.SetData(
                    definition_level_encoding, max_def_level_,
                    static_cast<int>(num_buffered_values_), buffer, max_size);
                levels_byte_size += def_levels_bytes;
                max_size -= def_levels_bytes;
            }

            return levels_byte_size;
        }

        int64_t InitializeLevelDecodersV2(const DataPageV2& page) {
            // Read a data page.
            num_buffered_values_ = page.num_values();

            // Have not decoded any values from the data page yet
            num_decoded_values_ = 0;
            const uint8_t* buffer = page.data();

            const int64_t total_levels_length =
                static_cast<int64_t>(page.repetition_levels_byte_length()) +
                page.definition_levels_byte_length();

            if (total_levels_length > page.size()) {
                throw ParquetException("Data page too small for levels (corrupt header?)");
            }

            if (max_rep_level_ > 0) {
                repetition_level_decoder_.SetDataV2(page.repetition_levels_byte_length(),
                                                    max_rep_level_,
                                                    static_cast<int>(num_buffered_values_), buffer);
                buffer += page.repetition_levels_byte_length();
            }

            if (max_def_level_ > 0) {
                definition_level_decoder_.SetDataV2(page.definition_levels_byte_length(),
                                                    max_def_level_,
                                                    static_cast<int>(num_buffered_values_), buffer);
            }

            return total_levels_length;
        }

        // Get a decoder object for this page or create a new decoder if this is the
        // first page with this encoding.
        void InitializeDataDecoder(const DataPage& page, int64_t levels_byte_size) {
            const uint8_t* buffer = page.data() + levels_byte_size;
            const int64_t data_size = page.size() - levels_byte_size;

            if (data_size < 0) {
                throw ParquetException("Page smaller than size of encoded levels");
            }

            Encoding::type encoding = page.encoding();

            if (IsDictionaryIndexEncoding(encoding)) {
                encoding = Encoding::RLE_DICTIONARY;
            }

            auto it = decoders_.find(static_cast<int>(encoding));
            if (it != decoders_.end()) {
                DCHECK(it->second.get() != nullptr);
                if (encoding == Encoding::RLE_DICTIONARY) {
                    DCHECK(current_decoder_->encoding() == Encoding::RLE_DICTIONARY);
                }
                current_decoder_ = it->second.get();
            } else {
                switch (encoding) {
                    case Encoding::PLAIN: {
                        auto decoder = MakeTypedDecoder<DType>(Encoding::PLAIN, descr_);
                        current_decoder_ = decoder.get();
                        decoders_[static_cast<int>(encoding)] = std::move(decoder);
                        break;
                    }
                    case Encoding::BYTE_STREAM_SPLIT: {
                        auto decoder = MakeTypedDecoder<DType>(Encoding::BYTE_STREAM_SPLIT, descr_);
                        current_decoder_ = decoder.get();
                        decoders_[static_cast<int>(encoding)] = std::move(decoder);
                        break;
                    }
                    case Encoding::RLE_DICTIONARY:
                        throw ParquetException("Dictionary page must be before data page.");

                    case Encoding::DELTA_BINARY_PACKED: {
                        auto decoder = MakeTypedDecoder<DType>(Encoding::DELTA_BINARY_PACKED, descr_);
                        current_decoder_ = decoder.get();
                        decoders_[static_cast<int>(encoding)] = std::move(decoder);
                        break;
                    }
                    case Encoding::DELTA_LENGTH_BYTE_ARRAY:
                    case Encoding::DELTA_BYTE_ARRAY:
                        ParquetException::NYI("Unsupported encoding");

                    default:
                        throw ParquetException("Unknown encoding type.");
                }
            }
            current_encoding_ = encoding;
            current_decoder_->SetData(static_cast<int>(num_buffered_values_), buffer,
                                      static_cast<int>(data_size));
        }

        const ColumnDescriptor* descr_;
        const int16_t max_def_level_;
        const int16_t max_rep_level_;

        std::unique_ptr<PageReader> pager_;
        std::shared_ptr<Page> current_page_;

        // Not set if full schema for this field has no optional or repeated elements
        LevelDecoder definition_level_decoder_;

        // Not set for flat schemas.
        LevelDecoder repetition_level_decoder_;

        // The total number of values stored in the data page. This is the maximum of
        // the number of encoded definition levels or encoded values. For
        // non-repeated, required columns, this is equal to the number of encoded
        // values. For repeated or optional values, there may be fewer data values
        // than levels, and this tells you how many encoded levels there are in that
        // case.
        int64_t num_buffered_values_;

        // The number of values from the current data page that have been decoded
        // into memory
        int64_t num_decoded_values_;

        ::arrow::MemoryPool* pool_;

        using DecoderType = TypedDecoder<DType>;
        DecoderType* current_decoder_;
        Encoding::type current_encoding_;

        /// Flag to signal when a new dictionary has been set, for the benefit of
        /// DictionaryRecordReader
        bool new_dictionary_;

        // The exposed encoding
        ExposedEncoding exposed_encoding_ = ExposedEncoding::NO_ENCODING;

        // Map of encoding type to the respective decoder object. For example, a
        // column chunk's data pages may include both dictionary-encoded and
        // plain-encoded data.
        std::unordered_map<int, std::unique_ptr<DecoderType>> decoders_;

        void ConsumeBufferedValues(int64_t num_values) { num_decoded_values_ += num_values; }
    };

    // ----------------------------------------------------------------------
    // TypedColumnReader implementations

    template <typename DType>
    class TypedColumnReaderImpl : public TypedColumnReader<DType>,
                                  public ColumnReaderImplBase<DType> {
    public:
        using T = typename DType::c_type;

        TypedColumnReaderImpl(const ColumnDescriptor* descr, std::unique_ptr<PageReader> pager,
                              ::arrow::MemoryPool* pool)
            : ColumnReaderImplBase<DType>(descr, pool) {
            this->pager_ = std::move(pager);
        }

        bool HasNext() override { return this->HasNextInternal(); }

        int64_t ReadBatch(int64_t batch_size, int16_t* def_levels, int16_t* rep_levels,
                          T* values, int64_t* values_read) override;

        int64_t ReadBatchSpaced(int64_t batch_size, int16_t* def_levels, int16_t* rep_levels,
                                T* values, uint8_t* valid_bits, int64_t valid_bits_offset,
                                int64_t* levels_read, int64_t* values_read,
                                int64_t* null_count) override;

        int64_t Skip(int64_t num_rows_to_skip) override;

        Type::type type() const override { return this->descr_->physical_type(); }

        const ColumnDescriptor* descr() const override { return this->descr_; }

        ExposedEncoding GetExposedEncoding() override { return this->exposed_encoding_; };

        int64_t ReadBatchWithDictionary(int64_t batch_size, int16_t* def_levels,
                                        int16_t* rep_levels, int32_t* indices,
                                        int64_t* indices_read, const T** dict,
                                        int32_t* dict_len) override;

    protected:
        void SetExposedEncoding(ExposedEncoding encoding) override {
            this->exposed_encoding_ = encoding;
        }

    private:
        // Read dictionary indices. Similar to ReadValues but decode data to dictionary indices.
        // This function is called only by ReadBatchWithDictionary().
        int64_t ReadDictionaryIndices(int64_t indices_to_read, int32_t* indices) {
            auto decoder = dynamic_cast<DictDecoder<DType>*>(this->current_decoder_);
            return decoder->DecodeIndices(static_cast<int>(indices_to_read), indices);
        }

        // Get dictionary. The dictionary should have been set by SetDict(). The dictionary is
        // owned by the internal decoder and is destroyed when the reader is destroyed. This
        // function is called only by ReadBatchWithDictionary() after dictionary is configured.
        void GetDictionary(const T** dictionary, int32_t* dictionary_length) {
            auto decoder = dynamic_cast<DictDecoder<DType>*>(this->current_decoder_);
            decoder->GetDictionary(dictionary, dictionary_length);
        }

        // Read definition and repetition levels. Also return the number of definition levels
        // and number of values to read. This function is called before reading values.
        void ReadLevels(int64_t batch_size, int16_t* def_levels, int16_t* rep_levels,
                        int64_t* num_def_levels, int64_t* values_to_read) {
            batch_size =
                std::min(batch_size, this->num_buffered_values_ - this->num_decoded_values_);

            // If the field is required and non-repeated, there are no definition levels
            if (this->max_def_level_ > 0 && def_levels != nullptr) {
                *num_def_levels = this->ReadDefinitionLevels(batch_size, def_levels);
                // TODO(wesm): this tallying of values-to-decode can be performed with better
                // cache-efficiency if fused with the level decoding.
                for (int64_t i = 0; i < *num_def_levels; ++i) {
                    if (def_levels[i] == this->max_def_level_) {
                        ++(*values_to_read);
                    }
                }
            } else {
                // Required field, read all values
                *values_to_read = batch_size;
            }

            // Not present for non-repeated fields
            if (this->max_rep_level_ > 0 && rep_levels != nullptr) {
                int64_t num_rep_levels = this->ReadRepetitionLevels(batch_size, rep_levels);
                if (def_levels != nullptr && *num_def_levels != num_rep_levels) {
                    throw ParquetException("Number of decoded rep / def levels did not match");
                }
            }
        }
    };

    template <typename DType>
    int64_t TypedColumnReaderImpl<DType>::ReadBatchWithDictionary(
        int64_t batch_size, int16_t* def_levels, int16_t* rep_levels, int32_t* indices,
        int64_t* indices_read, const T** dict, int32_t* dict_len) {
        bool has_dict_output = dict != nullptr && dict_len != nullptr;
        // Similar logic as ReadValues to get pages.
        if (!HasNext()) {
            *indices_read = 0;
            if (has_dict_output) {
                *dict = nullptr;
                *dict_len = 0;
            }
            return 0;
        }

        // Verify the current data page is dictionary encoded.
        if (this->current_encoding_ != Encoding::RLE_DICTIONARY) {
            std::stringstream ss;
            ss << "Data page is not dictionary encoded. Encoding: "
               << EncodingToString(this->current_encoding_);
            throw ParquetException(ss.str());
        }

        // Get dictionary pointer and length.
        if (has_dict_output) {
            GetDictionary(dict, dict_len);
        }

        // Similar logic as ReadValues to get def levels and rep levels.
        int64_t num_def_levels = 0;
        int64_t indices_to_read = 0;
        ReadLevels(batch_size, def_levels, rep_levels, &num_def_levels, &indices_to_read);

        // Read dictionary indices.
        *indices_read = ReadDictionaryIndices(indices_to_read, indices);
        int64_t total_indices = std::max(num_def_levels, *indices_read);
        this->ConsumeBufferedValues(total_indices);

        return total_indices;
    }

    template <typename DType>
    int64_t TypedColumnReaderImpl<DType>::ReadBatch(int64_t batch_size, int16_t* def_levels,
                                                    int16_t* rep_levels, T* values,
                                                    int64_t* values_read) {
        // HasNext invokes ReadNewPage
        if (!HasNext()) {
            *values_read = 0;
            return 0;
        }

        // TODO(wesm): keep reading data pages until batch_size is reached, or the
        // row group is finished
        int64_t num_def_levels = 0;
        int64_t values_to_read = 0;
        ReadLevels(batch_size, def_levels, rep_levels, &num_def_levels, &values_to_read);

        *values_read = this->ReadValues(values_to_read, values);
        int64_t total_values = std::max(num_def_levels, *values_read);
        this->ConsumeBufferedValues(total_values);

        return total_values;
    }

    template <typename DType>
    int64_t TypedColumnReaderImpl<DType>::ReadBatchSpaced(
        int64_t batch_size, int16_t* def_levels, int16_t* rep_levels, T* values,
        uint8_t* valid_bits, int64_t valid_bits_offset, int64_t* levels_read,
        int64_t* values_read, int64_t* null_count_out) {
        // HasNext invokes ReadNewPage
        if (!HasNext()) {
            *levels_read = 0;
            *values_read = 0;
            *null_count_out = 0;
            return 0;
        }

        int64_t total_values;
        // TODO(wesm): keep reading data pages until batch_size is reached, or the
        // row group is finished
        batch_size =
            std::min(batch_size, this->num_buffered_values_ - this->num_decoded_values_);

        // If the field is required and non-repeated, there are no definition levels
        if (this->max_def_level_ > 0) {
            int64_t num_def_levels = this->ReadDefinitionLevels(batch_size, def_levels);

            // Not present for non-repeated fields
            if (this->max_rep_level_ > 0) {
                int64_t num_rep_levels = this->ReadRepetitionLevels(batch_size, rep_levels);
                if (num_def_levels != num_rep_levels) {
                    throw ParquetException("Number of decoded rep / def levels did not match");
                }
            }

            const bool has_spaced_values = HasSpacedValues(this->descr_);
            int64_t null_count = 0;
            if (!has_spaced_values) {
                int values_to_read = 0;
                for (int64_t i = 0; i < num_def_levels; ++i) {
                    if (def_levels[i] == this->max_def_level_) {
                        ++values_to_read;
                    }
                }
                total_values = this->ReadValues(values_to_read, values);
                ::arrow::BitUtil::SetBitsTo(valid_bits, valid_bits_offset,
                                            /*length=*/total_values,
                                            /*bits_are_set=*/true);
                *values_read = total_values;
            } else {
                internal::LevelInfo info;
                info.repeated_ancestor_def_level = this->max_def_level_ - 1;
                info.def_level = this->max_def_level_;
                info.rep_level = this->max_rep_level_;
                internal::ValidityBitmapInputOutput validity_io;
                validity_io.values_read_upper_bound = num_def_levels;
                validity_io.valid_bits = valid_bits;
                validity_io.valid_bits_offset = valid_bits_offset;
                validity_io.null_count = null_count;
                validity_io.values_read = *values_read;

                internal::DefLevelsToBitmap(def_levels, num_def_levels, info, &validity_io);
                null_count = validity_io.null_count;
                *values_read = validity_io.values_read;

                total_values =
                    this->ReadValuesSpaced(*values_read, values, static_cast<int>(null_count),
                                           valid_bits, valid_bits_offset);
            }
            *levels_read = num_def_levels;
            *null_count_out = null_count;

        } else {
            // Required field, read all values
            total_values = this->ReadValues(batch_size, values);
            ::arrow::BitUtil::SetBitsTo(valid_bits, valid_bits_offset,
                                        /*length=*/total_values,
                                        /*bits_are_set=*/true);
            *null_count_out = 0;
            *values_read = total_values;
            *levels_read = total_values;
        }

        this->ConsumeBufferedValues(*levels_read);
        return total_values;
    }

    template <typename DType>
    int64_t TypedColumnReaderImpl<DType>::Skip(int64_t num_rows_to_skip) {
        int64_t rows_to_skip = num_rows_to_skip;
        while (HasNext() && rows_to_skip > 0) {
            // If the number of rows to skip is more than the number of undecoded values, skip the
            // Page.
            if (rows_to_skip > (this->num_buffered_values_ - this->num_decoded_values_)) {
                rows_to_skip -= this->num_buffered_values_ - this->num_decoded_values_;
                this->num_decoded_values_ = this->num_buffered_values_;
            } else {
                // We need to read this Page
                // Jump to the right offset in the Page
                int64_t batch_size = 1024;  // ReadBatch with a smaller memory footprint
                int64_t values_read = 0;

                // This will be enough scratch space to accommodate 16-bit levels or any
                // value type
                int value_size = type_traits<DType::type_num>::value_byte_size;
                std::shared_ptr<ResizableBuffer> scratch = AllocateBuffer(
                    this->pool_, batch_size * std::max<int>(sizeof(int16_t), value_size));

                do {
                    batch_size = std::min(batch_size, rows_to_skip);
                    values_read =
                        ReadBatch(static_cast<int>(batch_size),
                                  reinterpret_cast<int16_t*>(scratch->mutable_data()),
                                  reinterpret_cast<int16_t*>(scratch->mutable_data()),
                                  reinterpret_cast<T*>(scratch->mutable_data()), &values_read);
                    rows_to_skip -= values_read;
                } while (values_read > 0 && rows_to_skip > 0);
            }
        }
        return num_rows_to_skip - rows_to_skip;
    }

}  // namespace

// ----------------------------------------------------------------------
// Dynamic column reader constructor

std::shared_ptr<ColumnReader> ColumnReader::Make(const ColumnDescriptor* descr,
                                                 std::unique_ptr<PageReader> pager,
                                                 MemoryPool* pool) {
    switch (descr->physical_type()) {
        case Type::BOOLEAN:
            return std::make_shared<TypedColumnReaderImpl<BooleanType>>(descr, std::move(pager),
                                                                        pool);
        case Type::INT32:
            return std::make_shared<TypedColumnReaderImpl<Int32Type>>(descr, std::move(pager),
                                                                      pool);
        case Type::INT64:
            return std::make_shared<TypedColumnReaderImpl<Int64Type>>(descr, std::move(pager),
                                                                      pool);
        case Type::INT96:
            return std::make_shared<TypedColumnReaderImpl<Int96Type>>(descr, std::move(pager),
                                                                      pool);
        case Type::FLOAT:
            return std::make_shared<TypedColumnReaderImpl<FloatType>>(descr, std::move(pager),
                                                                      pool);
        case Type::DOUBLE:
            return std::make_shared<TypedColumnReaderImpl<DoubleType>>(descr, std::move(pager),
                                                                       pool);
        case Type::BYTE_ARRAY:
            return std::make_shared<TypedColumnReaderImpl<ByteArrayType>>(
                descr, std::move(pager), pool);
        case Type::FIXED_LEN_BYTE_ARRAY:
            return std::make_shared<TypedColumnReaderImpl<FLBAType>>(descr, std::move(pager),
                                                                     pool);
        default:
            ParquetException::NYI("type reader not implemented");
    }
    // Unreachable code, but suppress compiler warning
    return std::shared_ptr<ColumnReader>(nullptr);
}

// ----------------------------------------------------------------------
// RecordReader

namespace internal {
    namespace {

        // The minimum number of repetition/definition levels to decode at a time, for
        // better vectorized performance when doing many smaller record reads
        constexpr int64_t kMinLevelBatchSize = 1024;

        template <typename DType>
        class TypedRecordReader : public ColumnReaderImplBase<DType>,
                                  virtual public RecordReader {
        public:
            using T = typename DType::c_type;
            using BASE = ColumnReaderImplBase<DType>;
            TypedRecordReader(const ColumnDescriptor* descr, LevelInfo leaf_info, MemoryPool* pool)
                : BASE(descr, pool) {
                leaf_info_ = leaf_info;
                nullable_values_ = leaf_info.HasNullableValues();
                at_record_start_ = true;
                records_read_ = 0;
                values_written_ = 0;
                values_capacity_ = 0;
                null_count_ = 0;
                levels_written_ = 0;
                levels_position_ = 0;
                levels_capacity_ = 0;
                uses_values_ = !(descr->physical_type() == Type::BYTE_ARRAY);

                if (uses_values_) {
                    values_ = AllocateBuffer(pool);
                }
                valid_bits_ = AllocateBuffer(pool);
                def_levels_ = AllocateBuffer(pool);
                rep_levels_ = AllocateBuffer(pool);
                Reset();
            }

            int64_t available_values_current_page() const {
                return this->num_buffered_values_ - this->num_decoded_values_;
            }

            // Compute the values capacity in bytes for the given number of elements
            int64_t bytes_for_values(int64_t nitems) const {
                int64_t type_size = GetTypeByteSize(this->descr_->physical_type());
                int64_t bytes_for_values = -1;
                if (MultiplyWithOverflow(nitems, type_size, &bytes_for_values)) {
                    throw ParquetException("Total size of items too large");
                }
                return bytes_for_values;
            }

            int64_t ReadRecords(int64_t num_records) override {
                // Delimit records, then read values at the end
                int64_t records_read = 0;

                if (levels_position_ < levels_written_) {
                    records_read += ReadRecordData(num_records);
                }

                int64_t level_batch_size = std::max(kMinLevelBatchSize, num_records);

                // If we are in the middle of a record, we continue until reaching the
                // desired number of records or the end of the current record if we've found
                // enough records
                while (!at_record_start_ || records_read < num_records) {
                    // Is there more data to read in this row group?
                    if (!this->HasNextInternal()) {
                        if (!at_record_start_) {
                            // We ended the row group while inside a record that we haven't seen
                            // the end of yet. So increment the record count for the last record in
                            // the row group
                            ++records_read;
                            at_record_start_ = true;
                        }
                        break;
                    }

                    /// We perform multiple batch reads until we either exhaust the row group
                    /// or observe the desired number of records
                    int64_t batch_size = std::min(level_batch_size, available_values_current_page());

                    // No more data in column
                    if (batch_size == 0) {
                        break;
                    }

                    if (this->max_def_level_ > 0) {
                        ReserveLevels(batch_size);

                        int16_t* def_levels = this->def_levels() + levels_written_;
                        int16_t* rep_levels = this->rep_levels() + levels_written_;

                        // Not present for non-repeated fields
                        int64_t levels_read = 0;
                        if (this->max_rep_level_ > 0) {
                            levels_read = this->ReadDefinitionLevels(batch_size, def_levels);
                            if (this->ReadRepetitionLevels(batch_size, rep_levels) != levels_read) {
                                throw ParquetException("Number of decoded rep / def levels did not match");
                            }
                        } else if (this->max_def_level_ > 0) {
                            levels_read = this->ReadDefinitionLevels(batch_size, def_levels);
                        }

                        // Exhausted column chunk
                        if (levels_read == 0) {
                            break;
                        }

                        levels_written_ += levels_read;
                        records_read += ReadRecordData(num_records - records_read);
                    } else {
                        // No repetition or definition levels
                        batch_size = std::min(num_records - records_read, batch_size);
                        records_read += ReadRecordData(batch_size);
                    }
                }

                return records_read;
            }

            // We may outwardly have the appearance of having exhausted a column chunk
            // when in fact we are in the middle of processing the last batch
            bool has_values_to_process() const { return levels_position_ < levels_written_; }

            std::shared_ptr<ResizableBuffer> ReleaseValues() override {
                if (uses_values_) {
                    auto result = values_;
                    PARQUET_THROW_NOT_OK(result->Resize(bytes_for_values(values_written_), true));
                    values_ = AllocateBuffer(this->pool_);
                    values_capacity_ = 0;
                    return result;
                } else {
                    return nullptr;
                }
            }

            std::shared_ptr<ResizableBuffer> ReleaseIsValid() override {
                if (leaf_info_.HasNullableValues()) {
                    auto result = valid_bits_;
                    PARQUET_THROW_NOT_OK(result->Resize(BitUtil::BytesForBits(values_written_), true));
                    valid_bits_ = AllocateBuffer(this->pool_);
                    return result;
                } else {
                    return nullptr;
                }
            }

            // Process written repetition/definition levels to reach the end of
            // records. Process no more levels than necessary to delimit the indicated
            // number of logical records. Updates internal state of RecordReader
            //
            // \return Number of records delimited
            int64_t DelimitRecords(int64_t num_records, int64_t* values_seen) {
                int64_t values_to_read = 0;
                int64_t records_read = 0;

                const int16_t* def_levels = this->def_levels() + levels_position_;
                const int16_t* rep_levels = this->rep_levels() + levels_position_;

                DCHECK_GT(this->max_rep_level_, 0);

                // Count logical records and number of values to read
                while (levels_position_ < levels_written_) {
                    const int16_t rep_level = *rep_levels++;
                    if (rep_level == 0) {
                        // If at_record_start_ is true, we are seeing the start of a record
                        // for the second time, such as after repeated calls to
                        // DelimitRecords. In this case we must continue until we find
                        // another record start or exhausting the ColumnChunk
                        if (!at_record_start_) {
                            // We've reached the end of a record; increment the record count.
                            ++records_read;
                            if (records_read == num_records) {
                                // We've found the number of records we were looking for. Set
                                // at_record_start_ to true and break
                                at_record_start_ = true;
                                break;
                            }
                        }
                    }
                    // We have decided to consume the level at this position; therefore we
                    // must advance until we find another record boundary
                    at_record_start_ = false;

                    const int16_t def_level = *def_levels++;
                    if (def_level == this->max_def_level_) {
                        ++values_to_read;
                    }
                    ++levels_position_;
                }
                *values_seen = values_to_read;
                return records_read;
            }

            void Reserve(int64_t capacity) override {
                ReserveLevels(capacity);
                ReserveValues(capacity);
            }

            int64_t UpdateCapacity(int64_t capacity, int64_t size, int64_t extra_size) {
                if (extra_size < 0) {
                    throw ParquetException("Negative size (corrupt file?)");
                }
                int64_t target_size = -1;
                if (AddWithOverflow(size, extra_size, &target_size)) {
                    throw ParquetException("Allocation size too large (corrupt file?)");
                }
                if (target_size >= (1LL << 62)) {
                    throw ParquetException("Allocation size too large (corrupt file?)");
                }
                if (capacity >= target_size) {
                    return capacity;
                }
                return BitUtil::NextPower2(target_size);
            }

            void ReserveLevels(int64_t extra_levels) {
                if (this->max_def_level_ > 0) {
                    const int64_t new_levels_capacity =
                        UpdateCapacity(levels_capacity_, levels_written_, extra_levels);
                    if (new_levels_capacity > levels_capacity_) {
                        constexpr auto kItemSize = static_cast<int64_t>(sizeof(int16_t));
                        int64_t capacity_in_bytes = -1;
                        if (MultiplyWithOverflow(new_levels_capacity, kItemSize, &capacity_in_bytes)) {
                            throw ParquetException("Allocation size too large (corrupt file?)");
                        }
                        PARQUET_THROW_NOT_OK(def_levels_->Resize(capacity_in_bytes, false));
                        if (this->max_rep_level_ > 0) {
                            PARQUET_THROW_NOT_OK(rep_levels_->Resize(capacity_in_bytes, false));
                        }
                        levels_capacity_ = new_levels_capacity;
                    }
                }
            }

            void ReserveValues(int64_t extra_values) {
                const int64_t new_values_capacity =
                    UpdateCapacity(values_capacity_, values_written_, extra_values);
                if (new_values_capacity > values_capacity_) {
                    // XXX(wesm): A hack to avoid memory allocation when reading directly
                    // into builder classes
                    if (uses_values_) {
                        PARQUET_THROW_NOT_OK(
                            values_->Resize(bytes_for_values(new_values_capacity), false));
                    }
                    values_capacity_ = new_values_capacity;
                }
                if (leaf_info_.HasNullableValues()) {
                    int64_t valid_bytes_new = BitUtil::BytesForBits(values_capacity_);
                    if (valid_bits_->size() < valid_bytes_new) {
                        int64_t valid_bytes_old = BitUtil::BytesForBits(values_written_);
                        PARQUET_THROW_NOT_OK(valid_bits_->Resize(valid_bytes_new, false));

                        // Avoid valgrind warnings
                        memset(valid_bits_->mutable_data() + valid_bytes_old, 0,
                               valid_bytes_new - valid_bytes_old);
                    }
                }
            }

            void Reset() override {
                ResetValues();

                if (levels_written_ > 0) {
                    const int64_t levels_remaining = levels_written_ - levels_position_;
                    // Shift remaining levels to beginning of buffer and trim to only the number
                    // of decoded levels remaining
                    int16_t* def_data = def_levels();
                    int16_t* rep_data = rep_levels();

                    std::copy(def_data + levels_position_, def_data + levels_written_, def_data);
                    PARQUET_THROW_NOT_OK(
                        def_levels_->Resize(levels_remaining * sizeof(int16_t), false));

                    if (this->max_rep_level_ > 0) {
                        std::copy(rep_data + levels_position_, rep_data + levels_written_, rep_data);
                        PARQUET_THROW_NOT_OK(
                            rep_levels_->Resize(levels_remaining * sizeof(int16_t), false));
                    }

                    levels_written_ -= levels_position_;
                    levels_position_ = 0;
                    levels_capacity_ = levels_remaining;
                }

                records_read_ = 0;

                // Call Finish on the binary builders to reset them
            }

            void SetPageReader(std::unique_ptr<PageReader> reader) override {
                at_record_start_ = true;
                this->pager_ = std::move(reader);
                ResetDecoders();
            }

            bool HasMoreData() const override { return this->pager_ != nullptr; }

            // Dictionary decoders must be reset when advancing row groups
            void ResetDecoders() { this->decoders_.clear(); }

            virtual void ReadValuesSpaced(int64_t values_with_nulls, int64_t null_count) {
                uint8_t* valid_bits = valid_bits_->mutable_data();
                const int64_t valid_bits_offset = values_written_;

                int64_t num_decoded = this->current_decoder_->DecodeSpaced(
                    ValuesHead<T>(), static_cast<int>(values_with_nulls),
                    static_cast<int>(null_count), valid_bits, valid_bits_offset);
                DCHECK_EQ(num_decoded, values_with_nulls);
            }

            virtual void ReadValuesDense(int64_t values_to_read) {
                int64_t num_decoded =
                    this->current_decoder_->Decode(ValuesHead<T>(), static_cast<int>(values_to_read));
                DCHECK_EQ(num_decoded, values_to_read);
            }

            // Return number of logical records read
            int64_t ReadRecordData(int64_t num_records) {
                // Conservative upper bound
                const int64_t possible_num_values =
                    std::max(num_records, levels_written_ - levels_position_);
                ReserveValues(possible_num_values);

                const int64_t start_levels_position = levels_position_;

                int64_t values_to_read = 0;
                int64_t records_read = 0;
                if (this->max_rep_level_ > 0) {
                    records_read = DelimitRecords(num_records, &values_to_read);
                } else if (this->max_def_level_ > 0) {
                    // No repetition levels, skip delimiting logic. Each level represents a
                    // null or not null entry
                    records_read = std::min(levels_written_ - levels_position_, num_records);

                    // This is advanced by DelimitRecords, which we skipped
                    levels_position_ += records_read;
                } else {
                    records_read = values_to_read = num_records;
                }

                int64_t null_count = 0;
                if (leaf_info_.HasNullableValues()) {
                    ValidityBitmapInputOutput validity_io;
                    validity_io.values_read_upper_bound = levels_position_ - start_levels_position;
                    validity_io.valid_bits = valid_bits_->mutable_data();
                    validity_io.valid_bits_offset = values_written_;

                    DefLevelsToBitmap(def_levels() + start_levels_position,
                                      levels_position_ - start_levels_position, leaf_info_,
                                      &validity_io);
                    values_to_read = validity_io.values_read - validity_io.null_count;
                    null_count = validity_io.null_count;
                    DCHECK_GE(values_to_read, 0);
                    ReadValuesSpaced(validity_io.values_read, null_count);
                } else {
                    DCHECK_GE(values_to_read, 0);
                    ReadValuesDense(values_to_read);
                }
                if (this->leaf_info_.def_level > 0) {
                    // Optional, repeated, or some mix thereof
                    this->ConsumeBufferedValues(levels_position_ - start_levels_position);
                } else {
                    // Flat, non-repeated
                    this->ConsumeBufferedValues(values_to_read);
                }
                // Total values, including null spaces, if any
                values_written_ += values_to_read + null_count;
                null_count_ += null_count;

                return records_read;
            }

            void DebugPrintState() override {
                const int16_t* def_levels = this->def_levels();
                const int16_t* rep_levels = this->rep_levels();
                const int64_t total_levels_read = levels_position_;

                const T* vals = reinterpret_cast<const T*>(this->values());

                std::cout << "def levels: ";
                for (int64_t i = 0; i < total_levels_read; ++i) {
                    std::cout << def_levels[i] << " ";
                }
                std::cout << std::endl;

                std::cout << "rep levels: ";
                for (int64_t i = 0; i < total_levels_read; ++i) {
                    std::cout << rep_levels[i] << " ";
                }
                std::cout << std::endl;

                std::cout << "values: ";
                for (int64_t i = 0; i < this->values_written(); ++i) {
                    std::cout << vals[i] << " ";
                }
                std::cout << std::endl;
            }

            void ResetValues() {
                if (values_written_ > 0) {
                    // Resize to 0, but do not shrink to fit
                    if (uses_values_) {
                        PARQUET_THROW_NOT_OK(values_->Resize(0, false));
                    }
                    PARQUET_THROW_NOT_OK(valid_bits_->Resize(0, false));
                    values_written_ = 0;
                    values_capacity_ = 0;
                    null_count_ = 0;
                }
            }

        protected:
            template <typename T>
            T* ValuesHead() {
                return reinterpret_cast<T*>(values_->mutable_data()) + values_written_;
            }
            LevelInfo leaf_info_;
        };

        class FLBARecordReader : public TypedRecordReader<FLBAType>,
                                 virtual public BinaryRecordReader {
        public:
            FLBARecordReader(const ColumnDescriptor* descr, LevelInfo leaf_info,
                             ::arrow::MemoryPool* pool)
                : TypedRecordReader<FLBAType>(descr, leaf_info, pool), builder_(nullptr) {
                DCHECK_EQ(descr_->physical_type(), Type::FIXED_LEN_BYTE_ARRAY);
                int byte_width = descr_->type_length();
                std::shared_ptr<::arrow::DataType> type = ::arrow::fixed_size_binary(byte_width);
                builder_.reset(new ::arrow::FixedSizeBinaryBuilder(type, this->pool_));
            }

            ::arrow::ArrayVector GetBuilderChunks() override {
                std::shared_ptr<::arrow::Array> chunk;
                PARQUET_THROW_NOT_OK(builder_->Finish(&chunk));
                return ::arrow::ArrayVector({chunk});
            }

            void ReadValuesDense(int64_t values_to_read) override {
                auto values = ValuesHead<FLBA>();
                int64_t num_decoded =
                    this->current_decoder_->Decode(values, static_cast<int>(values_to_read));
                DCHECK_EQ(num_decoded, values_to_read);

                for (int64_t i = 0; i < num_decoded; i++) {
                    PARQUET_THROW_NOT_OK(builder_->Append(values[i].ptr));
                }
                ResetValues();
            }

            void ReadValuesSpaced(int64_t values_to_read, int64_t null_count) override {
                uint8_t* valid_bits = valid_bits_->mutable_data();
                const int64_t valid_bits_offset = values_written_;
                auto values = ValuesHead<FLBA>();

                int64_t num_decoded = this->current_decoder_->DecodeSpaced(
                    values, static_cast<int>(values_to_read), static_cast<int>(null_count),
                    valid_bits, valid_bits_offset);
                DCHECK_EQ(num_decoded, values_to_read);

                for (int64_t i = 0; i < num_decoded; i++) {
                    if (::arrow::BitUtil::GetBit(valid_bits, valid_bits_offset + i)) {
                        PARQUET_THROW_NOT_OK(builder_->Append(values[i].ptr));
                    } else {
                        PARQUET_THROW_NOT_OK(builder_->AppendNull());
                    }
                }
                ResetValues();
            }

        private:
            std::unique_ptr<::arrow::FixedSizeBinaryBuilder> builder_;
        };


        class ByteArrayChunkedRecordReader : public TypedRecordReader<ByteArrayType>,
                                             virtual public BinaryRecordReader {
        public:
            ByteArrayChunkedRecordReader(const ColumnDescriptor* descr, LevelInfo leaf_info,
                                         ::arrow::MemoryPool* pool)
                : TypedRecordReader<ByteArrayType>(descr, leaf_info, pool) {
                DCHECK_EQ(descr_->physical_type(), Type::BYTE_ARRAY);
                accumulator_.builder.reset(new ::arrow::BinaryBuilder(pool));
            }

            ::arrow::ArrayVector GetBuilderChunks() override {
                ::arrow::ArrayVector result = accumulator_.chunks;
                if (result.size() == 0 || accumulator_.builder->length() > 0) {
                    std::shared_ptr<::arrow::Array> last_chunk;
                    PARQUET_THROW_NOT_OK(accumulator_.builder->Finish(&last_chunk));
                    result.push_back(std::move(last_chunk));
                }
                accumulator_.chunks = {};
                return result;
            }

            void ReadValuesDense(int64_t values_to_read) override {
                int64_t num_decoded = this->current_decoder_->DecodeArrowNonNull(
                    static_cast<int>(values_to_read), &accumulator_);
                DCHECK_EQ(num_decoded, values_to_read);
                ResetValues();
            }

            void ReadValuesSpaced(int64_t values_to_read, int64_t null_count) override {
                int64_t num_decoded = this->current_decoder_->DecodeArrow(
                    static_cast<int>(values_to_read), static_cast<int>(null_count),
                    valid_bits_->mutable_data(), values_written_, &accumulator_);
                DCHECK_EQ(num_decoded, values_to_read - null_count);
                ResetValues();
            }

        private:
            // Helper data structure for accumulating builder chunks
            typename EncodingTraits<ByteArrayType>::Accumulator accumulator_;
        };



        class CHByteArrayChunkedRecordReader : public TypedRecordReader<ByteArrayType>, virtual public BinaryRecordReader
        {
        public:
            CHByteArrayChunkedRecordReader(const ColumnDescriptor * descr, LevelInfo leaf_info, ::arrow::MemoryPool * pool)
                : TypedRecordReader<ByteArrayType>(descr, leaf_info, pool)
            {
                DCHECK_EQ(descr_->physical_type(), Type::BYTE_ARRAY);
                this -> pool = pool;
                //accumulator_.builder.reset(new ::arrow::BinaryBuilder(pool));
            }

            bool inited = false;
            PaddedPODArray<UInt8> * column_chars_t_p;
            PaddedPODArray<UInt64> * column_offsets_p;
            std::unique_ptr<MutableColumnPtr> internal_column;
            ::arrow::MemoryPool * pool;

            std::shared_ptr<::arrow::Array> fake_array;
            int64_t null_counter = 0;
            int64_t value_counter = 0;

            void initialize() {
                accumulator_.builder = std::make_unique<::arrow::BinaryBuilder>(pool);
                if (!fake_array) {
                    accumulator_.builder->AppendNulls(8192);
                    accumulator_.builder->Finish(&fake_array);
                }
                inited = true;
            }

            void createColumnIfNeeded() {
                if (!internal_column) {
                    auto internal_type = std::make_shared<DataTypeString>();
                    internal_column = std::make_unique<MutableColumnPtr>(std::move(internal_type->createColumn()));
                    column_chars_t_p = &assert_cast<ColumnString &>(**internal_column).getChars();
                    column_offsets_p = &assert_cast<ColumnString &>(**internal_column).getOffsets();
                }
            }

            ::arrow::ArrayVector GetBuilderChunks() override
            {
                if (!internal_column) { // !internal_column happens at the last empty chunk
                    ::arrow::ArrayVector result = accumulator_.chunks;
                    if (accumulator_.builder->length() > 0) {
                        throw ::parquet::ParquetException("unexpected data existing");
                    }
                    accumulator_.chunks = {};
                    return result;
                } else {
                    MutableColumnPtr temp = std::move(*internal_column);
                    internal_column.reset();
                    fake_array->data()->length = temp->size();//the last batch's size may < 8192

                    fake_array->data()->SetNullCount(null_counter);
                    null_counter = 0;
                    value_counter = 0;
                    return {std::make_shared<CHStringArray>(
                        ColumnWithTypeAndName(std::move(temp), std::make_shared<DataTypeString>(), ""),fake_array)};
                }
            }

            void ReadValuesDense(int64_t values_to_read) override
            {
                if (unlikely(!inited)) {initialize();}

                ::arrow::internal::BitmapWriter bitmap_writer(
                    const_cast<uint8_t*>(fake_array->data()->buffers[0]->data()),
                    value_counter, values_to_read);

                createColumnIfNeeded();
                int64_t num_decoded
                    = this->current_decoder_->DecodeCHNonNull(static_cast<int>(values_to_read), column_chars_t_p, column_offsets_p, bitmap_writer);
                DCHECK_EQ(num_decoded, values_to_read);
                ResetValues();

                value_counter += values_to_read;
                bitmap_writer.Finish();
            }

            void ReadValuesSpaced(int64_t values_to_read, int64_t null_count) override
            {
                if (unlikely(!inited)) {initialize();}

                ::arrow::internal::BitmapWriter bitmap_writer(
                    const_cast<uint8_t*>(fake_array->data()->buffers[0]->data()),
                    value_counter, values_to_read);

                createColumnIfNeeded();
                int64_t num_decoded = this->current_decoder_->DecodeCH(
                    static_cast<int>(values_to_read),
                    static_cast<int>(null_count),
                    valid_bits_->mutable_data(),
                    values_written_,
                    column_chars_t_p,
                    column_offsets_p,
                    bitmap_writer);

                null_counter += null_count;
                value_counter += values_to_read;
                DCHECK_EQ(num_decoded, values_to_read - null_count);
                ResetValues();

                bitmap_writer.Finish();
            }

        private:
            // Helper data structure for accumulating builder chunks
            typename EncodingTraits<ByteArrayType>::Accumulator accumulator_;
        };



        class ByteArrayDictionaryRecordReader : public TypedRecordReader<ByteArrayType>,
                                                virtual public DictionaryRecordReader {
        public:
            ByteArrayDictionaryRecordReader(const ColumnDescriptor* descr, LevelInfo leaf_info,
                                            ::arrow::MemoryPool* pool)
                : TypedRecordReader<ByteArrayType>(descr, leaf_info, pool), builder_(pool) {
                this->read_dictionary_ = true;
            }

            std::shared_ptr<::arrow::ChunkedArray> GetResult() override {
                FlushBuilder();
                std::vector<std::shared_ptr<::arrow::Array>> result;
                std::swap(result, result_chunks_);
                return std::make_shared<::arrow::ChunkedArray>(std::move(result), builder_.type());
            }

            void FlushBuilder() {
                if (builder_.length() > 0) {
                    std::shared_ptr<::arrow::Array> chunk;
                    PARQUET_THROW_NOT_OK(builder_.Finish(&chunk));
                    result_chunks_.emplace_back(std::move(chunk));

                    // Also clears the dictionary memo table
                    builder_.Reset();
                }
            }

            void MaybeWriteNewDictionary() {
                if (this->new_dictionary_) {
                    /// If there is a new dictionary, we may need to flush the builder, then
                    /// insert the new dictionary values
                    FlushBuilder();
                    builder_.ResetFull();
                    auto decoder = dynamic_cast<BinaryDictDecoder*>(this->current_decoder_);
                    decoder->InsertDictionary(&builder_);
                    this->new_dictionary_ = false;
                }
            }

            void ReadValuesDense(int64_t values_to_read) override {
                int64_t num_decoded = 0;
                if (current_encoding_ == Encoding::RLE_DICTIONARY) {
                    MaybeWriteNewDictionary();
                    auto decoder = dynamic_cast<BinaryDictDecoder*>(this->current_decoder_);
                    num_decoded = decoder->DecodeIndices(static_cast<int>(values_to_read), &builder_);
                } else {
                    num_decoded = this->current_decoder_->DecodeArrowNonNull(
                        static_cast<int>(values_to_read), &builder_);

                    /// Flush values since they have been copied into the builder
                    ResetValues();
                }
                DCHECK_EQ(num_decoded, values_to_read);
            }

            void ReadValuesSpaced(int64_t values_to_read, int64_t null_count) override {
                int64_t num_decoded = 0;
                if (current_encoding_ == Encoding::RLE_DICTIONARY) {
                    MaybeWriteNewDictionary();
                    auto decoder = dynamic_cast<BinaryDictDecoder*>(this->current_decoder_);
                    num_decoded = decoder->DecodeIndicesSpaced(
                        static_cast<int>(values_to_read), static_cast<int>(null_count),
                        valid_bits_->mutable_data(), values_written_, &builder_);
                } else {
                    num_decoded = this->current_decoder_->DecodeArrow(
                        static_cast<int>(values_to_read), static_cast<int>(null_count),
                        valid_bits_->mutable_data(), values_written_, &builder_);

                    /// Flush values since they have been copied into the builder
                    ResetValues();
                }
                DCHECK_EQ(num_decoded, values_to_read - null_count);
            }

        private:
            using BinaryDictDecoder = DictDecoder<ByteArrayType>;

            ::arrow::BinaryDictionary32Builder builder_;
            std::vector<std::shared_ptr<::arrow::Array>> result_chunks_;
        };

        // TODO(wesm): Implement these to some satisfaction
        template <>
        void TypedRecordReader<Int96Type>::DebugPrintState() {}

        template <>
        void TypedRecordReader<ByteArrayType>::DebugPrintState() {}

        template <>
        void TypedRecordReader<FLBAType>::DebugPrintState() {}

        std::shared_ptr<RecordReader> MakeByteArrayRecordReader(const ColumnDescriptor* descr,
                                                                LevelInfo leaf_info,
                                                                ::arrow::MemoryPool* pool,
                                                                bool read_dictionary) {
            if (read_dictionary) {
                return std::make_shared<ByteArrayDictionaryRecordReader>(descr, leaf_info, pool);
            } else if (descr->logical_type()->type() == LogicalType::Type::type::STRING) {
                return std::make_shared<CHByteArrayChunkedRecordReader>(descr, leaf_info, pool);
            } else {
                return std::make_shared<ByteArrayChunkedRecordReader>(descr, leaf_info, pool);
            }
        }

    }  // namespace

    std::shared_ptr<RecordReader> RecordReader::Make(const ColumnDescriptor* descr,
                                                     LevelInfo leaf_info, MemoryPool* pool,
                                                     const bool read_dictionary) {
        switch (descr->physical_type()) {
            case Type::BOOLEAN:
                return std::make_shared<TypedRecordReader<BooleanType>>(descr, leaf_info, pool);
            case Type::INT32:
                return std::make_shared<TypedRecordReader<Int32Type>>(descr, leaf_info, pool);
            case Type::INT64:
                return std::make_shared<TypedRecordReader<Int64Type>>(descr, leaf_info, pool);
            case Type::INT96:
                return std::make_shared<TypedRecordReader<Int96Type>>(descr, leaf_info, pool);
            case Type::FLOAT:
                return std::make_shared<TypedRecordReader<FloatType>>(descr, leaf_info, pool);
            case Type::DOUBLE:
                return std::make_shared<TypedRecordReader<DoubleType>>(descr, leaf_info, pool);
            case Type::BYTE_ARRAY:
                return MakeByteArrayRecordReader(descr, leaf_info, pool, read_dictionary);
            case Type::FIXED_LEN_BYTE_ARRAY:
                return std::make_shared<FLBARecordReader>(descr, leaf_info, pool);
            default: {
                // PARQUET-1481: This can occur if the file is corrupt
                std::stringstream ss;
                ss << "Invalid physical column type: " << static_cast<int>(descr->physical_type());
                throw ParquetException(ss.str());
            }
        }
        // Unreachable code, but suppress compiler warning
        return nullptr;
    }

}  // namespace internal
}  // namespace parquet
