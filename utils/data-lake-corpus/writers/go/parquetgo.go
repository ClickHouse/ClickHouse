package main

import (
	"os"
	"path/filepath"
	"runtime/debug"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	pq "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/compress/brotli"
	"github.com/parquet-go/parquet-go/compress/gzip"
	"github.com/parquet-go/parquet-go/compress/lz4"
	"github.com/parquet-go/parquet-go/compress/snappy"
	"github.com/parquet-go/parquet-go/compress/uncompressed"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/parquet-go/parquet-go/encoding"
)

// Flat is the subset of the `flat` dataset that parquet-go writes from Go structs.
type Flat struct {
	ID    int64    `parquet:"id"`
	B     *bool    `parquet:"b,optional"`
	// parquet-go has no 8 and 16 bit Go types, so these are written as 32 bit integers.
	I8    *int32   `parquet:"i8,optional"`
	I16   *int32   `parquet:"i16,optional"`
	I32   *int32   `parquet:"i32,optional"`
	I64   *int64   `parquet:"i64,optional"`
	U8    *uint32  `parquet:"u8,optional"`
	U16   *uint32  `parquet:"u16,optional"`
	U32   *uint32  `parquet:"u32,optional"`
	U64   *uint64  `parquet:"u64,optional"`
	F32   *float32 `parquet:"f32,optional"`
	F64   *float64 `parquet:"f64,optional"`
	S     *string  `parquet:"s,optional"`
	SDict *string  `parquet:"s_dict,optional"`
	Bin   []byte   `parquet:"bin,optional"`
	// parquet-go accepts the `date` and `timestamp` tags only on non-pointer fields, so these
	// two columns are REQUIRED and a null in the input is written as zero.
	D    int32 `parquet:"d,date"`
	TsUs int64 `parquet:"ts_us,timestamp(microsecond)"`
}

func ptr[T any](v T) *T { return &v }

func flatFromArrow(tbl arrow.Table) []Flat {
	rows := make([]Flat, tbl.NumRows())
	col := func(name string) *arrow.Chunked {
		idx := tbl.Schema().FieldIndices(name)
		if len(idx) == 0 {
			panic("no column " + name)
		}
		return tbl.Column(idx[0]).Data()
	}
	each := func(name string, f func(i int, a arrow.Array, j int)) {
		i := 0
		for _, chunk := range col(name).Chunks() {
			for j := 0; j < chunk.Len(); j++ {
				if !chunk.IsNull(j) {
					f(i, chunk, j)
				}
				i++
			}
		}
	}
	each("id", func(i int, a arrow.Array, j int) { rows[i].ID = a.(*array.Int64).Value(j) })
	each("b", func(i int, a arrow.Array, j int) { rows[i].B = ptr(a.(*array.Boolean).Value(j)) })
	each("i8", func(i int, a arrow.Array, j int) { rows[i].I8 = ptr(int32(a.(*array.Int8).Value(j))) })
	each("i16", func(i int, a arrow.Array, j int) { rows[i].I16 = ptr(int32(a.(*array.Int16).Value(j))) })
	each("i32", func(i int, a arrow.Array, j int) { rows[i].I32 = ptr(a.(*array.Int32).Value(j)) })
	each("i64", func(i int, a arrow.Array, j int) { rows[i].I64 = ptr(a.(*array.Int64).Value(j)) })
	each("u8", func(i int, a arrow.Array, j int) { rows[i].U8 = ptr(uint32(a.(*array.Uint8).Value(j))) })
	each("u16", func(i int, a arrow.Array, j int) { rows[i].U16 = ptr(uint32(a.(*array.Uint16).Value(j))) })
	each("u32", func(i int, a arrow.Array, j int) { rows[i].U32 = ptr(a.(*array.Uint32).Value(j)) })
	each("u64", func(i int, a arrow.Array, j int) { rows[i].U64 = ptr(a.(*array.Uint64).Value(j)) })
	each("f32", func(i int, a arrow.Array, j int) { rows[i].F32 = ptr(a.(*array.Float32).Value(j)) })
	each("f64", func(i int, a arrow.Array, j int) { rows[i].F64 = ptr(a.(*array.Float64).Value(j)) })
	each("s", func(i int, a arrow.Array, j int) { rows[i].S = ptr(a.(*array.String).Value(j)) })
	each("s_dict", func(i int, a arrow.Array, j int) { rows[i].SDict = ptr(a.(*array.String).Value(j)) })
	each("bin", func(i int, a arrow.Array, j int) { rows[i].Bin = append([]byte{}, a.(*array.Binary).Value(j)...) })
	each("d", func(i int, a arrow.Array, j int) { rows[i].D = int32(a.(*array.Date32).Value(j)) })
	each("ts_us", func(i int, a arrow.Array, j int) { rows[i].TsUs = int64(a.(*array.Timestamp).Value(j)) })
	return rows
}

func flatToArrow(rows []Flat) arrow.Table {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "b", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "i8", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "i16", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "i32", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "i64", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "u8", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
		{Name: "u16", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
		{Name: "u32", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
		{Name: "u64", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
		{Name: "f32", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
		{Name: "f64", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "s_dict", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "bin", Type: arrow.BinaryTypes.Binary, Nullable: true},
		{Name: "d", Type: arrow.FixedWidthTypes.Date32},
		{Name: "ts_us", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for _, r := range rows {
		b.Field(0).(*array.Int64Builder).Append(r.ID)
		appendOpt(b.Field(1).(*array.BooleanBuilder), r.B)
		appendOpt(b.Field(2).(*array.Int32Builder), r.I8)
		appendOpt(b.Field(3).(*array.Int32Builder), r.I16)
		appendOpt(b.Field(4).(*array.Int32Builder), r.I32)
		appendOpt(b.Field(5).(*array.Int64Builder), r.I64)
		appendOpt(b.Field(6).(*array.Uint32Builder), r.U8)
		appendOpt(b.Field(7).(*array.Uint32Builder), r.U16)
		appendOpt(b.Field(8).(*array.Uint32Builder), r.U32)
		appendOpt(b.Field(9).(*array.Uint64Builder), r.U64)
		appendOpt(b.Field(10).(*array.Float32Builder), r.F32)
		appendOpt(b.Field(11).(*array.Float64Builder), r.F64)
		appendOpt(b.Field(12).(*array.StringBuilder), r.S)
		appendOpt(b.Field(13).(*array.StringBuilder), r.SDict)
		if r.Bin == nil {
			b.Field(14).AppendNull()
		} else {
			b.Field(14).(*array.BinaryBuilder).Append(r.Bin)
		}
		b.Field(15).(*array.Date32Builder).Append(arrow.Date32(r.D))
		b.Field(16).(*array.TimestampBuilder).Append(arrow.Timestamp(r.TsUs))
	}
	rec := b.NewRecord()
	return array.NewTableFromRecords(schema, []arrow.Record{rec})
}

type appender[T any] interface {
	Append(T)
	AppendNull()
}

func appendOpt[T any](b appender[T], v *T) {
	if v == nil {
		b.AppendNull()
	} else {
		b.Append(*v)
	}
}

var pqCodecs = map[string]compress.Codec{
	"none": &uncompressed.Codec{}, "snappy": &snappy.Codec{}, "gzip": &gzip.Codec{}, "brotli": &brotli.Codec{},
	"lz4_raw": &lz4.Codec{}, "zstd": &zstd.Codec{},
}

var pqEncodings = map[string]encoding.Encoding{
	"PLAIN": &pq.Plain, "RLE_DICTIONARY": &pq.RLEDictionary, "DELTA_BINARY_PACKED": &pq.DeltaBinaryPacked,
	"DELTA_LENGTH_BYTE_ARRAY": &pq.DeltaLengthByteArray, "DELTA_BYTE_ARRAY": &pq.DeltaByteArray,
	"BYTE_STREAM_SPLIT": &pq.ByteStreamSplit, "RLE": &pq.RLE,
}

var pqKinds = map[string]pq.Kind{
	"bool": pq.Boolean, "int32": pq.Int32, "int64": pq.Int64, "float": pq.Float, "double": pq.Double, "bytes": pq.ByteArray,
}

func parquetGo(job JobFile, j Job) {
	tbl := readIPC(filepath.Join(job.InputDir, j.Input))
	rows := flatFromArrow(tbl)
	tbl.Release()
	opts := []pq.WriterOption{}
	for k, v := range j.Options {
		switch k {
		case "compression":
			opts = append(opts, pq.Compression(pqCodecs[v.(string)]))
		case "data_page_version":
			opts = append(opts, pq.DataPageVersion(int(num(v))))
		case "page_buffer_size":
			opts = append(opts, pq.PageBufferSize(int(num(v))))
		case "row_group_size":
			opts = append(opts, pq.MaxRowsPerRowGroup(num(v)))
		case "statistics":
			opts = append(opts, pq.DataPageStatistics(v.(bool)))
		case "encoding":
			for kind, enc := range v.(map[string]interface{}) {
				opts = append(opts, pq.DefaultEncodingFor(pqKinds[kind], pqEncodings[enc.(string)]))
			}
		case "bloom_filter":
			for _, c := range v.([]interface{}) {
				opts = append(opts, pq.BloomFilters(pq.SplitBlockFilter(10, c.(string))))
			}
		default:
			panic("unknown option " + k)
		}
	}
	out := filepath.Join(job.OutputDir, j.Name+".parquet")
	f, err := os.Create(out)
	check(err)
	w := pq.NewGenericWriter[Flat](f, opts...)
	rowGroup := 0
	if v, ok := j.Options["row_group_size"]; ok {
		rowGroup = int(num(v))
	}
	for start := 0; start < len(rows); {
		end := len(rows)
		if rowGroup > 0 && start+rowGroup < end {
			end = start + rowGroup
		}
		_, err = w.Write(rows[start:end])
		check(err)
		check(w.Flush())
		start = end
	}
	check(w.Close())
	check(f.Close())

	if j.Corrupt {
		return
	}
	back, err := pq.ReadFile[Flat](out)
	check(err)
	writeIPC(flatToArrow(back), filepath.Join(job.OutputDir, j.Name+".arrow"))
}

func versions() map[string]string {
	out := map[string]string{}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		panic("no build info")
	}
	out["go"] = info.GoVersion
	for _, d := range info.Deps {
		if d.Path == "github.com/apache/arrow-go/v18" {
			out["arrow-go"] = d.Version
		}
		if d.Path == "github.com/parquet-go/parquet-go" {
			out["parquet-go"] = d.Version
		}
	}
	return out
}
