// Parquet writers implemented in Go, used by utils/data-lake-corpus/generate.py.
//
// Usage: go_writer <job.json>
//
// A job with "library": "arrow-go" converts the input Arrow IPC table with
// github.com/apache/arrow-go (pqarrow) using the given writer properties, and a job
// with "library": "parquet-go" writes the flat dataset with github.com/parquet-go/parquet-go
// (a separate implementation). Both read the file back with the same library and write the
// readback as Arrow IPC next to the Parquet file.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

type Job struct {
	Name    string                 `json:"name"`
	Input   string                 `json:"input"`
	Library string                 `json:"library"`
	Options map[string]interface{} `json:"options"`
	// The writer is known to produce a file that no reader accepts; do not read it back.
	Corrupt bool `json:"corrupt"`
}

type JobFile struct {
	InputDir  string `json:"input_dir"`
	OutputDir string `json:"output_dir"`
	Result    string `json:"result"`
	Jobs      []Job  `json:"jobs"`
}

type Result struct {
	Name string `json:"name"`
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func readIPC(path string) arrow.Table {
	f, err := os.Open(path)
	check(err)
	defer f.Close()
	r, err := ipc.NewFileReader(f, ipc.WithAllocator(memory.DefaultAllocator))
	check(err)
	defer r.Close()
	recs := make([]arrow.Record, 0, r.NumRecords())
	for i := 0; i < r.NumRecords(); i++ {
		rec, err := r.Record(i)
		check(err)
		rec.Retain()
		recs = append(recs, rec)
	}
	return array.NewTableFromRecords(r.Schema(), recs)
}

func writeIPC(tbl arrow.Table, path string) {
	f, err := os.Create(path)
	check(err)
	defer f.Close()
	w, err := ipc.NewFileWriter(f, ipc.WithSchema(tbl.Schema()))
	check(err)
	tr := array.NewTableReader(tbl, 1<<20)
	defer tr.Release()
	for tr.Next() {
		check(w.Write(tr.Record()))
	}
	check(w.Close())
}

var codecs = map[string]compress.Compression{
	"none": compress.Codecs.Uncompressed, "snappy": compress.Codecs.Snappy, "gzip": compress.Codecs.Gzip,
	"brotli": compress.Codecs.Brotli, "zstd": compress.Codecs.Zstd, "lz4_raw": compress.Codecs.Lz4Raw,
}

var encodings = map[string]parquet.Encoding{
	"PLAIN": parquet.Encodings.Plain, "DELTA_BINARY_PACKED": parquet.Encodings.DeltaBinaryPacked,
	"DELTA_LENGTH_BYTE_ARRAY": parquet.Encodings.DeltaLengthByteArray, "DELTA_BYTE_ARRAY": parquet.Encodings.DeltaByteArray,
	"BYTE_STREAM_SPLIT": parquet.Encodings.ByteStreamSplit, "RLE": parquet.Encodings.RLE,
}

func num(v interface{}) int64 { return int64(v.(float64)) }

func arrowGo(job JobFile, j Job) {
	tbl := readIPC(filepath.Join(job.InputDir, j.Input))
	defer tbl.Release()
	props := []parquet.WriterProperty{}
	arrowProps := []pqarrow.WriterOption{pqarrow.WithStoreSchema()}
	rowGroup := int64(1 << 20)
	for k, v := range j.Options {
		switch k {
		case "compression":
			props = append(props, parquet.WithCompression(codecs[v.(string)]))
		case "version":
			props = append(props, parquet.WithVersion(map[string]parquet.Version{"1.0": parquet.V1_0, "2.4": parquet.V2_4, "2.6": parquet.V2_6}[v.(string)]))
		case "data_page_version":
			props = append(props, parquet.WithDataPageVersion(map[string]parquet.DataPageVersion{"1.0": parquet.DataPageV1, "2.0": parquet.DataPageV2}[v.(string)]))
		case "dictionary":
			props = append(props, parquet.WithDictionaryDefault(v.(bool)))
		case "dictionary_page_size":
			props = append(props, parquet.WithDictionaryPageSizeLimit(num(v)))
		case "encoding":
			for col, enc := range v.(map[string]interface{}) {
				props = append(props, parquet.WithDictionaryFor(col, false), parquet.WithEncodingFor(col, encodings[enc.(string)]))
			}
		case "stats":
			props = append(props, parquet.WithStats(v.(bool)))
		case "data_page_size":
			props = append(props, parquet.WithDataPageSize(num(v)))
		case "batch_size":
			props = append(props, parquet.WithBatchSize(num(v)))
		case "row_group_size":
			rowGroup = num(v)
		case "page_index":
			props = append(props, parquet.WithPageIndexEnabled(v.(bool)))
		case "bloom_filter":
			for _, col := range v.([]interface{}) {
				props = append(props, parquet.WithBloomFilterEnabledFor(col.(string), true))
			}
		case "bloom_ndv":
			props = append(props, parquet.WithBloomFilterNDV(num(v)))
		case "int96":
			arrowProps = append(arrowProps, pqarrow.WithDeprecatedInt96Timestamps(v.(bool)))
		case "decimal_as_integer":
			props = append(props, parquet.WithStoreDecimalAsInteger(v.(bool)))
		default:
			panic("unknown option " + k)
		}
	}
	out := filepath.Join(job.OutputDir, j.Name+".parquet")
	f, err := os.Create(out)
	check(err)
	// WriteTable closes the sink.
	check(pqarrow.WriteTable(tbl, f, rowGroup, parquet.NewWriterProperties(props...), pqarrow.NewArrowWriterProperties(arrowProps...)))

	rdr, err := file.OpenParquetFile(out, false)
	check(err)
	fr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{BatchSize: 1 << 16}, memory.DefaultAllocator)
	check(err)
	back, err := fr.ReadTable(context.Background())
	check(err)
	writeIPC(back, filepath.Join(job.OutputDir, j.Name+".arrow"))
	back.Release()
	rdr.Close()
}

func main() {
	var job JobFile
	data, err := os.ReadFile(os.Args[1])
	check(err)
	check(json.Unmarshal(data, &job))
	results := []Result{}
	for _, j := range job.Jobs {
		switch j.Library {
		case "arrow-go":
			arrowGo(job, j)
		case "parquet-go":
			parquetGo(job, j)
		default:
			panic(fmt.Sprintf("unknown library %q", j.Library))
		}
		results = append(results, Result{Name: j.Name})
	}
	out, err := json.MarshalIndent(map[string]interface{}{"library": "go", "version": versions(), "results": results}, "", " ")
	check(err)
	check(os.WriteFile(job.Result, out, 0o644))
}
