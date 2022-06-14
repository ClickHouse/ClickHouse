package terminal

import (
	"bufio"
	"fmt"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/outputs"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/config"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/data"
	"github.com/olekukonko/tablewriter"
	"github.com/pkg/errors"
	"os"
)

const OutputName = "report"

type ReportOutput struct {
}

func (r ReportOutput) Write(id string, bundles map[string]*data.DiagnosticBundle, conf config.Configuration) (data.FrameErrors, error) {
	conf, err := conf.ValidateConfig(r.Configuration())
	if err != nil {
		return data.FrameErrors{}, err
	}
	format, err := config.ReadStringOptionsValue(conf, "format")
	if err != nil {
		return data.FrameErrors{}, err
	}
	nonInteractive, err := config.ReadBoolValue(conf, "continue")
	if err != nil {
		return data.FrameErrors{}, err
	}
	maxRows, err := config.ReadIntValue(conf, "row_limit")
	if err != nil {
		return data.FrameErrors{}, err
	}
	maxColumns, err := config.ReadIntValue(conf, "column_limit")
	if err != nil {
		return data.FrameErrors{}, err
	}
	frameErrors := data.FrameErrors{}
	for name := range bundles {
		frameError := printDiagnosticBundle(name, bundles[name], format, !nonInteractive, int(maxRows), int(maxColumns))
		frameErrors.Errors = append(frameErrors.Errors, frameError.Errors...)
	}
	return data.FrameErrors{}, nil
}

func printDiagnosticBundle(name string, diag *data.DiagnosticBundle, format string, interactive bool, maxRows, maxColumns int) data.FrameErrors {
	frameErrors := data.FrameErrors{}
	for frameId, frame := range diag.Frames {
		printFrameHeader(fmt.Sprintf("%s.%s", name, frameId))
		err := printFrame(frame, format, maxRows, maxColumns)
		if err != nil {
			frameErrors.Errors = append(frameErrors.Errors, err)
		}
		if interactive {
			err := waitForEnter()
			if err != nil {
				frameErrors.Errors = append(frameErrors.Errors, err)
			}
		}
	}
	return frameErrors
}

func waitForEnter() error {
	fmt.Println("Press the Enter Key to view the next frame report")
	for {
		consoleReader := bufio.NewReaderSize(os.Stdin, 1)
		input, err := consoleReader.ReadByte()
		if err != nil {
			return errors.New("Unable to read user input")
		}
		if input == 3 {
			//ctl +c
			fmt.Println("Exiting...")
			os.Exit(0)
		}
		if input == 10 {
			return nil
		}
	}
}

func printFrame(frame data.Frame, format string, maxRows, maxColumns int) error {
	switch f := frame.(type) {
	case data.DatabaseFrame:
		return printDatabaseFrame(f, format, maxRows, maxColumns)
	case data.ConfigFileFrame:
		return printConfigFrame(f, format)
	case data.DirectoryFileFrame:
		return printDirectoryFileFrame(f, format, maxRows)
	case data.HierarchicalFrame:
		return printHierarchicalFrame(f, format, maxRows, maxColumns)
	default:
		// for now our data frame writer supports all frames
		return printDatabaseFrame(f, format, maxRows, maxColumns)
	}
}

func createTable(format string) *tablewriter.Table {
	table := tablewriter.NewWriter(os.Stdout)
	if format == "markdown" {
		table.SetBorders(tablewriter.Border{Left: true, Top: false, Right: true, Bottom: false})
		table.SetCenterSeparator("|")
	}
	return table
}

func printFrameHeader(title string) {
	titleTable := tablewriter.NewWriter(os.Stdout)
	titleTable.SetHeader([]string{title})
	titleTable.SetAutoWrapText(false)
	titleTable.SetAutoFormatHeaders(true)
	titleTable.SetHeaderAlignment(tablewriter.ALIGN_CENTER)
	titleTable.SetRowSeparator("\n")
	titleTable.SetHeaderLine(false)
	titleTable.SetBorder(false)
	titleTable.SetTablePadding("\t") // pad with tabs
	titleTable.SetNoWhiteSpace(true)
	titleTable.Render()
}

func printHierarchicalFrame(frame data.HierarchicalFrame, format string, maxRows, maxColumns int) error {
	err := printDatabaseFrame(frame, format, maxRows, maxColumns)
	if err != nil {
		return err
	}
	for _, subFrame := range frame.SubFrames {
		err = printHierarchicalFrame(subFrame, format, maxRows, maxColumns)
		if err != nil {
			return err
		}
	}
	return nil
}

func printDatabaseFrame(frame data.Frame, format string, maxRows, maxColumns int) error {
	table := createTable(format)
	table.SetAutoWrapText(false)
	columns := len(frame.Columns())
	if maxColumns > 0 && maxColumns < columns {
		columns = maxColumns
	}
	table.SetHeader(frame.Columns()[:columns])
	r := 0
	trunColumns := 0
	for {
		values, ok, err := frame.Next()
		if !ok || r == maxRows {
			table.Render()
			if trunColumns > 0 {
				warning(fmt.Sprintf("Truncated %d columns, more available...", trunColumns))
			}
			if r == maxRows {
				warning("Truncated rows, more available...")
			}
			return err
		}
		if err != nil {
			return err
		}
		columns := len(values)
		// -1 means unlimited
		if maxColumns > 0 && maxColumns < columns {
			trunColumns = columns - maxColumns
			columns = maxColumns
		}
		row := make([]string, columns)
		for i, value := range values {
			if i == columns {
				break
			}
			row[i] = fmt.Sprintf("%v", value)
		}
		table.Append(row)
		r++
	}
}

// currently we dump the whole config - useless in parts
func printConfigFrame(frame data.Frame, format string) error {
	for {
		values, ok, err := frame.Next()
		if !ok {
			return err
		}
		if err != nil {
			return err
		}
		configFile := values[0].(data.File)
		dat, err := os.ReadFile(configFile.FilePath())
		if err != nil {
			return err
		}
		// create a table per row - as each will be a file
		table := createTable(format)
		table.SetAutoWrapText(false)
		table.SetAutoFormatHeaders(false)
		table.ClearRows()
		table.SetHeader([]string{configFile.FilePath()})
		table.Append([]string{string(dat)})
		table.Render()
	}
}

func printDirectoryFileFrame(frame data.Frame, format string, maxRows int) error {
	for {
		values, ok, err := frame.Next()
		if !ok {

			return err
		}
		if err != nil {
			return err
		}
		path := values[0].(data.SimpleFile)
		file, err := os.Open(path.FilePath())
		if err != nil {
			// failure on one file causes rest to be ignored in frame...we could improve this
			return errors.Wrapf(err, "Unable to read file %s", path.FilePath())
		}
		scanner := bufio.NewScanner(file)
		i := 0
		// create a table per row - as each will be a file
		table := createTable(format)
		table.SetAutoWrapText(false)
		table.SetAutoFormatHeaders(false)
		table.ClearRows()
		table.SetHeader([]string{path.FilePath()})
		for scanner.Scan() {
			if i == maxRows {
				fmt.Println()
				table.Render()
				warning("Truncated lines, more available...")
				fmt.Print("\n")
				break
			}
			table.Append([]string{scanner.Text()})
			i++
		}
	}
}

// prints a warning
func warning(s string) {
	fmt.Printf("\x1b[%dm%v\x1b[0m%s\n", 33, "WARNING: ", s)
}

func (r ReportOutput) Configuration() config.Configuration {
	return config.Configuration{
		Params: []config.ConfigParam{
			config.StringOptions{
				Value:   "default",
				Options: []string{"default", "markdown"},
				Param:   config.NewParam("format", "Format of tables. Default is terminal friendly.", false),
			},
			config.BoolParam{
				Value: false,
				Param: config.NewParam("continue", "Print report with no interaction", false),
			},
			config.IntParam{
				Value: 10,
				Param: config.NewParam("row_limit", "Max Rows to print per frame.", false),
			},
			config.IntParam{
				Value: 8,
				Param: config.NewParam("column_limit", "Max Columns to print per frame. Negative is unlimited.", false),
			},
		},
	}
}

func (r ReportOutput) Description() string {
	return "Writes out the diagnostic bundle to the terminal as a simple report."
}

// here we register the output for use
func init() {
	outputs.Register(OutputName, func() (outputs.Output, error) {
		return ReportOutput{}, nil
	})
}
