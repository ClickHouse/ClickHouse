package system

import (
	"bytes"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/collectors"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/config"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/data"
	"github.com/google/shlex"
	"github.com/pkg/errors"
	"os/exec"
)

// This collector runs a user specified command and collects it to a file

type CommandCollector struct {
	resourceManager *platform.ResourceManager
}

func NewCommandCollector(m *platform.ResourceManager) *CommandCollector {
	return &CommandCollector{
		resourceManager: m,
	}
}

func (c *CommandCollector) Collect(conf config.Configuration) (*data.DiagnosticBundle, error) {
	conf, err := conf.ValidateConfig(c.Configuration())
	if err != nil {
		return &data.DiagnosticBundle{}, err
	}
	command, err := config.ReadStringValue(conf, "command")
	if err != nil {
		return &data.DiagnosticBundle{}, err
	}
	var frameErrors []error
	// shlex to split the commands and args
	cmdArgs, err := shlex.Split(command)
	if err != nil || len(cmdArgs) == 0 {
		return &data.DiagnosticBundle{}, errors.Wrap(err, "Unable to parse command")
	}
	cmd := exec.Command(cmdArgs[0], cmdArgs[1:]...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err = cmd.Run()
	var sError string
	if err != nil {
		frameErrors = append(frameErrors, errors.Wrap(err, "Unable to execute command"))
		sError = err.Error()
	}
	memoryFrame := data.NewMemoryFrame("output", []string{"command", "stdout", "stderr", "error"}, [][]interface{}{
		{command, stdout.String(), stderr.String(), sError},
	})
	return &data.DiagnosticBundle{
		Errors: data.FrameErrors{Errors: frameErrors},
		Frames: map[string]data.Frame{
			"output": memoryFrame,
		},
	}, nil
}

func (c *CommandCollector) Configuration() config.Configuration {
	return config.Configuration{
		Params: []config.ConfigParam{
			config.StringParam{
				Value:      "",
				Param:      config.NewParam("command", "Command to execute", true),
				AllowEmpty: false,
			},
		},
	}
}

func (c *CommandCollector) IsDefault() bool {
	return false
}

func (c *CommandCollector) Description() string {
	return "Allows collection of the output from a user specified command"
}

// here we register the collector for use
func init() {
	collectors.Register("command", func() (collectors.Collector, error) {
		return &CommandCollector{
			resourceManager: platform.GetResourceManager(),
		}, nil
	})
}
