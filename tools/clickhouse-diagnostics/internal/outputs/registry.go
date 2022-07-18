package outputs

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/config"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/data"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

type Output interface {
	Write(id string, bundles map[string]*data.DiagnosticBundle, config config.Configuration) (data.FrameErrors, error)
	Configuration() config.Configuration
	Description() string
	// TODO: we will need to implement this for the convert function
	//Read(config config.Configuration) (data.DiagnosticBundle, error)
}

// Register can be called from init() on an output in this package
// It will automatically be added to the Outputs map to be called externally
func Register(name string, output OutputFactory) {
	// names must be unique
	if _, ok := Outputs[name]; ok {
		log.Error().Msgf("More than 1 output is trying to register under the name %s. Names must be unique.", name)
	}
	Outputs[name] = output
}

// OutputFactory lets us use a closure to get instances of the output struct
type OutputFactory func() (Output, error)

var Outputs = map[string]OutputFactory{}

func GetOutputNames() []string {
	outputs := make([]string, len(Outputs))
	i := 0
	for k := range Outputs {
		outputs[i] = k
		i++
	}
	return outputs
}

func GetOutputByName(name string) (Output, error) {
	if outputFactory, ok := Outputs[name]; ok {
		//do something here
		output, err := outputFactory()
		if err != nil {
			return nil, errors.Wrapf(err, "output %s could not be initialized", name)
		}
		return output, nil
	}
	return nil, fmt.Errorf("%s is not a valid output name", name)
}

func BuildConfigurationOptions() (map[string]config.Configuration, error) {
	configurations := make(map[string]config.Configuration)
	for name, collectorFactory := range Outputs {
		output, err := collectorFactory()
		if err != nil {
			return nil, errors.Wrapf(err, "output %s could not be initialized", name)
		}
		configurations[name] = output.Configuration()
	}
	return configurations, nil
}
