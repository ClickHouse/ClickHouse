package outputs_test

import (
	"github.com/ClickHouse/clickhouse-diagnostics/internal/outputs"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/outputs/file"
	_ "github.com/ClickHouse/clickhouse-diagnostics/internal/outputs/terminal"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGetOutputNames(t *testing.T) {
	t.Run("can get all output names", func(t *testing.T) {
		outputNames := outputs.GetOutputNames()
		require.ElementsMatch(t, []string{"simple", "report"}, outputNames)
	})

}

func TestGetOutputByName(t *testing.T) {

	t.Run("can get output by name", func(t *testing.T) {
		output, err := outputs.GetOutputByName("simple")
		require.Nil(t, err)
		require.Equal(t, file.SimpleOutput{}, output)
	})

	t.Run("fails on non existing output", func(t *testing.T) {
		output, err := outputs.GetOutputByName("random")
		require.NotNil(t, err)
		require.Equal(t, "random is not a valid output name", err.Error())
		require.Nil(t, output)
	})
}

func TestBuildConfigurationOptions(t *testing.T) {

	t.Run("can get all output configurations", func(t *testing.T) {
		outputs, err := outputs.BuildConfigurationOptions()
		require.Nil(t, err)
		require.Len(t, outputs, 2)
		require.Contains(t, outputs, "simple")
		require.Contains(t, outputs, "report")
	})
}
