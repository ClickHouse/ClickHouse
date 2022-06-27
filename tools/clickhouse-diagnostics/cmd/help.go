package cmd

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-diagnostics/cmd/params"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/collectors"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/outputs"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/config"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"os"
)

var cHelp = params.StringOptionsVar{
	Options: collectors.GetCollectorNames(false),
	Value:   "",
}
var oHelp = params.StringOptionsVar{
	Options: outputs.GetOutputNames(),
	Value:   "",
}

func init() {
	helpCmd.Flags().VarP(&cHelp, "collector", "c", "Specify collector to get description of available flags")
	helpCmd.Flags().VarP(&oHelp, "output", "o", "Specify output to get description of available flags")
	helpCmd.SetUsageTemplate(`Usage:{{if .Runnable}}
  {{.UseLine}}{{end}}{{if .HasExample}}

Examples:
{{.Example}}{{end}}

Available Commands:{{range .Parent.Commands}}{{if (or .IsAvailableCommand (eq .Name "help"))}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{if .HasAvailableLocalFlags}}

Flags:
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}

Alternatively use "{{.CommandPath}} [command] --help" for more information about a command.
`)
	helpCmd.SetFlagErrorFunc(handleFlagErrors)

}

var helpCmd = &cobra.Command{
	Use:   "help [command]",
	Short: "Help about any command, collector or output",
	Long:  `Help provides help for any command, collector or output in the application.`,
	Example: fmt.Sprintf(`%[1]v help collect
%[1]v help --collector=config
%[1]v help --output=simple`, rootCmd.Name()),
	Run: func(c *cobra.Command, args []string) {
		if len(args) != 0 {
			//find the command on which help is requested
			cmd, _, e := c.Root().Find(args)
			if cmd == nil || e != nil {
				c.Printf("Unknown help topic %#q\n", args)
				cobra.CheckErr(c.Root().Usage())
			} else {
				cmd.InitDefaultHelpFlag()
				cobra.CheckErr(cmd.Help())
			}
			return
		}
		if cHelp.Value != "" && oHelp.Value != "" {
			log.Error().Msg("Specify either --collector or --output not both")
			_ = c.Help()
			os.Exit(1)
		}
		if cHelp.Value != "" {
			collector, err := collectors.GetCollectorByName(cHelp.Value)
			if err != nil {
				log.Fatal().Err(err).Msgf("Unable to initialize collector %s", cHelp.Value)
			}
			configHelp(collector.Configuration(), "collector", cHelp.Value, collector.Description())
		} else if oHelp.Value != "" {
			output, err := outputs.GetOutputByName(oHelp.Value)
			if err != nil {
				log.Fatal().Err(err).Msgf("Unable to initialize output %s", oHelp.Value)
			}
			configHelp(output.Configuration(), "output", oHelp.Value, output.Description())
		} else {
			_ = c.Help()
		}
		os.Exit(0)
	},
}

func configHelp(conf config.Configuration, componentType, name, description string) {

	paramMap := params.NewParamMap(map[string]config.Configuration{
		name: conf,
	})
	tempHelpCmd := &cobra.Command{
		Use:           fmt.Sprintf("--%s=%s", componentType, name),
		Short:         fmt.Sprintf("Help about the %s collector", name),
		Long:          description,
		SilenceErrors: true,
		Run: func(c *cobra.Command, args []string) {
			_ = c.Help()
		},
	}
	params.AddParamMapToCmd(paramMap, tempHelpCmd, componentType, false)
	// this is workaround to hide the help flag
	tempHelpCmd.Flags().BoolP("help", "h", false, "Dummy help")
	tempHelpCmd.Flags().Lookup("help").Hidden = true
	tempHelpCmd.SetUsageTemplate(`
{{.Long}}

Usage:{{if .Runnable}}
  {{.UseLine}}{{end}}{{if .HasExample}}

Examples:
{{.Example}}{{end}}

Flags:{{if .HasAvailableLocalFlags}}
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}{{else}}

	No configuration flags available
{{end}}
`)

	_ = tempHelpCmd.Execute()
}
