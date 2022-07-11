package cmd

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/utils"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"time"
)

func enableDebug() {
	if debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
		go func() {
			err := http.ListenAndServe("localhost:8080", nil)
			if err != nil {
				log.Error().Err(err).Msg("unable to start debugger")
			} else {
				log.Debug().Msg("debugger has been started on port 8080")
			}
		}()
	}
}

var rootCmd = &cobra.Command{
	Use:   "clickhouse-diagnostics",
	Short: "Capture and convert ClickHouse diagnostic bundles.",
	Long:  `Captures ClickHouse diagnostic bundles to a number of supported formats, including file and ClickHouse itself. Converts bundles between formats.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		enableDebug()
		err := initializeConfig()
		if err != nil {
			log.Error().Err(err)
			os.Exit(1)
		}
		return nil
	},
	Example: `clickhouse-diagnostics collect`,
}

const (
	colorRed = iota + 31
	colorGreen
	colorYellow
	colorMagenta = 35

	colorBold = 1
)

const TimeFormat = time.RFC3339

var debug bool
var configFiles []string

const (
	// The environment variable prefix of all environment variables bound to our command line flags.
	// For example, --output is bound to CLICKHOUSE_DIAGNOSTIC_OUTPUT.
	envPrefix = "CLICKHOUSE_DIAGNOSTIC"
)

func init() {
	rootCmd.PersistentFlags().BoolVarP(&debug, "debug", "d", false, "Enable debug mode")
	rootCmd.PersistentFlags().StringSliceVarP(&configFiles, "config", "f", []string{"clickhouse-diagnostics.yml", "/etc/clickhouse-diagnostics.yml"}, "Configuration file path")
	// set a usage template to ensure flags on root are listed as global
	rootCmd.SetUsageTemplate(`Usage:{{if .Runnable}}
  {{.UseLine}}{{end}}{{if .HasAvailableSubCommands}}
  {{.CommandPath}} [command]{{end}}{{if gt (len .Aliases) 0}}

Aliases:
  {{.NameAndAliases}}{{end}}{{if .HasExample}}

Examples:
{{.Example}}{{end}}{{if .HasAvailableSubCommands}}

Available Commands:{{range .Commands}}{{if (or .IsAvailableCommand (eq .Name "help"))}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableLocalFlags}}

Global Flags:
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasAvailableInheritedFlags}}

Additional help topics:{{range .Commands}}{{if .IsAdditionalHelpTopicCommand}}
  {{rpad .CommandPath .CommandPathPadding}} {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableSubCommands}}

Use "{{.CommandPath}} [command] --help" for more information about a command.{{end}}
`)
	rootCmd.SetFlagErrorFunc(handleFlagErrors)

}

func Execute() {
	// logs go to stderr - stdout is exclusive for outputs e.g. tables
	output := zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: TimeFormat}
	// override the colors
	output.FormatLevel = func(i interface{}) string {
		var l string
		if ll, ok := i.(string); ok {
			switch ll {
			case zerolog.LevelTraceValue:
				l = colorize("TRC", colorMagenta)
			case zerolog.LevelDebugValue:
				l = colorize("DBG", colorMagenta)
			case zerolog.LevelInfoValue:
				l = colorize("INF", colorGreen)
			case zerolog.LevelWarnValue:
				l = colorize(colorize("WRN", colorYellow), colorBold)
			case zerolog.LevelErrorValue:
				l = colorize(colorize("ERR", colorRed), colorBold)
			case zerolog.LevelFatalValue:
				l = colorize(colorize("FTL", colorRed), colorBold)
			case zerolog.LevelPanicValue:
				l = colorize(colorize("PNC", colorRed), colorBold)
			default:
				l = colorize("???", colorBold)
			}
		} else {
			if i == nil {
				l = colorize("???", colorBold)
			} else {
				l = strings.ToUpper(fmt.Sprintf("%s", i))[0:3]
			}
		}
		return l
	}
	output.FormatTimestamp = func(i interface{}) string {
		tt := i.(string)
		return colorize(tt, colorGreen)
	}
	log.Logger = log.Output(output)
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	rootCmd.SetHelpCommand(helpCmd)
	if err := rootCmd.Execute(); err != nil {
		log.Fatal().Err(err)
	}
}

// colorize returns the string s wrapped in ANSI code c
func colorize(s interface{}, c int) string {
	return fmt.Sprintf("\x1b[%dm%v\x1b[0m", c, s)
}

func handleFlagErrors(cmd *cobra.Command, err error) error {
	fmt.Println(colorize(colorize(fmt.Sprintf("Error: %s\n", err), colorRed), colorBold))
	_ = cmd.Help()
	os.Exit(1)
	return nil
}

func initializeConfig() error {
	// we use the first config file we find
	var configFile string
	for _, confFile := range configFiles {
		if ok, _ := utils.FileExists(confFile); ok {
			configFile = confFile
			break
		}
	}
	if configFile == "" {
		log.Warn().Msgf("config file in %s not found - config file will be ignored", configFiles)
		return nil
	}
	viper.SetConfigFile(configFile)
	if err := viper.ReadInConfig(); err != nil {
		return errors.Wrapf(err, "Unable to read configuration file at %s", configFile)
	}
	return nil
}
