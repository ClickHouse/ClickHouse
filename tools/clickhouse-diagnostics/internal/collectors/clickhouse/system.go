package clickhouse

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/collectors"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/config"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/data"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/utils"
	"github.com/pkg/errors"
)

// This collector collects the system db from database

type SystemDatabaseCollector struct {
	resourceManager *platform.ResourceManager
}

const SystemDatabase = "system"

// ExcludeColumns columns if we need - this will be refined over time [table_name][columnA, columnB]
var ExcludeColumns = map[string][]string{}

// BannedTables - Hardcoded list. These are always excluded even if the user doesn't specify in exclude_tables.
//Attempts to export will work but we will warn
var BannedTables = []string{"numbers", "zeros"}

// OrderBy contains a map of tables to an order by clause - by default we don't order table dumps
var OrderBy = map[string]data.OrderBy{
	"errors": {
		Column: "last_error_message",
		Order:  data.Desc,
	},
	"replication_queue": {
		Column: "create_time",
		Order:  data.Asc,
	},
}

func NewSystemDatabaseCollector(m *platform.ResourceManager) *SystemDatabaseCollector {
	return &SystemDatabaseCollector{
		resourceManager: m,
	}
}

func (sc *SystemDatabaseCollector) Collect(conf config.Configuration) (*data.DiagnosticBundle, error) {
	conf, err := conf.ValidateConfig(sc.Configuration())
	if err != nil {
		return &data.DiagnosticBundle{}, err
	}
	includeTables, err := config.ReadStringListValues(conf, "include_tables")
	if err != nil {
		return &data.DiagnosticBundle{}, err
	}
	excludeTables, err := config.ReadStringListValues(conf, "exclude_tables")
	if err != nil {
		return &data.DiagnosticBundle{}, err
	}
	rowLimit, err := config.ReadIntValue(conf, "row_limit")
	if err != nil {
		return &data.DiagnosticBundle{}, err
	}
	excludeTables = checkBannedTables(includeTables, excludeTables)
	ds, err := sc.readSystemAllTables(includeTables, excludeTables, rowLimit)
	if err != nil {
		return &data.DiagnosticBundle{}, err
	}
	return ds, nil
}

// all banned tables are added to excluded if not present and not specified in included. Returns new exclude_tables list.
func checkBannedTables(includeTables []string, excludeTables []string) []string {
	for _, bannedTable := range BannedTables {
		//if its specified we don't add to our exclude list - explicitly included tables take precedence
		if !utils.Contains(includeTables, bannedTable) && !utils.Contains(excludeTables, bannedTable) {
			excludeTables = append(excludeTables, bannedTable)
		}
	}
	return excludeTables
}

func (sc *SystemDatabaseCollector) readSystemAllTables(include []string, exclude []string, limit int64) (*data.DiagnosticBundle, error) {
	tableNames, err := sc.resourceManager.DbClient.ReadTableNamesForDatabase(SystemDatabase)
	if err != nil {
		return nil, err
	}
	var frameErrors []error
	if include != nil {
		// nil means include everything
		tableNames = utils.Intersection(tableNames, include)
		if len(tableNames) != len(include) {
			// we warn that some included tables aren't present in db
			frameErrors = append(frameErrors, fmt.Errorf("some tables specified in the include_tables are not in the system database and will not be exported: %v",
				utils.Distinct(include, tableNames)))
		}
	}

	// exclude tables unless specified in includes
	excludedTables := utils.Distinct(exclude, include)
	tableNames = utils.Distinct(tableNames, excludedTables)
	frames := make(map[string]data.Frame)

	for _, tableName := range tableNames {
		var excludeColumns []string
		if _, ok := ExcludeColumns[tableName]; ok {
			excludeColumns = ExcludeColumns[tableName]
		}
		orderBy := data.OrderBy{}
		if _, ok := OrderBy[tableName]; ok {
			orderBy = OrderBy[tableName]
		}
		frame, err := sc.resourceManager.DbClient.ReadTable(SystemDatabase, tableName, excludeColumns, orderBy, limit)
		if err != nil {
			frameErrors = append(frameErrors, errors.Wrapf(err, "Unable to collect %s", tableName))
		} else {
			frames[tableName] = frame
		}
	}

	fErrors := data.FrameErrors{
		Errors: frameErrors,
	}
	return &data.DiagnosticBundle{
		Frames: frames,
		Errors: fErrors,
	}, nil
}

func (sc *SystemDatabaseCollector) Configuration() config.Configuration {
	return config.Configuration{
		Params: []config.ConfigParam{
			config.StringListParam{
				// nil means include everything
				Values: nil,
				Param:  config.NewParam("include_tables", "Specify list of tables to collect. Takes precedence over exclude_tables. If not specified (default) all tables except exclude_tables.", false),
			},
			config.StringListParam{
				Values: []string{"licenses", "distributed_ddl_queue", "query_thread_log", "query_log", "asynchronous_metric_log", "zookeeper", "aggregate_function_combinators", "collations", "contributors", "data_type_families", "formats", "graphite_retentions", "numbers", "numbers_mt", "one", "parts_columns", "projection_parts", "projection_parts_columns", "table_engines", "time_zones", "zeros", "zeros_mt"},
				Param:  config.NewParam("exclude_tables", "Specify list of tables to not collect.", false),
			},
			config.IntParam{
				Value: 100000,
				Param: config.NewParam("row_limit", "Maximum number of rows to collect from any table. Negative values mean unlimited.", false),
			},
		},
	}
}

func (sc *SystemDatabaseCollector) IsDefault() bool {
	return true
}

func (sc *SystemDatabaseCollector) Description() string {
	return "Collects all tables in the system database, except those which have been excluded."
}

// here we register the collector for use
func init() {
	collectors.Register("system_db", func() (collectors.Collector, error) {
		return &SystemDatabaseCollector{
			resourceManager: platform.GetResourceManager(),
		}, nil
	})
}
