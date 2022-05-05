package clickhouse

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/collectors"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/config"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/data"
	"github.com/Masterminds/semver"
	"github.com/pkg/errors"
	"strings"
	"text/template"
)

// This collector collects the system db from database

type SummaryCollector struct {
	resourceManager *platform.ResourceManager
}

type querySet struct {
	Queries map[string][]query `json:"queries"`
}

type query struct {
	Statement  string `json:"statement"`
	Constraint string `json:"constraint"`
}

type ParameterTemplate struct {
	Limit int64
}

//go:embed queries.json
var queryFile []byte

func NewSummaryCollector(m *platform.ResourceManager) *SummaryCollector {
	return &SummaryCollector{
		resourceManager: m,
	}
}

func (sc *SummaryCollector) Collect(conf config.Configuration) (*data.DiagnosticBundle, error) {
	conf, err := conf.ValidateConfig(sc.Configuration())
	if err != nil {
		return &data.DiagnosticBundle{}, err
	}
	var queries querySet
	err = json.Unmarshal(queryFile, &queries)
	if err != nil {
		return &data.DiagnosticBundle{}, errors.Wrap(err, "Unable to read queries from disk")
	}
	limit, err := config.ReadIntValue(conf, "row_limit")
	if err != nil {
		return &data.DiagnosticBundle{}, err
	}

	paramTemplate := ParameterTemplate{
		Limit: limit,
	}
	frames := make(map[string]data.Frame)

	serverVersion, err := getServerSemVersion(sc)
	if err != nil {
		return &data.DiagnosticBundle{}, errors.Wrapf(err, "Unable to read server version")
	}

	var frameErrors []error
	for queryId, sqlQueries := range queries.Queries {
		//  we find the first matching query that satisfies the current version. Empty version means ANY version is
		// supported
		for _, sqlQuery := range sqlQueries {
			var queryConstraint *semver.Constraints
			if sqlQuery.Constraint != "" {
				queryConstraint, err = semver.NewConstraint(sqlQuery.Constraint)
				if err != nil {
					//we try another one
					frameErrors = append(frameErrors, errors.Wrapf(err, "Unable to parse version %s for query %s", sqlQuery.Constraint, queryId))
					continue
				}
			}
			if sqlQuery.Constraint == "" || queryConstraint.Check(serverVersion) {
				tmpl, err := template.New(queryId).Parse(sqlQuery.Statement)
				if err != nil {
					frameErrors = append(frameErrors, errors.Wrapf(err, "Unable to parse query %s", queryId))
					//we try another one
					continue
				}
				buf := new(bytes.Buffer)
				err = tmpl.Execute(buf, paramTemplate)
				if err != nil {
					frameErrors = append(frameErrors, errors.Wrapf(err, "Unable to process query %s template", queryId))
					//we try another one
					continue
				}
				frame, err := sc.resourceManager.DbClient.ExecuteStatement(queryId, buf.String())
				if err != nil {
					frameErrors = append(frameErrors, errors.Wrapf(err, "Unable to execute query %s", queryId))
					//we try another one
				} else {
					frames[queryId] = frame
					// only 1 query executed
					break
				}
			}
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

func getServerSemVersion(sc *SummaryCollector) (*semver.Version, error) {
	serverVersion, err := sc.resourceManager.DbClient.Version()
	if err != nil {
		return &semver.Version{}, err
	}
	//drop the build number - it is not a semantic version
	versionComponents := strings.Split(serverVersion, ".")
	serverVersion = strings.Join(versionComponents[:len(versionComponents)-1], ".")
	return semver.NewVersion(serverVersion)
}

func (sc *SummaryCollector) Configuration() config.Configuration {
	return config.Configuration{
		Params: []config.ConfigParam{
			config.IntParam{
				Value: 20,
				Param: config.NewParam("row_limit", "Limit rows on supported queries.", false),
			},
		},
	}
}

func (sc *SummaryCollector) IsDefault() bool {
	return true
}

func (sc *SummaryCollector) Description() string {
	return "Collects summary statistics on the database based on a set of known useful queries."
}

// here we register the collector for use
func init() {
	collectors.Register("summary", func() (collectors.Collector, error) {
		return &SummaryCollector{
			resourceManager: platform.GetResourceManager(),
		}, nil
	})
}
