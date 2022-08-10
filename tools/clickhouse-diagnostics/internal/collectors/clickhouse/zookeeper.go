package clickhouse

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/collectors"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/config"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/data"
	"github.com/bmatcuk/doublestar/v4"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"strings"
)

// This collector collects the system zookeeper db

type ZookeeperCollector struct {
	resourceManager *platform.ResourceManager
}

func NewZookeeperCollector(m *platform.ResourceManager) *ZookeeperCollector {
	return &ZookeeperCollector{
		resourceManager: m,
	}
}

func (zkc *ZookeeperCollector) Collect(conf config.Configuration) (*data.DiagnosticBundle, error) {
	conf, err := conf.ValidateConfig(zkc.Configuration())
	if err != nil {
		return &data.DiagnosticBundle{}, err
	}

	pathPattern, err := config.ReadStringValue(conf, "path_pattern")
	if err != nil {
		return &data.DiagnosticBundle{}, err
	}
	defaultPattern, _ := zkc.Configuration().GetConfigParam("path_pattern")
	if defaultPattern.(config.StringParam).Value != pathPattern {
		log.Warn().Msgf("Using non default zookeeper glob pattern [%s] - this can potentially cause high query load", pathPattern)
	}
	maxDepth, err := config.ReadIntValue(conf, "max_depth")
	if err != nil {
		return &data.DiagnosticBundle{}, err
	}
	rowLimit, err := config.ReadIntValue(conf, "row_limit")
	if err != nil {
		return &data.DiagnosticBundle{}, err
	}
	// we use doublestar for globs as it provides us with ** but also allows us to identify prefix or base paths
	if !doublestar.ValidatePattern(pathPattern) {
		return &data.DiagnosticBundle{}, errors.Wrapf(err, "%s is not a valid pattern", pathPattern)
	}
	base, _ := doublestar.SplitPattern(pathPattern)
	frames := make(map[string]data.Frame)
	hFrame, frameErrors := zkc.collectSubFrames(base, pathPattern, rowLimit, 0, maxDepth)
	fErrors := data.FrameErrors{
		Errors: frameErrors,
	}
	frames["zookeeper_db"] = hFrame
	return &data.DiagnosticBundle{
		Frames: frames,
		Errors: fErrors,
	}, nil
}

// recursively iterates over the zookeeper sub tables to a max depth, applying the filter and max rows per table
func (zkc *ZookeeperCollector) collectSubFrames(path, pathPattern string, rowLimit, currentDepth, maxDepth int64) (data.HierarchicalFrame, []error) {
	var frameErrors []error
	var subFrames []data.HierarchicalFrame

	currentDepth += 1
	if currentDepth == maxDepth {
		return data.HierarchicalFrame{}, frameErrors
	}
	match, err := doublestar.PathMatch(pathPattern, path)
	if err != nil {
		frameErrors = append(frameErrors, errors.Wrapf(err, "Path match failed for pattern %s with path %s", pathPattern, path))
		return data.HierarchicalFrame{}, frameErrors
	}
	// we allow a single level to be examined or we never get going
	if !match && currentDepth > 1 {
		return data.HierarchicalFrame{}, frameErrors
	}
	frame, err := zkc.resourceManager.DbClient.ExecuteStatement(path, fmt.Sprintf("SELECT name FROM system.zookeeper WHERE path='%s' LIMIT %d", path, rowLimit))
	if err != nil {
		frameErrors = append(frameErrors, errors.Wrapf(err, "Unable to read zookeeper table path for sub paths %s", path))
		return data.HierarchicalFrame{}, frameErrors
	}

	// this isn't ideal, we add re-execute the query to our collection as this will be consumed by the output lazily
	outputFrame, err := zkc.resourceManager.DbClient.ExecuteStatement(path, fmt.Sprintf("SELECT * FROM system.zookeeper WHERE path='%s' LIMIT %d", path, rowLimit))
	if err != nil {
		frameErrors = append(frameErrors, errors.Wrapf(err, "Unable to read zookeeper table path %s", path))
		return data.HierarchicalFrame{}, frameErrors
	}
	frameComponents := strings.Split(path, "/")
	frameId := frameComponents[len(frameComponents)-1]

	for {
		values, ok, err := frame.Next()
		if err != nil {
			frameErrors = append(frameErrors, errors.Wrapf(err, "unable to read frame %s", frame.Name()))
			return data.NewHierarchicalFrame(frameId, outputFrame, subFrames), frameErrors
		}
		if !ok {
			return data.NewHierarchicalFrame(frameId, outputFrame, subFrames), frameErrors
		}
		subName := fmt.Sprintf("%v", values[0])
		subPath := fmt.Sprintf("%s/%s", path, subName)
		subFrame, errs := zkc.collectSubFrames(subPath, pathPattern, rowLimit, currentDepth, maxDepth)
		if subFrame.Name() != "" {
			subFrames = append(subFrames, subFrame)
		}
		frameErrors = append(frameErrors, errs...)
	}
}

func (zkc *ZookeeperCollector) Configuration() config.Configuration {
	return config.Configuration{
		Params: []config.ConfigParam{
			config.StringParam{
				Value: "/clickhouse/{task_queue}/**",
				Param: config.NewParam("path_pattern", "Glob pattern for zookeeper path matching. Change with caution.", false),
			},
			config.IntParam{
				Value: 8,
				Param: config.NewParam("max_depth", "Max depth for zookeeper navigation.", false),
			},
			config.IntParam{
				Value: 10,
				Param: config.NewParam("row_limit", "Maximum number of rows/sub nodes to collect/expand from any zookeeper leaf. Negative values mean unlimited.", false),
			},
		},
	}
}

func (zkc *ZookeeperCollector) IsDefault() bool {
	return false
}

func (zkc *ZookeeperCollector) Description() string {
	return "Collects Zookeeper information available within ClickHouse."
}

// here we register the collector for use
func init() {
	collectors.Register("zookeeper_db", func() (collectors.Collector, error) {
		return &ZookeeperCollector{
			resourceManager: platform.GetResourceManager(),
		}, nil
	})
}
