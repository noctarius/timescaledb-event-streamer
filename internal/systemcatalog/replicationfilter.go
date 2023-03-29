package systemcatalog

import (
	"fmt"
	"github.com/noctarius/event-stream-prototype/internal/configuring"
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog/model"
	"regexp"
	"strings"
)

type replicationFilter struct {
	includes    []*filter
	excludes    []*filter
	filterCache map[string]bool
}

func newReplicationFilter(config *configuring.Config) (*replicationFilter, error) {
	excludes := make([]*filter, 0)
	for _, exclude := range config.TimescaleDB.Hypertables.Excludes {
		f, err := parseFilter(exclude)
		if err != nil {
			return nil, err
		}
		excludes = append(excludes, f)
	}

	includes := make([]*filter, 0)
	for _, include := range config.TimescaleDB.Hypertables.Includes {
		f, err := parseFilter(include)
		if err != nil {
			return nil, err
		}
		includes = append(includes, f)
	}

	return &replicationFilter{
		includes:    includes,
		excludes:    excludes,
		filterCache: make(map[string]bool, 0),
	}, nil
}

func (rf *replicationFilter) enabled(hypertable *model.Hypertable) bool {
	// already tested?
	canonicalName := hypertable.CanonicalName()
	// _timescaledb_internal._compressed_hypertable_246
	if v, present := rf.filterCache[canonicalName]; present {
		return v
	}

	// excluded has priority
	for _, exclude := range rf.excludes {
		if exclude.matches(hypertable.SchemaName(), hypertable.HypertableName()) {
			rf.filterCache[canonicalName] = false
			return false
		}
	}

	// is explicitly included?
	for _, include := range rf.includes {
		if include.matches(hypertable.SchemaName(), hypertable.HypertableName()) {
			rf.filterCache[canonicalName] = true
			return true
		}
	}

	// otherwise false
	rf.filterCache[canonicalName] = false
	return false
}

type filter struct {
	namespace      string
	table          string
	namespaceRegex *regexp.Regexp
	tableRegex     *regexp.Regexp
}

func parseFilter(filterTerm string) (*filter, error) {
	tokens := strings.Split(filterTerm, ".")
	if len(tokens) != 2 {
		return nil, fmt.Errorf("failed parsing filter term: %s", filterTerm)
	}

	namespace := tokens[0]
	namespaceIsRegex := false
	if strings.Contains(namespace, "*") {
		namespace = strings.ReplaceAll(namespace, "*", ".*?")
		namespaceIsRegex = true
	}
	if strings.Contains(namespace, "?") {
		namespace = strings.ReplaceAll(namespace, "?", ".{1}")
		namespaceIsRegex = true
	}
	if strings.Contains(namespace, "+") {
		namespace = strings.ReplaceAll(namespace, "+", ".+?")
		namespaceIsRegex = true
	}

	table := tokens[1]
	tableIsRegex := false
	if strings.Contains(table, "*") {
		table = strings.ReplaceAll(table, "*", ".*?")
		tableIsRegex = true
	}
	if strings.Contains(table, "?") {
		table = strings.ReplaceAll(table, "?", ".{1}")
		tableIsRegex = true
	}
	if strings.Contains(table, "+") {
		table = strings.ReplaceAll(table, "+", ".+?")
		tableIsRegex = true
	}

	f := &filter{}
	if namespaceIsRegex {
		nr, err := regexp.Compile(namespace)
		if err != nil {
			return nil, err
		}
		f.namespaceRegex = nr
	} else {
		f.namespace = namespace
	}

	if tableIsRegex {
		tr, err := regexp.Compile(table)
		if err != nil {
			return nil, err
		}
		f.tableRegex = tr
	} else {
		f.table = table
	}
	return f, nil
}

func (f *filter) matches(namespace, table string) bool {
	if f.namespaceRegex != nil {
		if !f.namespaceRegex.Match([]byte(namespace)) {
			return false
		}
	} else {
		if f.namespace != namespace {
			return false
		}
	}

	if f.tableRegex != nil {
		if !f.tableRegex.Match([]byte(table)) {
			return false
		}
	} else {
		if f.table != table {
			return false
		}
	}
	return true
}
