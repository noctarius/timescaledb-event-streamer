package filtering

import (
	"fmt"
	"github.com/go-errors/errors"
	"github.com/noctarius/event-stream-prototype/internal/configuring/sysconfig"
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog/model"
	"regexp"
	"strings"
	"unicode"
)

type ReplicationFilter struct {
	includes    []*filter
	excludes    []*filter
	filterCache map[string]bool
}

func NewReplicationFilter(config *sysconfig.SystemConfig) (*ReplicationFilter, error) {
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

	return &ReplicationFilter{
		includes:    includes,
		excludes:    excludes,
		filterCache: make(map[string]bool, 0),
	}, nil
}

func (rf *ReplicationFilter) Enabled(hypertable *model.Hypertable) bool {
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

	namespace, namespaceIsRegex, err := parseToken(tokens[0])
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	table, tableIsRegex, err := parseToken(tokens[1])
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	f := &filter{}
	if namespaceIsRegex {
		f.namespaceRegex = regexp.MustCompile(fmt.Sprintf("^%s$", namespace))
	} else {
		f.namespace = namespace
	}

	if tableIsRegex {
		f.tableRegex = regexp.MustCompile(fmt.Sprintf("^%s$", table))
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

func parseToken(token string) (string, bool, error) {
	isQuoted := token[0] == '"' && token[len(token)-1] == '"'

	// When not quoted, all identifiers are folded to lowercase
	if !isQuoted {
		token = strings.ToLower(token)
	}

	// Check identifier length
	if len(token) > 63 {
		if !isQuoted || len(token) > 65 {
			return "", false, errors.Errorf("an pattern cannot be longer than 63 characters")
		}
	}

	firstIndex := 0
	if isQuoted {
		firstIndex++
	}
	lastIndex := len(token)
	if isQuoted {
		lastIndex--
	}

	// If unquoted the first character needs to be a letter, underscore, or a valid wildcard (*|?|+)
	if !isQuoted {
		if !unicode.IsLetter(rune(token[0])) &&
			token[0] != '_' &&
			token[0] != '*' &&
			token[0] != '?' &&
			token[0] != '+' {

			return "", false, errors.Errorf(
				"%s is an illegal first character of pattern '%s'", string(token[0]), token,
			)
		}
	}

	isRegex := false
	runedToken := []rune(token)
	builder := strings.Builder{}
	for i := firstIndex; i < lastIndex; i++ {
		char := runedToken[i]

		if char == '\\' && isQuoted {
			if i < len(runedToken)-1 {
				peekNextChar := runedToken[i+1]
				if peekNextChar == '*' {
					builder.WriteString("\\*")
					i++
				} else if peekNextChar == '?' {
					builder.WriteString("\\?")
					i++
				} else if peekNextChar == '+' {
					builder.WriteString("\\+")
					i++
				}
			}
		} else if char == '*' {
			builder.WriteString(".*?")
			isRegex = true
		} else if char == '?' {
			builder.WriteString(".{1}")
			isRegex = true
		} else if char == '+' {
			builder.WriteString(".+?")
			isRegex = true
		} else if unicode.IsLetter(char) || unicode.IsNumber(char) || char == '_' || isQuoted {
			builder.WriteRune(char)
		} else {
			return "", false, errors.Errorf(
				"illegal character in pattern '%s' at index %d", token, i,
			)
		}
	}

	parsedToken := builder.String()
	if !isQuoted && !isRegex {
		uppercaseParsedToken := strings.ToUpper(parsedToken)
		for _, keyword := range reservedKeywords {
			if keyword == uppercaseParsedToken {
				return "", false, errors.Errorf(
					"an unquoted pattern cannot match a reserved keyword: %s", keyword,
				)
			}
		}
	}

	return parsedToken, isRegex, nil
}
