package filtering

import (
	"github.com/noctarius/event-stream-prototype/internal/configuring"
	"github.com/noctarius/event-stream-prototype/internal/configuring/sysconfig"
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog/model"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_Default_Excluded(t *testing.T) {
	config := &sysconfig.SystemConfig{
		Config: &configuring.Config{
			TimescaleDB: configuring.TimescaleDBConfig{
				Hypertables: configuring.HypertablesConfig{},
			},
		},
	}

	replicationFilter, err := NewReplicationFilter(config)
	if err != nil {
		t.Fatalf("error parsing: %+v", err)
	}

	hypertable := makeHypertable(1, "public", "test")
	assert.Equal(t, false, replicationFilter.Enabled(hypertable))
}

func Test_Parse_Error_Too_Many_Tokens(t *testing.T) {
	config := &sysconfig.SystemConfig{
		Config: &configuring.Config{
			TimescaleDB: configuring.TimescaleDBConfig{
				Hypertables: configuring.HypertablesConfig{
					Includes: []string{"foo.bar.baz"},
				},
			},
		},
	}

	_, err := NewReplicationFilter(config)
	if err == nil {
		t.FailNow()
	}
	assert.ErrorContains(t, err, "failed parsing filter term: foo.bar.baz")
}

func Test_Parse_Error_Includes_Compile_Schema(t *testing.T) {
	config := &sysconfig.SystemConfig{
		Config: &configuring.Config{
			TimescaleDB: configuring.TimescaleDBConfig{
				Hypertables: configuring.HypertablesConfig{
					Includes: []string{"fo(+o.bar"},
				},
			},
		},
	}

	_, err := NewReplicationFilter(config)
	if err == nil {
		t.FailNow()
	}
	assert.ErrorContains(t, err, "illegal character in pattern 'fo(+o' at index 2")
}

func Test_Parse_Error_Includes_Compile_Table(t *testing.T) {
	config := &sysconfig.SystemConfig{
		Config: &configuring.Config{
			TimescaleDB: configuring.TimescaleDBConfig{
				Hypertables: configuring.HypertablesConfig{
					Includes: []string{"foo.ba(+r"},
				},
			},
		},
	}

	_, err := NewReplicationFilter(config)
	if err == nil {
		t.FailNow()
	}
	assert.ErrorContains(t, err, "illegal character in pattern 'ba(+r' at index 2")
}

func Test_Parse_Error_Excludes_Compile_Schema(t *testing.T) {
	config := &sysconfig.SystemConfig{
		Config: &configuring.Config{
			TimescaleDB: configuring.TimescaleDBConfig{
				Hypertables: configuring.HypertablesConfig{
					Excludes: []string{"fo(+o.bar"},
				},
			},
		},
	}

	_, err := NewReplicationFilter(config)
	if err == nil {
		t.FailNow()
	}
	assert.ErrorContains(t, err, "illegal character in pattern 'fo(+o' at index 2")
}

func Test_Parse_Error_Excludes_Compile_Table(t *testing.T) {
	config := &sysconfig.SystemConfig{
		Config: &configuring.Config{
			TimescaleDB: configuring.TimescaleDBConfig{
				Hypertables: configuring.HypertablesConfig{
					Excludes: []string{"foo.ba(+r"},
				},
			},
		},
	}

	_, err := NewReplicationFilter(config)
	if err == nil {
		t.FailNow()
	}
	assert.ErrorContains(t, err, "illegal character in pattern 'ba(+r' at index 2")
}

func Test_Parse_Error_Pattern_Too_Long(t *testing.T) {
	config := &sysconfig.SystemConfig{
		Config: &configuring.Config{
			TimescaleDB: configuring.TimescaleDBConfig{
				Hypertables: configuring.HypertablesConfig{
					Excludes: []string{"foo.falilwfrmscfoxqssyhojpwrairwvaeagdyxjkhdrpzjxprjmjhicqvogmrxtrew"},
				},
			},
		},
	}

	_, err := NewReplicationFilter(config)
	if err == nil {
		t.FailNow()
	}
	assert.ErrorContains(t, err, "an pattern cannot be longer than 63 characters")
}

func Test_Parse_Error_Illegal_First_Character(t *testing.T) {
	config := &sysconfig.SystemConfig{
		Config: &configuring.Config{
			TimescaleDB: configuring.TimescaleDBConfig{
				Hypertables: configuring.HypertablesConfig{
					Excludes: []string{"foo.%t"},
				},
			},
		},
	}

	_, err := NewReplicationFilter(config)
	if err == nil {
		t.FailNow()
	}
	assert.ErrorContains(t, err, "% is an illegal first character of pattern '%t'")
}

func Test_Parse_Error_Escape_Char(t *testing.T) {
	config := &sysconfig.SystemConfig{
		Config: &configuring.Config{
			TimescaleDB: configuring.TimescaleDBConfig{
				Hypertables: configuring.HypertablesConfig{
					Excludes: []string{"foo.t\\"},
				},
			},
		},
	}

	_, err := NewReplicationFilter(config)
	if err == nil {
		t.FailNow()
	}
	assert.ErrorContains(t, err, "illegal character in pattern 't\\' at index 1")
}

func Test_Parse_Error_Reserved_Keyword(t *testing.T) {
	config := &sysconfig.SystemConfig{
		Config: &configuring.Config{
			TimescaleDB: configuring.TimescaleDBConfig{
				Hypertables: configuring.HypertablesConfig{
					Excludes: []string{"binary.t\\"},
				},
			},
		},
	}

	_, err := NewReplicationFilter(config)
	if err == nil {
		t.FailNow()
	}
	assert.ErrorContains(t, err, "an unquoted pattern cannot match a reserved keyword: BINARY")
}

func Test_Quoted_Valid_Escape_Sequence_Asterisk(t *testing.T) {
	config := &sysconfig.SystemConfig{
		Config: &configuring.Config{
			TimescaleDB: configuring.TimescaleDBConfig{
				Hypertables: configuring.HypertablesConfig{
					Includes: []string{"public.\"t\\*\""},
				},
			},
		},
	}

	replicationFilter, err := NewReplicationFilter(config)
	if err != nil {
		t.Fatalf("error parsing: %+v", err)
	}

	hypertable := makeHypertable(1, "public", "t\\*")
	assert.Equal(t, true, replicationFilter.Enabled(hypertable))
}

func Test_Quoted_Valid_Escape_Sequence_QuestionMark(t *testing.T) {
	config := &sysconfig.SystemConfig{
		Config: &configuring.Config{
			TimescaleDB: configuring.TimescaleDBConfig{
				Hypertables: configuring.HypertablesConfig{
					Includes: []string{"public.\"t\\?\""},
				},
			},
		},
	}

	replicationFilter, err := NewReplicationFilter(config)
	if err != nil {
		t.Fatalf("error parsing: %+v", err)
	}

	hypertable := makeHypertable(1, "public", "t\\?")
	assert.Equal(t, true, replicationFilter.Enabled(hypertable))
}

func Test_Quoted_Valid_Escape_Sequence_Plus(t *testing.T) {
	config := &sysconfig.SystemConfig{
		Config: &configuring.Config{
			TimescaleDB: configuring.TimescaleDBConfig{
				Hypertables: configuring.HypertablesConfig{
					Includes: []string{"public.\"t\\+\""},
				},
			},
		},
	}

	replicationFilter, err := NewReplicationFilter(config)
	if err != nil {
		t.Fatalf("error parsing: %+v", err)
	}

	hypertable := makeHypertable(1, "public", "t\\+")
	assert.Equal(t, true, replicationFilter.Enabled(hypertable))
}

func Test_Simple_Include(t *testing.T) {
	config := &sysconfig.SystemConfig{
		Config: &configuring.Config{
			TimescaleDB: configuring.TimescaleDBConfig{
				Hypertables: configuring.HypertablesConfig{
					Includes: []string{"public.test"},
				},
			},
		},
	}

	replicationFilter, err := NewReplicationFilter(config)
	if err != nil {
		t.Fatalf("error parsing: %+v", err)
	}

	hypertable := makeHypertable(1, "public", "test")
	assert.Equal(t, true, replicationFilter.Enabled(hypertable))

	hypertable = makeHypertable(2, "public", "test2")
	assert.Equal(t, false, replicationFilter.Enabled(hypertable))
}

func Test_Exclude_Has_Precedence(t *testing.T) {
	config := &sysconfig.SystemConfig{
		Config: &configuring.Config{
			TimescaleDB: configuring.TimescaleDBConfig{
				Hypertables: configuring.HypertablesConfig{
					Excludes: []string{"public.test"},
					Includes: []string{"public.test"},
				},
			},
		},
	}

	replicationFilter, err := NewReplicationFilter(config)
	if err != nil {
		t.Fatalf("error parsing: %+v", err)
	}

	hypertable := makeHypertable(1, "public", "test")
	assert.Equal(t, false, replicationFilter.Enabled(hypertable))
}

func Test_Exclude_Has_Precedence_With_Wildcard(t *testing.T) {
	config := &sysconfig.SystemConfig{
		Config: &configuring.Config{
			TimescaleDB: configuring.TimescaleDBConfig{
				Hypertables: configuring.HypertablesConfig{
					Excludes: []string{"public.test"},
					Includes: []string{"public.*"},
				},
			},
		},
	}

	replicationFilter, err := NewReplicationFilter(config)
	if err != nil {
		t.Fatalf("error parsing: %+v", err)
	}

	hypertable := makeHypertable(1, "public", "test")
	assert.Equal(t, false, replicationFilter.Enabled(hypertable))
}

func Test_Include_Table_With_Wildcard_Asterisk(t *testing.T) {
	config := &sysconfig.SystemConfig{
		Config: &configuring.Config{
			TimescaleDB: configuring.TimescaleDBConfig{
				Hypertables: configuring.HypertablesConfig{
					Includes: []string{"public.*"},
				},
			},
		},
	}

	replicationFilter, err := NewReplicationFilter(config)
	if err != nil {
		t.Fatalf("error parsing: %+v", err)
	}

	hypertable := makeHypertable(1, "public", "test")
	assert.Equal(t, true, replicationFilter.Enabled(hypertable))

	hypertable = makeHypertable(2, "public", "test2")
	assert.Equal(t, true, replicationFilter.Enabled(hypertable))

	hypertable = makeHypertable(3, "public2", "test")
	assert.Equal(t, false, replicationFilter.Enabled(hypertable))
}

func Test_Include_Schema_With_Wildcard_Asterisk(t *testing.T) {
	config := &sysconfig.SystemConfig{
		Config: &configuring.Config{
			TimescaleDB: configuring.TimescaleDBConfig{
				Hypertables: configuring.HypertablesConfig{
					Includes: []string{"*.test"},
				},
			},
		},
	}

	replicationFilter, err := NewReplicationFilter(config)
	if err != nil {
		t.Fatalf("error parsing: %+v", err)
	}

	hypertable := makeHypertable(1, "public", "test")
	assert.Equal(t, true, replicationFilter.Enabled(hypertable))

	hypertable = makeHypertable(2, "public", "test2")
	assert.Equal(t, false, replicationFilter.Enabled(hypertable))

	hypertable = makeHypertable(3, "public2", "test")
	assert.Equal(t, true, replicationFilter.Enabled(hypertable))
}

func Test_Include_Table_With_Wildcard_QuestionMark(t *testing.T) {
	config := &sysconfig.SystemConfig{
		Config: &configuring.Config{
			TimescaleDB: configuring.TimescaleDBConfig{
				Hypertables: configuring.HypertablesConfig{
					Includes: []string{"public.test?a"},
				},
			},
		},
	}

	replicationFilter, err := NewReplicationFilter(config)
	if err != nil {
		t.Fatalf("error parsing: %+v", err)
	}

	hypertable := makeHypertable(1, "public", "test1a")
	assert.Equal(t, true, replicationFilter.Enabled(hypertable))

	hypertable = makeHypertable(2, "public", "test2a")
	assert.Equal(t, true, replicationFilter.Enabled(hypertable))

	hypertable = makeHypertable(3, "public2", "test1b")
	assert.Equal(t, false, replicationFilter.Enabled(hypertable))

	hypertable = makeHypertable(3, "public2", "test11a")
	assert.Equal(t, false, replicationFilter.Enabled(hypertable))
}

func Test_Include_Schema_With_Wildcard_QuestionMark(t *testing.T) {
	config := &sysconfig.SystemConfig{
		Config: &configuring.Config{
			TimescaleDB: configuring.TimescaleDBConfig{
				Hypertables: configuring.HypertablesConfig{
					Includes: []string{"t?p.test"},
				},
			},
		},
	}

	replicationFilter, err := NewReplicationFilter(config)
	if err != nil {
		t.Fatalf("error parsing: %+v", err)
	}

	hypertable := makeHypertable(1, "top", "test")
	assert.Equal(t, true, replicationFilter.Enabled(hypertable))

	hypertable = makeHypertable(2, "tap", "test")
	assert.Equal(t, true, replicationFilter.Enabled(hypertable))

	hypertable = makeHypertable(2, "tap", "test")
	assert.Equal(t, true, replicationFilter.Enabled(hypertable))

	hypertable = makeHypertable(3, "toop", "test")
	assert.Equal(t, false, replicationFilter.Enabled(hypertable))
}

func Test_Include_Table_With_Wildcard_Plus(t *testing.T) {
	config := &sysconfig.SystemConfig{
		Config: &configuring.Config{
			TimescaleDB: configuring.TimescaleDBConfig{
				Hypertables: configuring.HypertablesConfig{
					Includes: []string{"public.test+a"},
				},
			},
		},
	}

	replicationFilter, err := NewReplicationFilter(config)
	if err != nil {
		t.Fatalf("error parsing: %+v", err)
	}

	hypertable := makeHypertable(1, "public", "test1a")
	assert.Equal(t, true, replicationFilter.Enabled(hypertable))

	hypertable = makeHypertable(2, "public", "test2a")
	assert.Equal(t, true, replicationFilter.Enabled(hypertable))

	hypertable = makeHypertable(3, "public2", "test1b")
	assert.Equal(t, false, replicationFilter.Enabled(hypertable))

	hypertable = makeHypertable(3, "public", "test11a")
	assert.Equal(t, true, replicationFilter.Enabled(hypertable))
}

func Test_Include_Schema_With_Wildcard_Plus(t *testing.T) {
	config := &sysconfig.SystemConfig{
		Config: &configuring.Config{
			TimescaleDB: configuring.TimescaleDBConfig{
				Hypertables: configuring.HypertablesConfig{
					Includes: []string{"t+p.test"},
				},
			},
		},
	}

	replicationFilter, err := NewReplicationFilter(config)
	if err != nil {
		t.Fatalf("error parsing: %+v", err)
	}

	hypertable := makeHypertable(1, "top", "test")
	assert.Equal(t, true, replicationFilter.Enabled(hypertable))

	hypertable = makeHypertable(2, "tap", "test")
	assert.Equal(t, true, replicationFilter.Enabled(hypertable))

	hypertable = makeHypertable(3, "toop", "test")
	assert.Equal(t, true, replicationFilter.Enabled(hypertable))

	hypertable = makeHypertable(3, "tp", "test")
	assert.Equal(t, false, replicationFilter.Enabled(hypertable))
}

func Test_Include_Both_With_Wildcard(t *testing.T) {
	config := &sysconfig.SystemConfig{
		Config: &configuring.Config{
			TimescaleDB: configuring.TimescaleDBConfig{
				Hypertables: configuring.HypertablesConfig{
					Includes: []string{"t+p.test?"},
				},
			},
		},
	}

	replicationFilter, err := NewReplicationFilter(config)
	if err != nil {
		t.Fatalf("error parsing: %+v", err)
	}

	hypertable := makeHypertable(1, "top", "test1")
	assert.Equal(t, true, replicationFilter.Enabled(hypertable))

	hypertable = makeHypertable(2, "tap", "test2")
	assert.Equal(t, true, replicationFilter.Enabled(hypertable))

	hypertable = makeHypertable(3, "toop", "test3")
	assert.Equal(t, true, replicationFilter.Enabled(hypertable))

	hypertable = makeHypertable(3, "tp", "test4")
	assert.Equal(t, false, replicationFilter.Enabled(hypertable))

	hypertable = makeHypertable(3, "tap", "test11")
	assert.Equal(t, false, replicationFilter.Enabled(hypertable))
}

func Test_Valid_Strings(t *testing.T) {
	validStringParsing(t, "Customers5", "customers5")
	validStringParsing(t, "\"5Customers\"", "5Customers")
	validStringParsing(t, "dataField", "datafield")
	validStringParsing(t, "_dataField1", "_datafield1")
	validStringParsing(t, "ADGROUP", "adgroup")
	validStringParsing(t, "\"tableName~\"", "tableName~")
	validStringParsing(t, "\"GROUP\"", "GROUP")
	validStringParsing(t, "\"A\"\"A\"", "A\"\"A")
	validStringParsing(t, "\"A\"A\"", "A\"\"A")
}

func Test_Invalid_Strings(t *testing.T) {
	invalidStringParsing(t, "5Customers", "5 is an illegal first character of pattern '5customers'")
	invalidStringParsing(t, "_dataField!", "illegal character in pattern '_datafield!' at index 10")
	invalidStringParsing(t, "GROUP", "an unquoted pattern cannot match a reserved keyword: GROUP")
}

func validStringParsing(t *testing.T, token, expected string) {
	token, regex, err := parseToken(token)
	if err != nil {
		t.FailNow()
	}
	if regex {
		t.FailNow()
	}
	assert.Equal(t, expected, token)
}

func invalidStringParsing(t *testing.T, token, expected string) {
	_, _, err := parseToken(token)
	if err == nil {
		t.FailNow()
	}
	assert.ErrorContains(t, err, expected)
}

func makeHypertable(id int32, schemaName, tableName string) *model.Hypertable {
	return model.NewHypertable(
		id,
		"test",
		schemaName,
		tableName,
		"test",
		"test",
		nil,
		0,
		false,
		nil,
		nil,
	)
}
