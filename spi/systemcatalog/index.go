/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package systemcatalog

import (
	"fmt"
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/samber/lo"
	"reflect"
	"strings"
	"time"
)

type IndexSortOrder string

const (
	ASC  IndexSortOrder = "ASC"
	DESC IndexSortOrder = "DESC"
)

type IndexNullsOrder string

const (
	NULLS_FIRST IndexNullsOrder = "NULLS FIRST"
	NULLS_LAST  IndexNullsOrder = "NULLS LAST"
)

// Index represents either a (compound) primary key index
// or replica identity index in the database and attached
// to a hypertable (and its chunks)
type Index struct {
	name            string
	columns         []Column
	primaryKey      bool
	replicaIdentity bool
}

func newIndex(
	name string, column []Column, primaryKey bool, replicaIdentity bool,
) *Index {

	return &Index{
		name:            name,
		columns:         column,
		primaryKey:      primaryKey,
		replicaIdentity: replicaIdentity,
	}
}

// Name returns the index name
func (i *Index) Name() string {
	return i.name
}

// PrimaryKey returns true if the index represents a
// primary key, otherwise false
func (i *Index) PrimaryKey() bool {
	return i.primaryKey
}

// ReplicaIdentity returns true if the index represents a
// replica identity index, otherwise false
func (i *Index) ReplicaIdentity() bool {
	return i.replicaIdentity
}

// Columns returns an array of colum instances representing
// the columns of the index (in order of definition)
func (i *Index) Columns() []Column {
	return i.columns
}

func (i *Index) String() string {
	builder := strings.Builder{}
	builder.WriteString("{")
	builder.WriteString(fmt.Sprintf("name:%s ", i.name))
	builder.WriteString(fmt.Sprintf("primaryKey:%t ", i.primaryKey))
	builder.WriteString(fmt.Sprintf("replicaIdentity:%t ", i.replicaIdentity))
	builder.WriteString("columns:[")
	for c, column := range i.columns {
		builder.WriteString(column.String())
		if c < len(i.columns)-1 {
			builder.WriteString(" ")
		}
	}
	builder.WriteString("]}")
	return builder.String()
}

// AsSqlTuple creates a string to be used as a tuple definition
// in a WHERE-clause:
// (col1, col2, col3, ...)
func (i *Index) AsSqlTuple() string {
	columnList := lo.Map(i.columns, func(t Column, _ int) string {
		return t.Name()
	})
	return fmt.Sprintf("(%s)", strings.Join(columnList, ","))
}

// AsSqlOrderBy creates a string for ORDER BY clauses with all parts
// of the index being ordered in descending direction
func (i *Index) AsSqlOrderBy(
	desc bool,
) string {

	order := "ASC"
	if desc {
		order = "DESC"
	}

	columnList := lo.Map(i.columns, func(t Column, _ int) string {
		return fmt.Sprintf("%s %s", t.Name(), order)
	})
	return strings.Join(columnList, ",")
}

// WhereTupleGE creates a WHERE-clause string which selects all values
// greater or equal to the given set of index parameter values
func (i *Index) WhereTupleGE(
	params map[string]any,
) (string, bool) {

	return i.whereClause(">=", params)
}

// WhereTupleGT creates a WHERE-clause string which selects all values
// greater than to the given set of index parameter values
func (i *Index) WhereTupleGT(
	params map[string]any,
) (string, bool) {

	return i.whereClause(">", params)
}

// WhereTupleLE creates a WHERE-clause string which selects all values
// less or equal to the given set of index parameter values
func (i *Index) WhereTupleLE(
	params map[string]any,
) (string, bool) {

	return i.whereClause("<=", params)
}

// WhereTupleLT creates a WHERE-clause string which selects all values
// less than to the given set of index parameter values
func (i *Index) WhereTupleLT(
	params map[string]any,
) (string, bool) {

	return i.whereClause("<", params)
}

// WhereTupleEQ creates a WHERE-clause string which selects all values
// equal to the given set of index parameter values
func (i *Index) WhereTupleEQ(
	params map[string]any,
) (string, bool) {

	return i.whereClause("=", params)
}

func (i *Index) whereClause(
	comparison string, params map[string]any,
) (string, bool) {

	tupleList := i.AsSqlTuple()

	success := true
	comparisonList := lo.Map(i.columns, func(t Column, _ int) string {
		v, present := params[t.name]
		if !present {
			success = false
			return ""
		}
		return param2value(v, t)
	})

	if !success {
		return "", false
	}

	return fmt.Sprintf("%s %s (%s)", tupleList, comparison, strings.Join(comparisonList, ",")), true
}

func param2value(
	param any, column Column,
) string {

	pv := reflect.ValueOf(param)
	pt := pv.Type()

	if pv.Kind() == reflect.Pointer {
		if pv.IsNil() {
			return "NULL"
		}

		pv = reflect.Indirect(pv)
		pt = pv.Type()
	}

	switch column.pgType.SchemaType() {
	case schema.FLOAT32, schema.FLOAT64:
		return fmt.Sprintf("%f", pv.Float())
	case schema.INT8, schema.INT16, schema.INT32, schema.INT64:
		return fmt.Sprintf("%d", pv.Int())
	case schema.BOOLEAN:
		if pv.Bool() {
			return "TRUE"
		}
		return "FALSE"
	case schema.STRING:
		val := pv.String()
		if pt.Kind() != reflect.String {
			switch v := pv.Interface().(type) {
			case time.Time:
				val = v.Format(time.RFC3339Nano)
			default:
				panic(errors.Errorf("unhandled string value: %v", pt.String()))
			}
		}
		return fmt.Sprintf("'%s'", sanitizeString(val))

	case schema.BYTES:
		bytes := pv.Interface().([]byte)
		return fmt.Sprintf("bytea '\\x%X'", bytes)
	case schema.ARRAY:
		numOfElements := pv.Len()
		elements := lo.Map(
			lo.RepeatBy(numOfElements, func(index int) reflect.Value {
				return pv.Index(index)
			}),
			func(item reflect.Value, _ int) string {
				return param2value(item.Interface(), column) // FIXME, right now arrays aren't fully supported
			},
		)
		return fmt.Sprintf("'{%s}'", strings.Join(elements, ","))

	default:
		return reflect.Zero(pt).String()
	}
}

func sanitizeString(
	val string,
) string {

	return strings.ReplaceAll(strings.ReplaceAll(val, "'", "\\'"), "\\\\'", "\\'")
}
