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

package eventfiltering

import (
	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/internal/systemcatalog/tablefiltering"
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
)

type EventFilter interface {
	Evaluate(
		table schema.TableAlike, key, value schema.Struct,
	) (bool, error)
}

type eventFilterFunc func(
	table schema.TableAlike, key, value schema.Struct,
) (bool, error)

func (eff eventFilterFunc) Evaluate(
	table schema.TableAlike, key, value schema.Struct,
) (bool, error) {

	return eff(table, key, value)
}

func NewEventFilter(
	filterDefinitions map[string]config.EventFilterConfig,
) (EventFilter, error) {

	if filterDefinitions == nil {
		return acceptAllFilter, nil
	}

	filters := make([]*eventFilter, 0)
	tableFilters := make([]tableFilter, 0)
	for _, def := range filterDefinitions {
		defaultValue := true
		if def.DefaultValue != nil {
			defaultValue = *def.DefaultValue
		}

		if def.Tables != nil {
			tf, err := tablefiltering.NewTableFilter(def.Tables.Excludes, def.Tables.Includes, true)
			if err != nil {
				return nil, err
			}
			tableFilters = append(tableFilters, tf)
		} else {
			tableFilters = append(tableFilters, acceptAllTableFilter)
		}

		prog, err := expr.Compile(def.Condition)
		if err != nil {
			return nil, err
		}

		filters = append(filters, &eventFilter{
			defaultValue: defaultValue,
			condition:    def.Condition,
			prog:         prog,
			vm:           &vm.VM{},
		})
	}
	return compositeFilter(filters, tableFilters), nil
}

var acceptAllFilter eventFilterFunc = func(
	_ schema.TableAlike, _, _ schema.Struct,
) (bool, error) {

	return true, nil
}

var compositeFilter = func(
	filters []*eventFilter, tableFilters []tableFilter,
) EventFilter {

	return eventFilterFunc(
		func(
			table schema.TableAlike, key, value schema.Struct,
		) (bool, error) {

			for i, tableFilter := range tableFilters {
				if table == nil || tableFilter.Enabled(table) {
					success, err := filters[i].evaluate(key, value)
					if err != nil {
						return false, err
					}
					if !success {
						return false, nil
					}
				}
			}
			return true, nil
		},
	)
}

type eventFilter struct {
	defaultValue bool
	condition    string
	prog         *vm.Program
	vm           *vm.VM
}

func (f *eventFilter) evaluate(
	key, value schema.Struct,
) (bool, error) {

	env := map[string]schema.Struct{
		"key":         key[schema.FieldNamePayload].(schema.Struct),
		"keySchema":   key[schema.FieldNameSchema].(schema.Struct),
		"value":       value[schema.FieldNamePayload].(schema.Struct),
		"valueSchema": value[schema.FieldNameSchema].(schema.Struct),
	}

	result, err := f.vm.Run(f.prog, env)
	if err != nil {
		return false, err
	}

	r, ok := result.(bool)
	if !ok {
		return false, errors.Errorf("result of filter «%s» isn't a boolean", f.condition)
	}

	if r {
		return f.defaultValue, nil
	}
	return !f.defaultValue, nil
}

type tableFilter interface {
	Enabled(
		table systemcatalog.SystemEntity,
	) bool
}

type tableFilterFunc func(
	table systemcatalog.SystemEntity,
) bool

func (tff tableFilterFunc) Enabled(
	table systemcatalog.SystemEntity,
) bool {

	return tff(table)
}

var acceptAllTableFilter tableFilterFunc = func(
	systemcatalog.SystemEntity,
) bool {

	return true
}
