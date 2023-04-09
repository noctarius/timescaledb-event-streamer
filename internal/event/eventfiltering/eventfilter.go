package eventfiltering

import (
	"github.com/antonmedv/expr"
	"github.com/antonmedv/expr/vm"
	"github.com/go-errors/errors"
	"github.com/noctarius/event-stream-prototype/internal/configuring"
	"github.com/noctarius/event-stream-prototype/internal/schema"
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog/filtering"
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog/model"
)

type Filter interface {
	Evaluate(hypertable *model.Hypertable, key, value schema.Struct) (bool, error)
}

func NewSinkEventFilter(config *configuring.Config) (Filter, error) {
	filterDefinitions := config.Sink.Filters
	if filterDefinitions == nil {
		return &acceptAllFilter{}, nil
	}

	filters := make([]*eventFilter, 0)
	tableFilters := make([]tableFilter, 0)
	for _, def := range filterDefinitions {
		defaultValue := true
		if def.DefaultValue != nil {
			defaultValue = *def.DefaultValue
		}

		if def.Hypertables != nil {
			tf, err := filtering.NewTableFilter(def.Hypertables.Excludes, def.Hypertables.Includes, true)
			if err != nil {
				return nil, err
			}
			tableFilters = append(tableFilters, tf)
		} else {
			tableFilters = append(tableFilters, &acceptAllTableFilter{})
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
	return &compositeFilter{
		filters:      filters,
		tableFilters: tableFilters,
	}, nil
}

type acceptAllFilter struct {
}

func (f *acceptAllFilter) Evaluate(_ *model.Hypertable, _, _ schema.Struct) (bool, error) {
	return true, nil
}

type compositeFilter struct {
	filters      []*eventFilter
	tableFilters []tableFilter
}

func (f *compositeFilter) Evaluate(hypertable *model.Hypertable, key, value schema.Struct) (bool, error) {
	for i, tableFilter := range f.tableFilters {
		if tableFilter.Enabled(hypertable) {
			success, err := f.filters[i].evaluate(key, value)
			if err != nil {
				return false, err
			}
			if !success {
				return false, nil
			}
		}
	}
	return true, nil
}

type eventFilter struct {
	defaultValue bool
	condition    string
	prog         *vm.Program
	vm           *vm.VM
}

func (f *eventFilter) evaluate(key, value schema.Struct) (bool, error) {
	env := map[string]schema.Struct{
		"key":         key["payload"].(schema.Struct),
		"keySchema":   key["schema"].(schema.Struct),
		"value":       value["payload"].(schema.Struct),
		"valueSchema": value["schema"].(schema.Struct),
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
	Enabled(hypertable *model.Hypertable) bool
}

type acceptAllTableFilter struct {
}

func (aats *acceptAllTableFilter) Enabled(_ *model.Hypertable) bool {
	return true
}
