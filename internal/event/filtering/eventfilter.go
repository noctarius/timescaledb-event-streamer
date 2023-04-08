package filtering

import (
	"github.com/antonmedv/expr"
	"github.com/antonmedv/expr/vm"
	"github.com/go-errors/errors"
	"github.com/noctarius/event-stream-prototype/internal/configuring"
	"github.com/noctarius/event-stream-prototype/internal/schema"
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog/model"
)

type Filter struct {
	defaultValue bool
	condition    string
	prog         *vm.Program
	vm           *vm.VM
}

func NewSinkEventFilters(config *configuring.Config) ([]*Filter, error) {
	filterDefinitions := config.Sink.Filters
	if filterDefinitions == nil {
		return []*Filter{}, nil
	}

	filters := make([]*Filter, 0)
	for _, def := range filterDefinitions {
		defaultValue := true
		if def.DefaultValue != nil {
			defaultValue = *def.DefaultValue
		}

		prog, err := expr.Compile(def.Condition)
		if err != nil {
			return nil, err
		}

		filters = append(filters, &Filter{
			defaultValue: defaultValue,
			condition:    def.Condition,
			prog:         prog,
			vm:           &vm.VM{},
		})
	}
	return filters, nil
}

func (f *Filter) Evaluate(hypertable *model.Hypertable, key, value schema.Struct) (bool, error) {
	result, err := f.vm.Run(f.prog, map[string]schema.Struct{})
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
