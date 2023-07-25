package wiring

import (
	"github.com/samber/do"
	"reflect"
)

type Container interface {
	Service(service any) error
}

func NewContainer(modules ...Module) (Container, error) {
	injector := do.New()

	// Register providers
	for _, module := range modules {
		if err := module.stage1(injector); err != nil {
			return nil, err
		}
	}

	// Late initialization
	for _, module := range modules {
		if err := module.stage2(injector); err != nil {
			return nil, err
		}
	}

	return &container{
		injector: injector,
	}, nil
}

type container struct {
	injector *do.Injector
}

func (c *container) Service(service any) error {
	serviceValue := reflect.ValueOf(service)

	if serviceValue.Kind() == reflect.Pointer {
		serviceValue = reflect.Indirect(serviceValue)
	}

	serviceType := serviceValue.Type()
	serviceInstance, err := do.InvokeNamed[any](c.injector, serviceType.String())
	if err != nil {
		return err
	}
	serviceValue.Set(reflect.ValueOf(serviceInstance))
	return nil
}
