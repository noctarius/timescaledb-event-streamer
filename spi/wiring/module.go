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

package wiring

import (
	"github.com/go-errors/errors"
	"github.com/samber/do"
	"github.com/samber/lo"
	"reflect"
)

var errorReflectiveType = reflect.TypeOf((*error)(nil)).Elem()

type PostConstructable interface {
	PostConstruct() error
}

type ProvideOption interface {
	applyProvideOption(info *bindingInfo)
}

/* Keep it around to improve debugging later on
type provideOptions struct {
	Name     string
	Group    string
	Info     *bindingInfo
	As       any
	Location *Func
	Exported bool
}

type Func struct {
	Name    string
	Package string
	File    string
	Line    int
}*/

func ForceInitialization() ProvideOption {
	return forceInitializationProvideOption{}
}

type forceInitializationProvideOption struct {
}

func (f forceInitializationProvideOption) applyProvideOption(info *bindingInfo) {
	info.forceInit = true
}

type Module interface {
	MayProvide(constructor any, options ...ProvideOption)
	Provide(constructor any, options ...ProvideOption)
	Invoke(call any)
	stage1(injector *do.Injector) error
	stage2(injector *do.Injector) error
}

func DefineModule(name string, definer func(module Module)) Module {
	module := &module{
		name: name,
	}
	definer(module)
	return module
}

type module struct {
	name     string
	bindings []*bindingInfo
}

func (m *module) stage1(injector *do.Injector) error {
	for _, binding := range m.bindings {
		if binding.invoker == nil {
			if lo.Contains(injector.ListProvidedServices(), binding.output.name) {
				do.OverrideNamed(injector, binding.output.name, binding.provider)
			} else {
				do.ProvideNamed(injector, binding.output.name, binding.provider)
			}
		}
	}
	return nil
}

func (m *module) stage2(injector *do.Injector) error {
	for _, binding := range m.bindings {
		if binding.invoker != nil {
			if err := binding.invoker(injector); err != nil {
				return err
			}
		}
		if binding.forceInit {
			if _, err := do.InvokeNamed[any](injector, binding.output.name); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *module) MayProvide(constructor any, options ...ProvideOption) {
	if constructor == nil {
		return
	}
	if reflect.ValueOf(constructor).IsNil() {
		return
	}
	m.Provide(constructor, options...)
}

func (m *module) Provide(constructor any, options ...ProvideOption) {
	t := reflect.TypeOf(constructor)
	if t.Kind() != reflect.Func {
		panic(errors.Errorf("Type %s is not a function", t.String()))
	}
	v := reflect.ValueOf(constructor)

	binding := &bindingInfo{
		name: t.String(),
	}

	numOfParams := t.NumIn()
	for i := 0; i < numOfParams; i++ {
		paramType := t.In(i)
		binding.inputs = append(binding.inputs, &input{
			t: paramType,
		})
	}

	mayReturnError := false
	if 2 == t.NumOut() {
		errorType := t.Out(1)
		if !errorType.ConvertibleTo(errorReflectiveType) {
			panic(errors.Errorf("Type %s has two return values, but the second one isn't an error", t.String()))
		}
		mayReturnError = true
	} else if t.NumOut() > 2 {
		panic(errors.Errorf("Type %s can only have 1 or 2 return values, but has %d", t.String(), t.NumOut()))
	}

	retVal := t.Out(0)
	binding.output = &output{
		name: retVal.String(),
		t:    retVal,
	}

	binding.provider = func(injector *do.Injector) (any, error) {
		params := make([]reflect.Value, 0)
		for i := 0; i < len(binding.inputs); i++ {
			input := binding.inputs[i]
			param, err := do.InvokeNamed[any](injector, input.t.String())
			if err != nil {
				return nil, err
			}
			params = append(params, reflect.ValueOf(param))
		}

		results := v.Call(params)
		if mayReturnError {
			errorValue := results[1]
			if !errorValue.IsNil() {
				err := asError(errorValue.Convert(errorReflectiveType).Interface())
				return nil, err
			}
		}

		value := results[0].Interface()
		if v, ok := value.(PostConstructable); ok {
			if err := v.PostConstruct(); err != nil {
				return nil, err
			}
		}
		return value, nil
	}

	for _, option := range options {
		option.applyProvideOption(binding)
	}

	m.bindings = append(m.bindings, binding)
}

func (m *module) Invoke(call any) {
	t := reflect.TypeOf(call)
	if t.Kind() != reflect.Func {
		panic(errors.Errorf("Type %s is not a function", t.String()))
	}
	v := reflect.ValueOf(call)

	binding := &bindingInfo{
		name: t.String(),
	}

	numOfParams := t.NumIn()
	for i := 0; i < numOfParams; i++ {
		paramType := t.In(i)
		binding.inputs = append(binding.inputs, &input{
			t: paramType,
		})
	}

	mayReturnError := false
	if 1 == t.NumOut() {
		errorType := t.Out(0)
		if !errorType.ConvertibleTo(errorReflectiveType) {
			panic(errors.Errorf("Type %s has two return values, but the second one isn't an error", t.String()))
		}
		mayReturnError = true
	} else if t.NumOut() > 1 {
		panic(errors.Errorf("Type %s can only have 1 return value, but has %d", t.String(), t.NumOut()))
	}

	binding.invoker = func(injector *do.Injector) error {
		params := make([]reflect.Value, 0)
		for i := 0; i < len(binding.inputs); i++ {
			input := binding.inputs[i]
			param, err := do.InvokeNamed[any](injector, input.t.String())
			if err != nil {
				return err
			}
			params = append(params, reflect.ValueOf(param))
		}

		results := v.Call(params)
		if mayReturnError {
			errorValue := results[0]
			if !errorValue.IsNil() {
				err := asError(errorValue.Convert(errorReflectiveType).Interface())
				return err
			}
		}
		return nil
	}

	m.bindings = append(m.bindings, binding)
}

func asError[T any](t T) error {
	return any(t).(error)
}

type bindingInfo struct {
	name      string
	inputs    []*input
	output    *output
	forceInit bool
	provider  func(injector *do.Injector) (any, error)
	invoker   func(injector *do.Injector) error
}

type input struct {
	t reflect.Type
}

type output struct {
	t    reflect.Type
	name string
}
