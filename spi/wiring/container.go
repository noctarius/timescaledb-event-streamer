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
	"github.com/samber/do"
	"reflect"
)

type Container interface {
	Service(service any) error
}

func NewContainer(
	modules ...Module,
) (Container, error) {

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

func (c *container) Service(
	service any,
) error {

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
