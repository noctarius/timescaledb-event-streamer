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

package config

import (
	"github.com/BurntSushi/toml"
	"gopkg.in/yaml.v3"
)

func Unmarshall(
	content []byte, config *Config, toml bool,
) error {

	if toml {
		return fromToml(content, config)
	}
	return fromYaml(content, config)
}

func fromToml(
	content []byte, config *Config,
) error {

	return toml.Unmarshal(content, config)
}

func fromYaml(
	content []byte, config *Config,
) error {

	return yaml.Unmarshal(content, config)
}
