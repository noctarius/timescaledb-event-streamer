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
	"github.com/stretchr/testify/assert"
	"os"
	"reflect"
	"runtime"
	"testing"
)

func Test_Env_Vars(t *testing.T) {
	os.Setenv("FOO_BAR", "foo")
	defer os.Unsetenv("FOO_BAR")

	os.Setenv("FOO_BAR__BAZ", "bar")
	defer os.Unsetenv("FOO_BAR__BAZ")

	// On Windows environment variables are case-insensitive, therefore,
	// this test will always fail if trying to use different casing versions
	if runtime.GOOS != "windows" {
		os.Setenv("foo_bar", "bar")
		defer os.Unsetenv("foo_bar")

		os.Setenv("foo_bar__baz", "foo")
		defer os.Unsetenv("foo_bar__baz")
	}

	v, found := findEnvProperty("foo.bar", "test")
	assert.Equal(t, true, found)
	assert.Equal(t, "foo", v)

	v, found = findEnvProperty("foo.bar_baz", "test")
	assert.Equal(t, true, found)
	assert.Equal(t, "bar", v)

	v, found = findEnvProperty("oof.bar", "test")
	assert.Equal(t, false, found)
	assert.Equal(t, "test", v)

	v, found = findEnvProperty("oof.bar_baz", "test")
	assert.Equal(t, false, found)
	assert.Equal(t, "test", v)
}

func Test_Property_Extraction(t *testing.T) {
	config := Config{
		Sink: SinkConfig{
			Type: Kafka,
			Kafka: KafkaConfig{
				Brokers: []string{"foo", "bar"},
			},
		},
	}

	value := reflect.ValueOf(config)
	v1, found := findProperty(value, "sink")
	assert.Equal(t, true, found)

	v2, found := findProperty(v1, "type")
	assert.Equal(t, true, found)
	assert.Equal(t, "kafka", string(v2.Interface().(SinkType)))

	v3, found := findProperty(v1, "kafka")
	assert.Equal(t, true, found)

	v4, found := findProperty(v3, "brokers")
	assert.Equal(t, true, found)
	assert.Equal(t, []string{"foo", "bar"}, v4.Interface().([]string))
}

func Test_Config_Property_Reading(t *testing.T) {
	config := &Config{
		Sink: SinkConfig{
			Type: Kafka,
			Kafka: KafkaConfig{
				Brokers: []string{"foo", "bar"},
			},
		},
	}

	v1 := GetOrDefault(config, PropertySink, "foo")
	assert.Equal(t, "kafka", v1)

	v2 := GetOrDefault(config, PropertyKafkaBrokers, []string{"baz"})
	assert.Equal(t, []string{"foo", "bar"}, v2)

	v3 := GetOrDefault(config, PropertyKafkaTlsEnabled, true)
	assert.Equal(t, true, v3)

	v4 := GetOrDefault(config, "sink.kafka.non.existent", true)
	assert.Equal(t, true, v4)

	os.Setenv("SINK_TYPE", "redis")
	defer os.Unsetenv("SINK_TYPE")

	v5 := GetOrDefault(config, PropertySink, "foo")
	assert.Equal(t, "redis", v5)

}
