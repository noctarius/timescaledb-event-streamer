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
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"os"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func Test_Env_Vars(
	t *testing.T,
) {

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

func Test_Property_Extraction(
	t *testing.T,
) {

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

func Test_Config_Property_Reading(
	t *testing.T,
) {

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

func Test_Config_Read_String_Ptr_From_String_Ptr(
	t *testing.T,
) {

	config := &Config{
		Sink: SinkConfig{
			Type: AwsSQS,
			AwsSqs: AwsSqsConfig{
				Queue: AwsSqsQueueConfig{
					Url: lo.ToPtr("test"),
				},
			},
		},
	}

	v := GetOrDefault[*string](config, PropertySqsQueueUrl, nil)
	assert.Equal(t, "test", *v)
}

func Test_Config_Read_String_From_String_Ptr(
	t *testing.T,
) {

	config := &Config{
		Sink: SinkConfig{
			Type: AwsSQS,
			AwsSqs: AwsSqsConfig{
				Queue: AwsSqsQueueConfig{
					Url: lo.ToPtr("test"),
				},
			},
		},
	}

	v := GetOrDefault[string](config, PropertySqsQueueUrl, "")
	assert.Equal(t, "test", v)
}

func Test_Config_Read_String_Ptr_From_String(
	t *testing.T,
) {

	config := &Config{
		PostgreSQL: PostgreSQLConfig{
			Connection: "test",
		},
	}

	v := GetOrDefault[*string](config, PropertyPostgresqlConnection, nil)
	assert.Equal(t, "test", *v)
}

func Test_Config_Read_String_From_String(
	t *testing.T,
) {

	config := &Config{
		PostgreSQL: PostgreSQLConfig{
			Connection: "test",
		},
	}

	v := GetOrDefault[string](config, PropertyPostgresqlConnection, "")
	assert.Equal(t, "test", v)
}

func Test_Loading_YAML_Logging_Config_From_String(
	t *testing.T,
) {

	yaml := `logging:
  level: 'info'
  outputs:
    console:
      enabled: true
    file:
      enabled: false
      path: '/path/to/logfile'
      rotate: true
      maxSize: '5MB'
      maxDuration: 600 #seconds
      compress: true
  loggers:
    LogicalReplicationResolver:
      level: 'debug'
      outputs:
        console:
          enabled: false`

	config := &Config{}
	if err := Unmarshall([]byte(yaml), config, false); err != nil {
		t.Error(err)
	}

	assert.Equal(t, "info", config.Logging.Level)

	assert.Equal(t, true, *config.Logging.Outputs.Console.Enabled)
	assert.Equal(t, false, *config.Logging.Outputs.File.Enabled)
	assert.Equal(t, "/path/to/logfile", config.Logging.Outputs.File.Path)
	assert.Equal(t, true, *config.Logging.Outputs.File.Rotate)
	assert.Equal(t, "5MB", *config.Logging.Outputs.File.MaxSize)
	assert.Equal(t, 600, *config.Logging.Outputs.File.MaxDuration)
	assert.Equal(t, true, config.Logging.Outputs.File.Compress)

	logger := config.Logging.Loggers["LogicalReplicationResolver"]
	assert.NotNil(t, logger)
	assert.Equal(t, "debug", *logger.Level)
	assert.Equal(t, false, *logger.Outputs.Console.Enabled)
}

func Test_Loading_YAML_Postgresql_Config_From_String(
	t *testing.T,
) {

	yaml := `postgresql:
  connection: 'postgres://repl_user@localhost:5432/postgres'
  password: '...'
  publication:
    name: 'replication_name'
    create: false
    autoDrop: true
  replicationSlot:
    name: 'slot_name'
    create: true
    autoDrop: true
  snapshot:
    batchSize: 1000
    initial: 'always'
  transaction:
    window:
      enabled: true
      timeout: 60
      maxSize: 100000`

	config := &Config{}
	if err := Unmarshall([]byte(yaml), config, false); err != nil {
		t.Error(err)
	}

	assert.Equal(t, "postgres://repl_user@localhost:5432/postgres", config.PostgreSQL.Connection)
	assert.Equal(t, "...", config.PostgreSQL.Password)

	assert.Equal(t, "replication_name", config.PostgreSQL.Publication.Name)
	assert.Equal(t, false, *config.PostgreSQL.Publication.Create)
	assert.Equal(t, true, *config.PostgreSQL.Publication.AutoDrop)

	assert.Equal(t, "slot_name", config.PostgreSQL.ReplicationSlot.Name)
	assert.Equal(t, true, *config.PostgreSQL.ReplicationSlot.Create)
	assert.Equal(t, true, *config.PostgreSQL.ReplicationSlot.AutoDrop)

	assert.Equal(t, uint(1000), config.PostgreSQL.Snapshot.BatchSize)
	assert.Equal(t, Always, *config.PostgreSQL.Snapshot.Initial)

	assert.Equal(t, true, *config.PostgreSQL.Transaction.Window.Enabled)
	assert.Equal(t, 60, config.PostgreSQL.Transaction.Window.Timeout)
	assert.Equal(t, uint(100000), config.PostgreSQL.Transaction.Window.MaxSize)
}

func Test_Loading_TOML_Logging_Config_From_String(
	t *testing.T,
) {

	toml := `logging.level = 'info'
logging.outputs.console.enabled = true
logging.outputs.file.enabled = false
logging.outputs.file.path = '/path/to/logfile'
logging.outputs.file.rotate = true
logging.outputs.file.maxsize = '5MB'
logging.outputs.file.maxduration = 600 #seconds
logging.outputs.file.compress = true
logging.loggers.LogicalReplicationResolver.level = 'debug'
logging.loggers.LogicalReplicationResolver.outputs.console.enabled = false`

	config := &Config{}
	if err := Unmarshall([]byte(toml), config, true); err != nil {
		t.Error(err)
	}

	assert.Equal(t, "info", config.Logging.Level)

	assert.Equal(t, true, *config.Logging.Outputs.Console.Enabled)
	assert.Equal(t, false, *config.Logging.Outputs.File.Enabled)
	assert.Equal(t, "/path/to/logfile", config.Logging.Outputs.File.Path)
	assert.Equal(t, true, *config.Logging.Outputs.File.Rotate)
	assert.Equal(t, "5MB", *config.Logging.Outputs.File.MaxSize)
	assert.Equal(t, 600, *config.Logging.Outputs.File.MaxDuration)
	assert.Equal(t, true, config.Logging.Outputs.File.Compress)

	logger := config.Logging.Loggers["LogicalReplicationResolver"]
	assert.NotNil(t, logger)
	assert.Equal(t, "debug", *logger.Level)
	assert.Equal(t, false, *logger.Outputs.Console.Enabled)
}

func Test_Loading_TOML_Postgresql_Config_From_String(
	t *testing.T,
) {

	toml := `postgresql.connection = 'postgres://repl_user@localhost:5432/postgres'
postgresql.password = '...'
postgresql.publication.name = 'replication_name'
postgresql.publication.create = false
postgresql.publication.autodrop = true
postgresql.replicationslot.name = 'slot_name'
postgresql.replicationslot.create = true
postgresql.replicationslot.autodrop = true
postgresql.snapshot.batchsize = 1000
postgresql.snapshot.initial = 'always'
postgresql.transaction.window.enabled = true
postgresql.transaction.window.timeout = 60
postgresql.transaction.window.maxsize = 100000`

	config := &Config{}
	if err := Unmarshall([]byte(toml), config, true); err != nil {
		t.Error(err)
	}

	assert.Equal(t, "postgres://repl_user@localhost:5432/postgres", config.PostgreSQL.Connection)
	assert.Equal(t, "...", config.PostgreSQL.Password)

	assert.Equal(t, "replication_name", config.PostgreSQL.Publication.Name)
	assert.Equal(t, false, *config.PostgreSQL.Publication.Create)
	assert.Equal(t, true, *config.PostgreSQL.Publication.AutoDrop)

	assert.Equal(t, "slot_name", config.PostgreSQL.ReplicationSlot.Name)
	assert.Equal(t, true, *config.PostgreSQL.ReplicationSlot.Create)
	assert.Equal(t, true, *config.PostgreSQL.ReplicationSlot.AutoDrop)

	assert.Equal(t, uint(1000), config.PostgreSQL.Snapshot.BatchSize)
	assert.Equal(t, Always, *config.PostgreSQL.Snapshot.Initial)

	assert.Equal(t, true, *config.PostgreSQL.Transaction.Window.Enabled)
	assert.Equal(t, 60, config.PostgreSQL.Transaction.Window.Timeout)
	assert.Equal(t, uint(100000), config.PostgreSQL.Transaction.Window.MaxSize)
}

func Test_Config_Tags_Match_Between_Yaml_Toml(
	t *testing.T,
) {

	configValue := reflect.ValueOf(Config{})

	var recursiveCheck func(value reflect.Value, path string)
	recursiveCheck = func(value reflect.Value, path string) {
		numOfFields := value.NumField()
		for i := 0; i < numOfFields; i++ {
			fieldType := value.Type().Field(i)

			toml := fieldType.Tag.Get("toml")
			yaml := fieldType.Tag.Get("yaml")

			fieldPath := toml
			if len(path) > 0 {
				fieldPath = path + "." + fieldPath
			}

			if strings.ToLower(yaml) != toml {
				t.Errorf("Yaml and Toml tags aren't matching at path %s: %s != %s", fieldPath, yaml, toml)
			}

			fieldValue := value.Field(i)
			if fieldValue.Kind() == reflect.Struct {
				recursiveCheck(fieldValue, fieldPath)
			}
		}
	}

	recursiveCheck(configValue, "")
}
