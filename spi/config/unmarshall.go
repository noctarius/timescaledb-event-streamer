package config

import (
	"github.com/BurntSushi/toml"
	"github.com/go-yaml/yaml"
)

func Unmarshall(content []byte, config *Config, toml bool) error {
	if toml {
		return fromToml(content, config)
	}
	return fromYaml(content, config)
}

func fromToml(content []byte, config *Config) error {
	return toml.Unmarshal(content, config)
}

func fromYaml(content []byte, config *Config) error {
	return yaml.Unmarshal(content, config)
}
