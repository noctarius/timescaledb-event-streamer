package configuration

type SinkType string

const (
	Stdout SinkType = "stdout"
	NATS   SinkType = "nats"
)

type NamingStrategyType string

const (
	Debezium NamingStrategyType = "debezium"
)

type NatsAuthorizationType string

const (
	UserInfo    NatsAuthorizationType = "userinfo"
	Credentials NatsAuthorizationType = "credentials"
	Jwt         NatsAuthorizationType = "jwt"
)

type Config struct {
	PostgreSQL struct {
		Connection  string `toml:"connection"`
		Password    string `toml:"password"`
		Publication string `toml:"publication"`
	} `toml:"postgresql"`

	Sink struct {
		Type SinkType `toml:"type"`
		Nats struct {
			Address       string                `toml:"address"`
			Authorization NatsAuthorizationType `toml:"authorization"`
			UserInfo      struct {
				Username string `toml:"username"`
				Password string `toml:"password"`
			} `toml:"userinfo"`
			Credentials struct {
				Certificate string   `toml:"certificate"`
				Seeds       []string `toml:"seeds"`
			} `toml:"credentials"`
			JWT struct {
				JWT  string `toml:"jwt"`
				Seed string `toml:"seed"`
			} `toml:"jwt"`
		} `toml:"nats"`
	} `toml:"sink"`

	Topic struct {
		NamingStrategy struct {
			Type NamingStrategyType `toml:"type"`
		} `toml:"namingstrategy"`
		Prefix string `toml:"prefix"`
	} `toml:"topic"`

	TimescaleDB struct {
		Hypertables struct {
			Excludes []string `toml:"excludes"`
			Includes []string `toml:"includes"`
		} `toml:"hypertables"`
		Events struct {
			Read          bool `toml:"read"`
			Insert        bool `toml:"insert"`
			Update        bool `toml:"update"`
			Delete        bool `toml:"delete"`
			Truncate      bool `toml:"truncate"`
			Compression   bool `toml:"compression"`
			Decompression bool `toml:"decompression"`
		} `toml:"events"`
	} `toml:"timescaledb"`
}
