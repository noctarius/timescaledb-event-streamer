package main

import (
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/go-errors/errors"
	"github.com/jackc/pgx/v5"
	"github.com/noctarius/event-stream-prototype/internal/configuration"
	"github.com/noctarius/event-stream-prototype/internal/event/sink"
	"github.com/noctarius/event-stream-prototype/internal/event/sink/nats"
	"github.com/noctarius/event-stream-prototype/internal/event/sink/stdout"
	"github.com/noctarius/event-stream-prototype/internal/event/topic"
	"github.com/noctarius/event-stream-prototype/internal/replication"
	"github.com/noctarius/event-stream-prototype/internal/schema"
	"io"
	"os"
	"os/signal"
	"syscall"
)

const publicationName = "pg_ts_streamer"

var (
	configurationFile string
	verbose           bool
)

func init() {
	flag.StringVar(&configurationFile, "config", "", "The tool configuration file")
	flag.BoolVar(&verbose, "verbose", false, "Show verbose output")
	flag.Parse()

	if configurationFile == "" {
		fmt.Fprintln(os.Stderr, "No configuration file was provided. Please use -config parameter.")
		os.Exit(2)
	}
}

func main() {
	f, err := os.Open(configurationFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Configuration file couldn't be opened: %v\n", err)
		os.Exit(3)
	}

	b, err := io.ReadAll(f)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Configuration file couldn't be read: %v\n", err)
		os.Exit(4)
	}

	config := &configuration.Config{}
	if err := toml.Unmarshal(b, &config); err != nil {
		fmt.Fprintf(os.Stderr, "Configuration file couldn't be decoded: %v\n", err)
		os.Exit(5)
	}

	if config.PostgreSQL.ConnString == "" {
		fmt.Fprintf(os.Stderr, "PostgreSQL connection string required: %v\n", err)
		os.Exit(6)
	}
	connConfig, err := pgx.ParseConfig(config.PostgreSQL.ConnString)
	if err != nil {
		fmt.Fprintf(os.Stderr, "PostgreSQL connection string failed to parse: %v\n", err)
		os.Exit(6)
	}
	if config.PostgreSQL.Password != "" {
		connConfig.Password = config.PostgreSQL.Password
	}

	if config.PostgreSQL.Publication == "" {
		config.PostgreSQL.Publication = publicationName
	}

	replicator := replication.NewReplicator(config, connConfig)
	schemaRegistry := schema.NewSchemaRegistry()

	topicNameGenerator, err := newNameGenerator(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v", err)
		os.Exit(7)
	}

	eventEmitter, err := newEventEmitter(config, schemaRegistry, topicNameGenerator)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v", err)
		os.Exit(8)
	}

	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	go func() {
		<-sigs
		if err := replicator.StopReplication(); err != nil {
			fmt.Fprintf(os.Stderr, "Hard error when stopping replication: %v\n", err)
			os.Exit(1)
		}
		done <- true
	}()

	if err := replicator.StartReplication(schemaRegistry, topicNameGenerator, eventEmitter); err != nil {
		fmt.Fprintln(os.Stderr, err.(*errors.Error).ErrorStack())
		os.Exit(1)
	}

	<-done
}

func newEventEmitter(config *configuration.Config, schemaRegistry *schema.Registry,
	topicNameGenerator *topic.NameGenerator) (*sink.EventEmitter, error) {

	s, err := newSink(config)
	if err != nil {
		return nil, err
	}

	return sink.NewEventEmitter(schemaRegistry, topicNameGenerator, s), nil
}

func newSink(config *configuration.Config) (sink.Sink, error) {
	switch config.Sink.Type {
	case configuration.Stdout:
		return stdout.NewStdoutSink(), nil
	case configuration.NATS:
		return nats.NewNatsSink(config)
	}
	return nil, fmt.Errorf("SinkType '%s' doesn't exist", config.Sink.Type)
}

func newNameGenerator(config *configuration.Config) (*topic.NameGenerator, error) {
	namingStrategy, err := newNamingStrategy(config)
	if err != nil {
		return nil, err
	}
	return topic.NewNameGenerator(config.Topic.Prefix, namingStrategy), nil
}

func newNamingStrategy(config *configuration.Config) (topic.NamingStrategy, error) {
	switch config.Topic.NamingStrategy.Type {
	case configuration.Debezium:
		return &topic.DebeziumNamingStrategy{}, nil
	}
	return nil, fmt.Errorf("NamingStrategyType '%s' doesn't exist", config.Topic.NamingStrategy.Type)
}
