package main

import (
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/go-errors/errors"
	"github.com/noctarius/event-stream-prototype/internal"
	"github.com/noctarius/event-stream-prototype/internal/configuring"
	"github.com/noctarius/event-stream-prototype/internal/configuring/sysconfig"
	"io"
	"os"
	"os/signal"
	"syscall"
)

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

	config := &configuring.Config{}
	if err := toml.Unmarshal(b, &config); err != nil {
		fmt.Fprintf(os.Stderr, "Configuration file couldn't be decoded: %v\n", err)
		os.Exit(5)
	}

	if config.PostgreSQL.Connection == "" {
		fmt.Fprintf(os.Stderr, "PostgreSQL connection string required: %v\n", err)
		os.Exit(6)
	}

	systemConfig := sysconfig.NewSystemConfig(config)
	streamer, err, exitCode := internal.NewStreamer(systemConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(exitCode)
	}

	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	go func() {
		<-sigs
		if err := streamer.Stop(); err != nil {
			fmt.Fprintf(os.Stderr, "Hard error when stopping replication: %v\n", err)
			os.Exit(1)
		}
		done <- true
	}()

	if err := streamer.Start(); err != nil {
		fmt.Fprintln(os.Stderr, err.(*errors.Error).ErrorStack())
		os.Exit(1)
	}

	<-done
}
