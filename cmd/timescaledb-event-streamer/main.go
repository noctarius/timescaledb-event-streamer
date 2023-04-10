package main

import (
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/internal"
	"github.com/noctarius/timescaledb-event-streamer/internal/configuring"
	"github.com/noctarius/timescaledb-event-streamer/internal/configuring/sysconfig"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/internal/version"
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
}

func main() {
	fmt.Printf("%s version %s (git revision %s; branch %s)\n",
		version.BinName, version.Version, version.CommitHash, version.Branch,
	)

	config := &configuring.Config{}

	if configurationFile != "" {
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

		if err := toml.Unmarshal(b, &config); err != nil {
			fmt.Fprintf(os.Stderr, "Configuration file couldn't be decoded: %v\n", err)
			os.Exit(5)
		}
	}

	if configuring.GetOrDefault(config, "postgresql.connection", "") == "" {
		fmt.Fprintf(os.Stderr, "PostgreSQL connection string required")
		os.Exit(6)
	}

	systemConfig := sysconfig.NewSystemConfig(config)
	streamer, err, exitCode := internal.NewStreamer(systemConfig)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(exitCode)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	done := supporting.NewWaiter()
	go func() {
		<-signals
		if err := streamer.Stop(); err != nil {
			fmt.Fprintf(os.Stderr, "Hard error when stopping replication: %v\n", err)
			os.Exit(1)
		}
		done.Signal()
	}()

	if err := streamer.Start(); err != nil {
		fmt.Fprintln(os.Stderr, err.(*errors.Error).ErrorStack())
		os.Exit(1)
	}

	if err := done.Await(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(10)
	}
}
