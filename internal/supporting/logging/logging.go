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

package logging

import (
	"fmt"
	"github.com/go-errors/errors"
	"github.com/gookit/color"
	"github.com/gookit/slog"
	"github.com/gookit/slog/handler"
	"github.com/gookit/slog/rotatefile"
	"github.com/inhies/go-bytesize"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"os"
	"strings"
	"sync"
	"time"
)

var WithVerbose = false
var WithCaller = false

const (
	VerboseLevel slog.Level        = 650
	fiveMegabyte bytesize.ByteSize = 5242880
)

var (
	loggingConfig                spiconfig.LoggerConfig
	defaultLevel                 slog.Level
	defaultConsoleHandler        slog.Handler
	defaultFileHandler           *handler.SyncCloseHandler
	fileHandlers                 = make(map[string]*handler.SyncCloseHandler)
	defaultConsoleHandlerEnabled bool
)

func InitializeLogging(config *spiconfig.Config, logToStdErr bool) error {
	slog.LevelNames[VerboseLevel] = "VERBOSE"
	slog.AllLevels = slog.Levels{
		slog.PanicLevel,
		slog.FatalLevel,
		slog.ErrorLevel,
		slog.WarnLevel,
		slog.NoticeLevel,
		slog.InfoLevel,
		VerboseLevel,
		slog.DebugLevel,
		slog.TraceLevel,
	}
	slog.NormalLevels = slog.Levels{
		slog.InfoLevel,
		slog.NoticeLevel,
		slog.DebugLevel,
		slog.TraceLevel,
		VerboseLevel,
	}
	slog.ColorTheme[VerboseLevel] = color.FgLightGreen

	loggingConfig = config.Logging
	defaultLevel = Name2Level(loggingConfig.Level)

	defaultConsoleHandler = newConsoleHandler(logToStdErr)

	defaultConsoleHandlerEnabled =
		loggingConfig.Outputs.Console.Enabled == nil || *loggingConfig.Outputs.Console.Enabled

	if _, fileHandler, err := newFileHandler(loggingConfig.Outputs.File); err != nil {
		return supporting.AdaptError(err, 1)
	} else {
		defaultFileHandler = fileHandler
	}
	return nil
}

func newConsoleHandler(logToStdErr bool) slog.Handler {
	consoleHandler := handler.NewConsoleHandler(slog.AllLevels)
	if !WithCaller {
		consoleHandler.TextFormatter().SetTemplate(
			"[{{datetime}}] [{{level}}] {{message}} {{data}} {{extra}}\n",
		)
	} else {
		consoleHandler.TextFormatter().SetTemplate(
			"[{{datetime}}] [{{level}}] [{{caller}}] {{message}} {{data}} {{extra}}\n",
		)
	}
	if logToStdErr {
		consoleHandler.IOWriterHandler = *handler.NewIOWriterHandler(os.Stderr, slog.AllLevels)
	}
	return &consoleHandlerSyncAdapter{ConsoleHandler: consoleHandler}
}

type consoleHandlerSyncAdapter struct {
	*handler.ConsoleHandler
	mutex sync.Mutex
}

func (h *consoleHandlerSyncAdapter) Handle(record *slog.Record) error {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	return h.ConsoleHandler.Handle(record)
}

type Logger struct {
	slogger *slog.Logger
	level   slog.Level
	name    string
}

func NewLogger(name string) (*Logger, error) {
	baseConfiguration := func(l *slog.Logger) {
		l.CallerSkip = l.CallerSkip + 2
		l.ReportCaller = WithCaller
	}

	if config, found := loggingConfig.Loggers[name]; found {
		// Found specific config
		handlers := make([]slog.Handler, 0)
		if config.Outputs.Console.Enabled == nil || *config.Outputs.Console.Enabled {
			handlers = append(handlers, defaultConsoleHandler)
		}

		found, fileHandler, err := newFileHandler(config.Outputs.File)
		if err != nil {
			return nil, err
		}

		if found {
			handlers = append(handlers, fileHandler)
		} else if defaultFileHandler != nil {
			handlers = append(handlers, defaultFileHandler)
		}

		slogger := slog.NewWithName(name, func(l *slog.Logger) {
			baseConfiguration(l)
			l.AddHandlers(handlers...)
		})

		level := defaultLevel
		if config.Level != nil {
			level = Name2Level(*config.Level)
		}

		return &Logger{
			level:   level,
			slogger: slogger,
			name:    name,
		}, nil
	}

	slogger := slog.NewWithName(name, func(l *slog.Logger) {
		baseConfiguration(l)
		if defaultConsoleHandlerEnabled {
			l.AddHandler(defaultConsoleHandler)
		}
		if defaultFileHandler != nil {
			l.AddHandler(defaultFileHandler)
		}
	})
	return &Logger{
		level:   defaultLevel,
		slogger: slogger,
		name:    name,
	}, nil
}

func (l *Logger) Tracef(format string, args ...interface{}) {
	l.logf(slog.TraceLevel, format, args)
}

func (l *Logger) Traceln(args ...interface{}) {
	l.log(slog.TraceLevel, args)
}

func (l *Logger) Debugf(format string, args ...interface{}) {
	l.logf(slog.DebugLevel, format, args)
}

func (l *Logger) Debugln(args ...interface{}) {
	l.log(slog.DebugLevel, args)
}

func (l *Logger) Verbosef(format string, args ...any) {
	l.logf(VerboseLevel, format, args)
}

func (l *Logger) Verboseln(args ...any) {
	l.log(VerboseLevel, args)
}

func (l *Logger) Printf(format string, args ...any) {
	l.logf(slog.InfoLevel, format, args)
}

func (l *Logger) Println(args ...any) {
	l.log(slog.InfoLevel, args)
}

func (l *Logger) Infof(format string, args ...any) {
	l.logf(slog.InfoLevel, format, args)
}

func (l *Logger) Infoln(args ...any) {
	l.log(slog.InfoLevel, args)
}

func (l *Logger) Warnf(format string, args ...interface{}) {
	l.logf(slog.WarnLevel, format, args)
}

func (l *Logger) Warnln(args ...interface{}) {
	l.log(slog.WarnLevel, args)
}

func (l *Logger) Errorf(format string, args ...any) {
	l.logf(slog.ErrorLevel, format, args)
}

func (l *Logger) Errorln(args ...any) {
	l.log(slog.ErrorLevel, args)
}

func (l *Logger) Fatalf(format string, args ...any) {
	l.logf(slog.FatalLevel, format, args)
}

func (l *Logger) Fatalln(args ...any) {
	l.log(slog.FatalLevel, args)
}

func (l *Logger) logf(level slog.Level, format string, args []any) {
	if l.level >= level || (level == VerboseLevel && WithVerbose) {
		format = strings.TrimSuffix(format, "\n")
		l.slogger.Logf(level, fmt.Sprintf("[%s] %s", l.name, format), args...)
	}
}

func (l *Logger) log(level slog.Level, args []any) {
	if l.level >= level || (level == VerboseLevel && WithVerbose) {
		args = append([]any{fmt.Sprintf("[%s]", l.name)}, args...)
		l.slogger.Log(level, args...)
	}
}

func Name2Level(ln string) slog.Level {
	switch strings.ToLower(ln) {
	case "panic":
		return slog.PanicLevel
	case "fatal":
		return slog.FatalLevel
	case "err", "error":
		return slog.ErrorLevel
	case "warn", "warning":
		return slog.WarnLevel
	case "notice":
		return slog.NoticeLevel
	case "verbose":
		return VerboseLevel
	case "debug":
		return slog.DebugLevel
	case "trace":
		return slog.TraceLevel
	default:
		return slog.InfoLevel
	}
}

func newFileHandler(config spiconfig.LoggerFileConfig) (bool, *handler.SyncCloseHandler, error) {
	if config.Enabled == nil || !*config.Enabled {
		return false, nil, nil
	}

	// Already cached?
	if h, ok := fileHandlers[config.Path]; ok {
		return true, h, nil
	}

	configurator := func(c *handler.Config) {
		c.Levels = slog.AllLevels
		c.Level = slog.TraceLevel
		c.Compress = config.Compress
	}

	var fileHandler *handler.SyncCloseHandler
	if config.Rotate == nil || !*config.Rotate {
		if h, err := handler.NewBuffFileHandler(config.Path, 1024, configurator); err != nil {
			return false, nil, errors.Errorf(
				fmt.Sprintf("Failed to initialize logfile handler => %s", err.Error()),
			)
		} else {
			fileHandler = h
		}
	}

	if fileHandler == nil {
		if config.MaxDuration != nil {
			seconds := rotatefile.RotateTime((time.Second * time.Duration(*config.MaxDuration)).Seconds())
			if h, err := handler.NewTimeRotateFileHandler(config.Path, seconds, configurator); err != nil {
				return false, nil, errors.Errorf(
					fmt.Sprintf("Failed to initialize logfile handler => %s", err.Error()),
				)
			} else {
				fileHandler = h
			}
		}
	}

	if fileHandler == nil {
		maxSize := fiveMegabyte
		if config.MaxSize != nil {
			bs, err := bytesize.Parse(*config.MaxSize)
			if err != nil {
				return false, nil, errors.Errorf(
					fmt.Sprintf("Failed to parse max size property '%s' => %s", *config.MaxSize, err.Error()),
				)
			}
			maxSize = bs
		}

		if h, err := handler.NewSizeRotateFileHandler(config.Path, int(maxSize), configurator); err != nil {
			return false, nil, errors.Errorf(
				fmt.Sprintf("Failed to initialize logfile handler => %s", err.Error()),
			)
		} else {
			fileHandler = h
		}
	}

	fileHandlers[config.Path] = fileHandler
	return true, fileHandler, nil
}
