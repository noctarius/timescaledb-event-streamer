package logging

import (
	"fmt"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/sirupsen/logrus"
	"runtime"
	"time"
)

const fixedFieldCaller = "caller"

var logger = logrus.New()

var WithVerbose = false
var WithDebug = false
var WithCaller = false

func init() {
	logger.SetFormatter(&logrus.TextFormatter{
		ForceColors: true,
		SortingFunc: func(strings []string) {
			if supporting.Contains(strings, fixedFieldCaller) {
				index := supporting.IndexOf(strings, fixedFieldCaller)
				for i := index + 1; i < len(strings); i++ {
					strings[i-1] = strings[i]
				}
				strings[len(strings)-1] = fixedFieldCaller
			}
		},
		PadLevelText:    true,
		TimestampFormat: time.RFC3339,
		FullTimestamp:   true,
	})
	logger.SetLevel(logrus.TraceLevel)
}

type Logger struct {
	name string
}

func NewLogger(name string) *Logger {
	return &Logger{
		name: name,
	}
}

func (l *Logger) Debugf(format string, args ...interface{}) {
	if WithDebug {
		getLogger().Debugf(fmt.Sprintf("[%s] %s", l.name, format), args...)
	}
}

func (l *Logger) Debugln(args ...interface{}) {
	if WithDebug {
		args = append([]any{fmt.Sprintf("[%s]", l.name)}, args...)
		getLogger().Println(args...)
	}
}

func (l *Logger) Verbosef(format string, args ...any) {
	if WithVerbose || WithDebug {
		getLogger().Infof(fmt.Sprintf("[%s] %s", l.name, format), args...)
	}
}

func (l *Logger) Verboseln(args ...any) {
	if WithVerbose || WithDebug {
		args = append([]any{fmt.Sprintf("[%s]", l.name)}, args...)
		getLogger().Infoln(args...)
	}
}

func (l *Logger) Printf(format string, args ...any) {
	getLogger().Infof(fmt.Sprintf("[%s] %s", l.name, format), args...)
}

func (l *Logger) Println(args ...any) {
	args = append([]any{fmt.Sprintf("[%s]", l.name)}, args...)
	getLogger().Infoln(args...)
}

func (l *Logger) Infof(format string, args ...any) {
	getLogger().Infof(fmt.Sprintf("[%s] %s", l.name, format), args...)
}

func (l *Logger) Infoln(args ...any) {
	args = append([]any{fmt.Sprintf("[%s]", l.name)}, args...)
	getLogger().Infoln(args...)
}

func (l *Logger) Warnf(format string, args ...interface{}) {
	getLogger().Warnf(fmt.Sprintf("[%s] %s", l.name, format), args...)
}

func (l *Logger) Warnln(args ...interface{}) {
	args = append([]any{fmt.Sprintf("[%s]", l.name)}, args...)
	getLogger().Warnln(args...)
}

func (l *Logger) Errorf(format string, args ...any) {
	logger.Printf(fmt.Sprintf("[%s] %s", l.name, format), args...)
}

func (l *Logger) Errorln(args ...any) {
	args = append([]any{fmt.Sprintf("[%s]", l.name)}, args...)
	logger.Println(args...)
}

func (l *Logger) Fatalf(format string, args ...any) {
	logger.Fatalf(fmt.Sprintf("[%s] %s", l.name, format), args...)
}

func (l *Logger) Fatalln(args ...any) {
	args = append([]any{fmt.Sprintf("[%s]", l.name)}, args...)
	logger.Fatalln(args...)
}

func getCaller() *string {
	if !WithCaller {
		return nil
	}
	if pc, _, line, ok := runtime.Caller(3); ok {
		f := runtime.FuncForPC(pc)
		caller := fmt.Sprintf("%s:%d", f.Name(), line)
		return &caller
	}
	return nil
}

func getLogger() logrus.FieldLogger {
	caller := getCaller()
	if caller != nil {
		return logger.WithField(fixedFieldCaller, *caller)
	}
	return logger
}
