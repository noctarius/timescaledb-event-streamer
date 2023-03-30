package logging

import (
	"fmt"
	"log"
)

type Logger struct {
	name string
}

func NewLogger(name string) *Logger {
	return &Logger{
		name: name,
	}
}

func (l *Logger) Printf(format string, args ...any) {
	log.Printf(fmt.Sprintf("[%s] %s", l.name, format), args...)
}

func (l *Logger) Println(v ...any) {
	v = append([]any{fmt.Sprintf("[%s]", l.name)}, v...)
	log.Println(v...)
}

func (l *Logger) Errorf(format string, args ...any) {
	log.Printf(fmt.Sprintf("[%s] %s", l.name, format), args...)
}

func (l *Logger) Errorln(v ...any) {
	v = append([]any{fmt.Sprintf("[%s]", l.name)}, v...)
	log.Println(v...)
}

func (l *Logger) Fatalf(format string, args ...any) {
	log.Fatalf(fmt.Sprintf("[%s] %s", l.name, format), args...)
}

func (l *Logger) Fatalln(v ...any) {
	v = append([]any{fmt.Sprintf("[%s]", l.name)}, v...)
	log.Fatalln(v...)
}
