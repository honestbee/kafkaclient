package kafkaclient

import (
	"fmt"
	"log"
)

// DefaultLogger default logger
type DefaultLogger struct{}

// NewDefaultLogger returns a new default logger
func NewDefaultLogger() *DefaultLogger {
	return new(DefaultLogger)
}

// Info prints info log
func (l *DefaultLogger) Info(message string, data map[string]interface{}) {
	log.Println(fmt.Sprintf("[Info] %s %v", message, data))
}

// Error prints error log
func (l *DefaultLogger) Error(message string, data map[string]interface{}) {
	log.Println(fmt.Sprintf("[Error] %s %v", message, data))
}
