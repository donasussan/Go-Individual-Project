package logs

import (
	"os"
	"testing"
)

func TestLogger(t *testing.T) {
	tmpLogFile, err := os.CreateTemp("", "testlog*.log")
	if err != nil {
		t.Fatalf("Failed to create temporary log file: %v", err)
	}
	defer tmpLogFile.Close()

	logger, err := NewSimpleLogger(tmpLogFile.Name())
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	defer logger.logFile.Close()

	logger.Info("This is an info message")
	logger.Warning("This is a warning message")
	logger.Error("This is an error message")

}

func TestLoggerWithErrors(t *testing.T) {
	_, err := NewSimpleLogger("/nonexistentdirectory/test.log")
	if err == nil {
		t.Error("Expected an error when creating logger with non-existent directory, but got nil")
	}
}

func TestLoggingFunctions(t *testing.T) {
	tmpLogFile, err := os.CreateTemp("", "testlog*.log")
	if err != nil {
		t.Fatalf("Failed to create temporary log file: %v", err)
	}
	defer tmpLogFile.Close()

	logger, err := NewSimpleLogger(tmpLogFile.Name())
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	defer logger.logFile.Close()
	logger.Info("This is an info message")
	logger.Warning("This is a warning message")
	logger.Error("This is an error message")
	logger.Fatalf("This is a fatal error message")
}

func TestLogInitialization(t *testing.T) {
	InsForLogging()

	if NewLog == nil {
		t.Error("Logger is not initialized")
	}

	NewLog.Info("This is an info message")
	NewLog.Warning("This is a warning message")
	NewLog.Error("This is an error message")

	defer NewLog.logFile.Close()

}
