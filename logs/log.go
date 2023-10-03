package logs

import (
	"io"
	"log"
	"os"
)

type SimpleLogger struct {
	infoLogger    *log.Logger
	warningLogger *log.Logger
	errorLogger   *log.Logger
}

func NewSimpleLogger(logFileName string) (*SimpleLogger, error) {
	file, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {

		return nil, err

	}

	return &SimpleLogger{
		infoLogger:    log.New(io.MultiWriter(os.Stdout, file), "[INFO] ", log.Ldate|log.Ltime|log.Lshortfile),
		warningLogger: log.New(io.MultiWriter(os.Stdout, file), "[WARNING] ", log.Ldate|log.Ltime|log.Lshortfile),
		errorLogger:   log.New(io.MultiWriter(os.Stdout, file), "[ERROR] ", log.Ldate|log.Ltime|log.Lshortfile),
	}, nil

}
func (l *SimpleLogger) Info(message string) {
	l.infoLogger.Println(message)
}

func (l *SimpleLogger) Warning(message string) {
	l.warningLogger.Println(message)
}

func (l *SimpleLogger) Error(message string) {
	l.errorLogger.Println(message)
}
func (l *SimpleLogger) Fatalf(message string) {
	l.errorLogger.Println(message)
}
func (l *SimpleLogger) Errorf(message string) {
	l.errorLogger.Println(message)
}
