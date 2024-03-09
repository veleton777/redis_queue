package logger

import "os"

type FileLogger struct {
	file *os.File
}

func NewFileLogger(file *os.File) *FileLogger {
	return &FileLogger{
		file: file,
	}
}

func (l *FileLogger) Err(msg string) {
	l.file.WriteString("ERR: " + msg + "\n")
}

func (l *FileLogger) Info(msg string) {
	l.file.WriteString("INFO: " + msg + "\n")
}

func (l *FileLogger) Success(msg string) {
	l.file.WriteString("SUCCESS: " + msg + "\n")
}
