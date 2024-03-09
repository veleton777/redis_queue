package logger

type Logger interface {
	Err(msg string)
	Info(msg string)
	Success(msg string)
}
