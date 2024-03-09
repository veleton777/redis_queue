package main

import "sync"

type Logger struct {
	qtyInfos   int
	qtyErrs    int
	qtySuccess int

	mu *sync.Mutex
}

func NewLogger() *Logger {
	return &Logger{
		mu: &sync.Mutex{},
	}
}

func (l *Logger) Info(msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.qtyInfos++
}

func (l *Logger) Err(msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.qtyErrs++
}

func (l *Logger) Success(msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.qtySuccess++
}
