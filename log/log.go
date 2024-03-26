package log

import (
	"log"
	"log/slog"
	"os"
)

var (
	level = &slog.LevelVar{}

	Logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	Debug = slog.NewLogLogger(Logger.Handler(), slog.LevelDebug)
	Info  = slog.NewLogLogger(Logger.Handler(), slog.LevelInfo)
	Error = slog.NewLogLogger(Logger.Handler(), slog.LevelError)
)

type Level = slog.Level

var (
	DebugLevel = slog.LevelDebug
	InfoLevel  = slog.LevelInfo
	ErrorLevel = slog.LevelError
)

func New(level Level, prefix string) *log.Logger {
	logger := slog.NewLogLogger(Logger.Handler(), level)
	logger.SetPrefix(prefix)
	return logger
}

func SetPrefix(prefix string) {
	for _, logger := range []*log.Logger{Debug, Info, Error} {
		logger.SetPrefix(prefix)
	}
}

func SetLevel(l Level) {
	level.Set(l)
}
