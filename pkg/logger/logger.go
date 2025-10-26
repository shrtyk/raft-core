package logger

import (
	"bytes"
	"log/slog"
	"os"
)

// Can be one of:
//   - Prod
//   - Dev
//   - Staging
type Enviroment int

const (
	_ Enviroment = iota
	Prod
	Dev
	Staging
)

// NewLogger creates new slog.Logger and return pointer to it
func NewLogger(env Enviroment) *slog.Logger {
	var level slog.Level

	switch env {
	case Prod, Staging:
		level = slog.LevelInfo
	case Dev:
		level = slog.LevelDebug
	}

	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     level,
	})
	return slog.New(h)
}

func NewTestLogger() (*bytes.Buffer, *slog.Logger) {
	b := &bytes.Buffer{}
	l := slog.New(slog.NewTextHandler(b, &slog.HandlerOptions{AddSource: false}))
	return b, l
}

func ErrAttr(err error) slog.Attr {
	return slog.String("error", err.Error())
}
