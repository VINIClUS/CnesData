package obs

import (
	"io"
	"log/slog"
	"os"

	"gopkg.in/natefinch/lumberjack.v2"
)

// NewJSONHandler retorna slog.Handler JSON gravando em w.
func NewJSONHandler(w io.Writer, level slog.Level) slog.Handler {
	return slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level:     level,
		AddSource: false,
	})
}

// NewRotatingHandler cria handler JSON com rotação via lumberjack.
// Retorna closer que fecha o arquivo — chamar no shutdown do processo.
func NewRotatingHandler(logPath string, level slog.Level) (slog.Handler, func()) {
	lj := &lumberjack.Logger{
		Filename:   logPath,
		MaxSize:    10,
		MaxBackups: 5,
		MaxAge:     30,
		Compress:   true,
	}
	handler := NewJSONHandler(lj, level)
	return handler, func() { _ = lj.Close() }
}

// NewStdoutHandler conveniência para modo foreground.
func NewStdoutHandler(level slog.Level) slog.Handler {
	return NewJSONHandler(os.Stdout, level)
}
