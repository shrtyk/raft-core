package logger

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func captureStdout(t *testing.T, f func()) string {
	r, w, _ := os.Pipe()
	stdout := os.Stdout
	os.Stdout = w
	defer func() {
		os.Stdout = stdout
	}()

	f()
	w.Close()
	var buf bytes.Buffer
	_, err := io.Copy(&buf, r)
	require.NoError(t, err)
	return buf.String()
}

func TestNewLogger(t *testing.T) {
	t.Run("Prod", func(t *testing.T) {
		output := captureStdout(t, func() {
			log := NewLogger(Prod, false)
			log.Info("test info message")
			log.Debug("test debug message")
		})

		assert.Contains(t, output, `"level":"INFO"`)
		assert.Contains(t, output, `"msg":"test info message"`)
		assert.NotContains(t, output, `"msg":"test debug message"`)
		assert.NotContains(t, output, `"source":`)
	})

	t.Run("Dev", func(t *testing.T) {
		output := captureStdout(t, func() {
			log := NewLogger(Dev, false)
			log.Info("test info message")
			log.Debug("test debug message")
		})
		assert.Contains(t, output, `"level":"DEBUG"`)
		assert.Contains(t, output, `"msg":"test info message"`)
		assert.Contains(t, output, `"msg":"test debug message"`)
		assert.NotContains(t, output, `"source":`)
	})

	t.Run("Staging", func(t *testing.T) {
		output := captureStdout(t, func() {
			log := NewLogger(Staging, false)
			log.Info("test info message")
			log.Debug("test debug message")
		})
		assert.Contains(t, output, `"level":"INFO"`)
		assert.Contains(t, output, `"msg":"test info message"`)
		assert.NotContains(t, output, `"msg":"test debug message"`)
		assert.NotContains(t, output, `"source":`)
	})

	t.Run("addSource true includes source", func(t *testing.T) {
		output := captureStdout(t, func() {
			log := NewLogger(Dev, true)
			log.Info("test info message with source")
		})
		assert.Contains(t, output, `"level":"INFO"`)
		assert.Contains(t, output, `"msg":"test info message with source"`)
		assert.Contains(t, output, `"source":`)
		assert.Contains(t, output, "logger_test.go")
	})
}

func TestNewTestLogger(t *testing.T) {
	b, log := NewTestLogger()
	require.NotNil(t, b)
	require.NotNil(t, log)

	log.Info("test message")
	logged := b.String()

	assert.Contains(t, logged, "level=INFO")
	assert.Contains(t, logged, "msg=\"test message\"")
	assert.NotContains(t, logged, "source=")
}

func TestErrAttr(t *testing.T) {
	testErr := errors.New("something went wrong")
	attr := ErrAttr(testErr)

	assert.Equal(t, "error", attr.Key)
	assert.Equal(t, slog.StringValue("something went wrong").Kind(), attr.Value.Kind())
	assert.Equal(t, "something went wrong", attr.Value.String())
}
