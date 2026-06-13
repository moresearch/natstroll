package shared

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"

	"golang.org/x/term"
)

// ANSI escape codes for colorized log output.
const (
	ansiReset  = "\033[0m"
	ansiRed    = "\033[31m"
	ansiGreen  = "\033[32m"
	ansiYellow = "\033[33m"
	ansiBlue   = "\033[34m"
	ansiCyan   = "\033[36m"
	ansiGray   = "\033[90m"
	ansiBold   = "\033[1m"
	ansiDim    = "\033[2m"
)

// isTerminal reports whether the given file descriptor is a terminal.
func isTerminal(fd uintptr) bool {
	return term.IsTerminal(int(fd))
}

// colorHandler is a slog.Handler that writes colorized key=value log lines to w.
// Color is disabled when w is not a terminal (e.g. piped to a file).
type colorHandler struct {
	w          io.Writer
	level      slog.Leveler
	attrs      []slog.Attr
	groups     []string
	color      bool
	replaceAttr func(groups []string, a slog.Attr) slog.Attr
	mu         sync.Mutex
}

// ColorHandlerOptions holds options for the color handler.
type ColorHandlerOptions struct {
	Level       slog.Leveler
	ReplaceAttr func(groups []string, a slog.Attr) slog.Attr
}

// NewColorHandler creates a colorized slog.Handler that writes to w.
func NewColorHandler(w io.Writer, opts *ColorHandlerOptions) slog.Handler {
	h := &colorHandler{w: w, color: false}
	if opts != nil {
		if opts.Level != nil {
			h.level = opts.Level
		}
		h.replaceAttr = opts.ReplaceAttr
	}
	if h.level == nil {
		h.level = slog.LevelInfo
	}

	// Enable color only when writing to a terminal.
	if f, ok := w.(*os.File); ok {
		h.color = isTerminal(f.Fd())
	}

	return h
}

// Enabled reports whether the handler is enabled for the given level.
func (h *colorHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level.Level()
}

// Handle formats and writes a single log record.
func (h *colorHandler) Handle(_ context.Context, r slog.Record) error {
	var buf strings.Builder

	// Timestamp (dim).
	ts := r.Time.Format("15:04:05.000")
	if h.color {
		buf.WriteString(ansiDim)
	}
	buf.WriteString(ts)
	if h.color {
		buf.WriteString(ansiReset)
	}
	buf.WriteByte(' ')

	// Level (colorized, uppercase, fixed width).
	levelStr := r.Level.String()
	if h.color {
		buf.WriteString(h.levelColor(r.Level))
	}
	buf.WriteString(strings.ToUpper(levelStr))
	if h.color {
		buf.WriteString(ansiReset)
	}
	buf.WriteByte(' ')

	// Message.
	if h.color && r.Level >= slog.LevelError {
		buf.WriteString(ansiBold)
	}
	buf.WriteString(r.Message)
	if h.color && r.Level >= slog.LevelError {
		buf.WriteString(ansiReset)
	}
	buf.WriteByte(' ')

	// Attributes: handler-level first (from With/WithAttrs), then record-level.
	for _, a := range h.attrs {
		buf.WriteString(h.formatAttr(a))
	}
	r.Attrs(func(a slog.Attr) bool {
		buf.WriteString(h.formatAttr(a))
		return true
	})

	buf.WriteByte('\n')

	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := h.w.Write([]byte(buf.String()))
	return err
}

// WithAttrs returns a new handler with the given attributes pre-appended.
func (h *colorHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	h2 := h.clone()
	h2.attrs = append(h2.attrs, attrs...)
	return h2
}

// WithGroup returns a new handler with the given group name.
func (h *colorHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	h2 := h.clone()
	h2.groups = append(h2.groups, name)
	return h2
}

func (h *colorHandler) clone() *colorHandler {
	h2 := &colorHandler{
		w:       h.w,
		level:   h.level,
		color:   h.color,
		replaceAttr: h.replaceAttr,
	}
	h2.attrs = make([]slog.Attr, len(h.attrs))
	copy(h2.attrs, h.attrs)
	h2.groups = make([]string, len(h.groups))
	copy(h2.groups, h.groups)
	return h2
}

func (h *colorHandler) levelColor(level slog.Level) string {
	switch {
	case level >= slog.LevelError:
		return ansiRed
	case level >= slog.LevelWarn:
		return ansiYellow
	case level >= slog.LevelInfo:
		return ansiCyan
	default:
		return ansiGray
	}
}

func (h *colorHandler) formatAttr(a slog.Attr) string {
	// Resolve groups and apply replaceAttr if set.
	if h.replaceAttr != nil {
		a = h.replaceAttr(h.groups, a)
	}

	if a.Equal(slog.Attr{}) {
		return ""
	}

	var sb strings.Builder

	if h.color {
		sb.WriteString(ansiDim)
	}
	sb.WriteString(a.Key)

	if h.color {
		sb.WriteString(ansiReset)
	}

	if a.Value.Kind() != slog.KindGroup {
		sb.WriteByte('=')
		sb.WriteString(a.Value.String())
	}

	sb.WriteByte(' ')
	return sb.String()
}
