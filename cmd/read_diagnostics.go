package cmd

import (
	"os"
	"sync"

	"github.com/JiangHe12/opskit-core/v2/redact"

	"github.com/JiangHe12/mqgov-cli/internal/tlspin"
)

const (
	maxReadDiagnosticBytes       = 64 * 1024
	readDiagnosticOverflowNotice = "warning: additional backend diagnostics were suppressed\n"
)

type readDiagnosticBuffer struct {
	mu      sync.Mutex
	stderr  []byte
	dropped bool
	done    bool
}

func (buffer *readDiagnosticBuffer) appendStderr(value string) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	if buffer.done {
		return
	}
	if len(value) > maxReadDiagnosticBytes-len(buffer.stderr) {
		buffer.dropped = true
		return
	}
	value = redact.String(value)
	if len(value) > maxReadDiagnosticBytes-len(buffer.stderr) {
		buffer.dropped = true
		return
	}
	buffer.stderr = append(buffer.stderr, value...)
}

func (buffer *readDiagnosticBuffer) notifyTLS(event tlspin.Event) {
	buffer.appendStderr(tlspin.FormatNotification(event))
}

func (buffer *readDiagnosticBuffer) complete(flush bool, write func([]byte) (int, error)) {
	buffer.mu.Lock()
	if buffer.done {
		buffer.mu.Unlock()
		return
	}
	buffer.done = true
	data := buffer.stderr
	dropped := buffer.dropped
	buffer.stderr = nil
	buffer.mu.Unlock()

	if !flush {
		return
	}
	if write == nil {
		write = os.Stderr.Write
	}
	if len(data) > 0 {
		_, _ = write(data)
	}
	if dropped {
		_, _ = write([]byte(readDiagnosticOverflowNotice))
	}
}

func readTLSNotify(f *cliFlags) tlspin.NotifyFunc {
	if f == nil || f.readDiagnostics == nil {
		return tlspin.NotifyDiscard
	}
	return f.readDiagnostics.notifyTLS
}
