package xwal

import (
	"io"
	"log/slog"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(m.Run())
}

func cleanupTrunks(t *testing.T, trunks *Trunks) {
	t.Helper()
	t.Cleanup(func() {
		if err := trunks.Close(); err != nil {
			t.Errorf("close trunks: %v", err)
		}
	})
}
