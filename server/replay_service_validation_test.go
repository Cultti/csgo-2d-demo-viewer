package main

import (
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func writeTempFile(t *testing.T, dir string, name string, payload []byte) *os.File {
	t.Helper()

	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatalf("failed to write temp demo: %v", err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("failed to open temp demo: %v", err)
	}
	return f
}

func TestValidateDemoFilestampZSTD(t *testing.T) {
	var compressed bytes.Buffer
	zw, err := zstd.NewWriter(&compressed)
	if err != nil {
		t.Fatalf("failed to create zstd writer: %v", err)
	}
	if _, err := zw.Write([]byte("PBDEMS2\x00body")); err != nil {
		t.Fatalf("failed to write zstd payload: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("failed to close zstd writer: %v", err)
	}

	f := writeTempFile(t, t.TempDir(), "demo.dem.zst", compressed.Bytes())
	defer f.Close()

	if err := validateDemoFilestamp(f, "demo.dem.zst"); err != nil {
		t.Fatalf("expected valid filestamp, got %v", err)
	}

	pos, err := f.Seek(0, os.SEEK_CUR)
	if err != nil {
		t.Fatalf("failed to read file position: %v", err)
	}
	if pos != 0 {
		t.Fatalf("expected file offset reset to 0, got %d", pos)
	}
}

func TestValidateDemoFilestampGZIPInvalid(t *testing.T) {
	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	if _, err := zw.Write([]byte("NOTDEMO!payload")); err != nil {
		t.Fatalf("failed to write gzip payload: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("failed to close gzip writer: %v", err)
	}

	f := writeTempFile(t, t.TempDir(), "demo.dem.gz", compressed.Bytes())
	defer f.Close()

	if err := validateDemoFilestamp(f, "demo.dem.gz"); err == nil {
		t.Fatalf("expected invalid filestamp error")
	}
}
