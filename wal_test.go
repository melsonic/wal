package wal

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func TestWALBasicAppendAndRead(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := DefaultOptions()
	w, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	records := []string{"first", "second", "third"}
	var seqs []uint64

	// Append
	for _, rec := range records {
		seq, err := w.Append([]byte(rec))
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}
		seqs = append(seqs, seq)
	}

	// Read
	for i, seq := range seqs {
		data, err := w.Read(seq)
		if err != nil {
			t.Fatalf("Read failed for seq %d: %v", seq, err)
		}
		if string(data) != records[i] {
			t.Errorf("Expected %q, got %q", records[i], string(data))
		}
	}

	err = w.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestWALSegmentRotation(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal-test-rot-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := DefaultOptions()
	// Small segment size to force rapid rotation
	// 16 byte header + a few records (16 byte header + data)
	opts.MaxSegmentSize = 64

	w, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Append enough to cause rotations
	for i := range 10 {
		_, err := w.Append(fmt.Appendf(nil, "data-%d", i))
		if err != nil {
			t.Fatalf("Append %d failed: %v", i, err)
		}
	}

	// Read everything back
	for i := range 10 {
		data, err := w.Read(uint64(i))
		if err != nil {
			t.Fatalf("Read %d failed: %v", i, err)
		}
		expected := fmt.Sprintf("data-%d", i)
		if string(data) != expected {
			t.Errorf("Expected %q, got %q", expected, string(data))
		}
	}

	// Should have multiple old segments now
	w.mu.RLock()
	oldSegCount := len(w.oldSegments)
	w.mu.RUnlock()

	if oldSegCount == 0 {
		t.Fatal("Expected WAL to rotate into multiple segments, but got 0 old segments")
	}

	w.Close()
}

func TestWALRecovery(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal-test-rec-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := DefaultOptions()
	w, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}

	// Write 5 entries
	for i := range 5 {
		w.Append(fmt.Appendf(nil, "entry-%d", i))
	}

	// Close it to simulate restart
	w.Close()

	// Reopen
	w2, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}

	// Next append should be seq 5
	seq, err := w2.Append([]byte("entry-5"))
	if err != nil {
		t.Fatalf("Append after reopen failed: %v", err)
	}
	if seq != 5 {
		t.Errorf("Expected new seq to be 5, got %d", seq)
	}

	// Can still read older entries
	data, err := w2.Read(0)
	if err != nil {
		t.Fatalf("Read 0 failed: %v", err)
	}
	if !bytes.Equal(data, []byte("entry-0")) {
		t.Errorf("Expected entry-0, got %s", data)
	}

	w2.Close()
}

func TestWALReadNotFound(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal-test-notfound-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	w, _ := Open(dir, DefaultOptions())
	defer w.Close()

	_, err = w.Read(999)
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound, got %v", err)
	}
}

func TestWALCircularBuffer(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal-test-circular-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := DefaultOptions()
	opts.MaxSegmentSize = 38
	opts.MaxGlobalSegment = 3 // Only 3 slots: 0, 1, 2

	w, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Append enough records to trigger multiple wraparounds.
	// Since MaxSegmentSize is 64, it will hold roughly 1 record per segment.
	var lastSeq uint64
	for i := range 10 {
		seq, err := w.Append(fmt.Appendf(nil, "data-%d", i))
		if err != nil {
			t.Fatalf("Append %d failed: %v", i, err)
		}
		lastSeq = seq
	}

	if lastSeq != 9 {
		t.Errorf("Expected sequence 9, got %d", lastSeq)
	}

	// Now check which sequences are still available.
	// Since we keep at most 3 segments, and each segment roughly holds 1 record,
	// only the most recent few sequences should be readable.
	for i := range uint64(10) {
		data, err := w.Read(i)

		if i < 7 { // These should be overwritten by now
			if err != ErrNotFound {
				t.Errorf("Expected ErrNotFound for seq %d, got err: %v, data: %s", i, err, string(data))
			}
		} else { // These should still exist
			if err != nil {
				t.Errorf("Expected seq %d to exist, got error: %v", i, err)
			}
			expected := fmt.Sprintf("data-%d", i)
			if string(data) != expected {
				t.Errorf("Expected %q, got %q", expected, string(data))
			}
		}
	}

	w.Close()

	// Test Recovery of Circular Buffer
	w2, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}

	// Check next seq
	seq, err := w2.Append([]byte("data-10"))
	if err != nil {
		t.Fatalf("Append after reopen failed: %v", err)
	}
	if seq != 10 {
		t.Errorf("Expected new seq to be 10, got %d", seq)
	}

	w2.Close()
}

func TestWALCircularBufferMaxSegmentOne(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal-test-circ-one-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := DefaultOptions()
	opts.MaxSegmentSize = 64
	opts.MaxGlobalSegment = 1 // Only 1 slot

	w, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	for i := range 5 {
		_, err := w.Append(fmt.Appendf(nil, "data-%d", i))
		if err != nil {
			t.Fatalf("Append %d failed: %v", i, err)
		}
	}

	// With MaxGlobalSegment=1, as soon as rotation happens, it overwrites slot 0.
	// Only the very latest entries will survive.
	_, err = w.Read(4)
	if err != nil {
		t.Errorf("Expected last element to exist, got err: %v", err)
	}

	w.Close()
}
