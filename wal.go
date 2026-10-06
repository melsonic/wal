// Package wal implements a Write-Ahead Log (WAL).
//
// A Write-Ahead Log is a fundamental concept in database systems and distributed
// systems used to ensure data durability and crash recovery. The core idea is that
// all modifications are written to an append-only log on stable storage before they
// are applied to the main data structures. If a crash occurs, the system can
// recover its state by replaying the log.
//
// Key Concepts:
//   - Record: The smallest unit of data in the WAL. It wraps the user's payload
//     along with metadata like a CRC checksum to detect data corruption.
//   - Segment: To prevent the log file from growing indefinitely, the WAL is split
//     into multiple smaller files called segments. When the active segment reaches
//     a maximum size, the WAL rotates to a new segment. Older segments can be
//     archived or deleted once they are no longer needed.
//   - Sequence Number / Offset: A unique, monotonically increasing identifier for
//     each appended record. This allows tracking the current state and reading
//     specific historical entries.
package wal

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"melsonic/wal/internal/record"
)

var (
	ErrLogClosed = errors.New("log is closed")
	ErrNotFound  = errors.New("entry not found")
)

// WAL represents a Write-Ahead Log.
// It manages a directory of segment files and provides thread-safe operations
// for appending new records and reading historical records.
type WAL struct {
	mu   sync.RWMutex
	path string   // Directory path where the WAL segment files are stored
	file *os.File // The current active segment file being written to

	// active segment reference
	currentSegment uint64
	// a list/slice of older segments for read operations
	oldSegments []uint64
	// current active segment size
	activeSize uint64
	options    *Options
}

// Options contains configuration for the WAL behavior.
type Options struct {
	// MaxSegmentSize dictates the maximum size in bytes of a single segment file.
	// Once the active segment exceeds this size, the WAL should rotate to a new file.
	MaxSegmentSize uint64

	// SyncOnWrite determines if the WAL should force a disk sync (fsync) after every
	// Append operation. Setting this to true guarantees durability but heavily
	// impacts write throughput. If false, the OS decides when to flush, which is
	// faster but risks data loss on power failure.
	SyncOnWrite bool

	MaxGlobalSegment int8
}

// DefaultOptions returns the default options for the WAL.
func DefaultOptions() Options {
	return Options{
		MaxSegmentSize:   1024 * 1024 * 20, // 20 MB
		SyncOnWrite:      false,
		MaxGlobalSegment: 100,
	}
}

// Open opens or creates a WAL at the given path with the specified options.
func Open(path string, opts Options) (*WAL, error) {
	wal := WAL{path: path, options: &opts}
	_, err := os.Stat(path)

	if errors.Is(err, fs.ErrNotExist) {
		err := os.MkdirAll(path, 0o755)
		if err != nil {
			return nil, err
		}

		// create initial segment
		wal.currentSegment = 0
		filename := fmt.Sprintf("%020d.seg", wal.currentSegment)
		fullSegmentPath := filepath.Join(path, filename)
		segment, err := os.OpenFile(fullSegmentPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
		if err != nil {
			return nil, err
		}
		wal.file = segment
		wal.activeSize = 0
	} else {
		// load the existing
		pattern := filepath.Join(path, "*.seg")
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return nil, err
		}

		var segNums []uint64
		for _, match := range matches {
			baseName := filepath.Base(match)
			nameWithoutExt := strings.TrimSuffix(baseName, ".seg")
			num, err := strconv.ParseUint(nameWithoutExt, 10, 64)
			if err != nil {
				continue
			}
			segNums = append(segNums, num)
		}

		if len(segNums) == 0 {
			wal.currentSegment = 0
			filename := fmt.Sprintf("%020d.seg", wal.currentSegment)
			fullSegmentPath := filepath.Join(path, filename)
			segment, err := os.OpenFile(fullSegmentPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
			if err != nil {
				return nil, err
			}
			wal.file = segment
			wal.activeSize = 0
		} else {
			wal.currentSegment = segNums[len(segNums)-1]
			wal.oldSegments = segNums[:len(segNums)-1]
			filename := fmt.Sprintf("%020d.seg", wal.currentSegment)
			lastSegmentPath := filepath.Join(path, filename)

			file, err := os.OpenFile(lastSegmentPath, os.O_RDWR|os.O_APPEND, 0o644)
			if err != nil {
				return nil, err
			}
			wal.file = file

			stat, err := file.Stat()
			if err != nil {
				return nil, err
			}
			wal.activeSize = uint64(stat.Size())
		}
	}
	return &wal, nil
}

// Append appends a new entry to the write-ahead log.
// It returns the sequence number/offset of the appended entry.
func (w *WAL) Append(data []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 1. Check if the active segment has enough space
	logSize := uint64(len(data) + 8)
	if logSize > w.options.MaxSegmentSize-w.activeSize {
		w.activeSize = 0
		w.oldSegments = append(w.oldSegments, w.currentSegment)
		w.currentSegment = (w.currentSegment + 1) % uint64(w.options.MaxGlobalSegment)

		filename := fmt.Sprintf("%020d.seg", w.currentSegment)
		lastSegmentPath := filepath.Join(w.path, filename)

		file, err := os.OpenFile(lastSegmentPath, os.O_RDWR|os.O_APPEND, 0o644)
		if err != nil {
			return 0, err
		}
		w.file = file
	}
	// 3. Write data to the active segment (using internal/record format)
	record.Encode(data)

	// 4. Sync to disk if SyncOnWrite is true
	return logSize, nil
}

// Read reads an entry from the log at the given sequence number/offset.
func (w *WAL) Read(seq uint64) ([]byte, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	// TODO: Implement
	// 1. Find the segment containing the sequence number
	// 2. Read the record from the segment
	// 3. Verify checksum
	return nil, nil
}

// Sync flushes any buffered data to the underlying storage.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// TODO: Implement
	// Sync the active segment file to disk
	return nil
}

// Close closes the write-ahead log.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// TODO: Implement
	// Close all open segment files
	return nil
}
