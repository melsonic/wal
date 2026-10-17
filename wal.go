// Package wal provides a durable, append-only Write-Ahead Log (WAL).
//
// Theory of Operation:
// A Write-Ahead Log is a fundamental component in database systems used to ensure
// atomicity and durability (the 'A' and 'D' in ACID). Before making changes to the actual
// state of the system, the changes are first appended to this log. If the system crashes,
// it can replay the log upon recovery to restore its state.
//
// Design:
// 1. Append-Only: Writes are strictly sequential, which maximizes disk throughput
//    by avoiding random disk seeks.
// 2. Segmentation: The log is divided into multiple "segments" (e.g., 00000000000000000000.seg).
//    This bounds the size of individual files, making them easier to manage, memory-map,
//    or eventually garbage collect (purging old data).
// 3. Indexing: Every `.seg` file has a companion `.idx` file. The index maps a global
//    sequence number (Seq) directly to the byte offset in the `.seg` file where that record
//    starts. This enables fast O(1) lookups for any sequence number without scanning the entire file.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"melsonic/wal/internal/record"
)

var (
	ErrLogClosed = errors.New("log is closed")
	ErrNotFound  = errors.New("entry not found")
)

const (
	segmentHeaderSize = 16
	baseSeqOffset     = 0
	maxSeqOffset      = 8
)

// segmentInfo holds metadata about a specific log segment.
// A segment consists of two physical files on disk:
// 1. Data file (.seg): Contains the actual binary records appended to the log.
// 2. Index file (.idx): Contains fixed-size 8-byte offsets mapping a logical
//    sequence number (relative to baseSeq) to the physical byte offset in the .seg file.
type segmentInfo struct {
	slotID  int    // The physical circular slot this segment occupies (0 to MaxGlobalSegment-1)
	path    string // Path to the data segment file (.seg)
	idxPath string // Path to the companion index file (.idx)
	baseSeq uint64 // The first sequence number that was written to this segment
	maxSeq  uint64 // The last sequence number written to this segment (inclusive)
}

// WAL represents a Write-Ahead Log instance.
// It manages the active segment being written to, as well as a collection of older,
// immutable segments used for reading historical data. Concurrency is managed via
// a reader-writer mutex, allowing multiple concurrent readers but only serializing writers.
type WAL struct {
	mu      sync.RWMutex
	path    string
	file    *os.File // Active .seg file
	idxFile *os.File // Active .idx file

	nextSeq     uint64        // The global sequence number for the next appended record
	activeSeg   segmentInfo   // Metadata for the currently active segment
	oldSegments []segmentInfo // Metadata for all older, sealed segments
	activeSize  uint64        // Current size in bytes of the active .seg file
	options     *Options
	logger      *zap.Logger

	closeCh chan struct{}
	wg      sync.WaitGroup
}

// Options configure the behavior of the WAL.
type Options struct {
	// MaxSegmentSize is the maximum threshold size (in bytes) a segment can reach
	// before the WAL forces a rotation, creating a new segment and sealing the current one.
	MaxSegmentSize uint64
	// SyncOnWrite determines if the operating system should be forced to flush
	// file buffers to physical disk synchronously after every Append operation (fsync).
	// Setting this to true guarantees durability at the cost of high latency and low throughput.
	// Setting this to false yields better performance, but risks data loss during power failure.
	SyncOnWrite bool
	// SyncInterval is the duration between background syncs if SyncOnWrite is false.
	// If 0, background sync is disabled.
	SyncInterval time.Duration
	// MaxGlobalSegment controls how many old segments are retained.
	// When a rotation occurs, if the number of old segments exceeds this value,
	// the oldest segments are physically deleted from disk to reclaim space.
	MaxGlobalSegment int8
	// Logger specifies a zap structured logger for WAL operations.
	// If nil, zap.L() (or zap.NewNop()) is used.
	Logger *zap.Logger
}

// DefaultOptions returns a set of sensible default configurations for the WAL.
func DefaultOptions() Options {
	logger, _ := zap.NewProduction()
	if logger == nil {
		logger = zap.NewNop()
	}
	return Options{
		MaxSegmentSize:   1024 * 1024 * 20,
		SyncOnWrite:      false,
		SyncInterval:     100 * time.Millisecond,
		MaxGlobalSegment: 100,
		Logger:           logger,
	}
}

func readSegmentHeader(path string) (uint64, uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	header := make([]byte, segmentHeaderSize)
	_, err = io.ReadFull(f, header)
	if err != nil {
		return 0, 0, err
	}

	baseSeq := binary.LittleEndian.Uint64(header[baseSeqOffset:maxSeqOffset])
	maxSeq := binary.LittleEndian.Uint64(header[maxSeqOffset:segmentHeaderSize])
	return baseSeq, maxSeq, nil
}

// createSegment creates a new .seg and .idx file in the specified circular slot
func createSegment(logger *zap.Logger, dir string, slotID int, baseSeq uint64) (*os.File, *os.File, segmentInfo, error) {
	filename := fmt.Sprintf("slot-%d", slotID)
	segPath := filepath.Join(dir, filename+".seg")
	idxPath := filepath.Join(dir, filename+".idx")

	logger.Debug("creating segment files",
		zap.Int("slot_id", slotID),
		zap.Uint64("base_seq", baseSeq),
		zap.String("seg_path", segPath),
		zap.String("idx_path", idxPath),
	)

	// Use O_TRUNC to ensure we wipe out any older generation data residing in this reused slot
	segFile, err := os.OpenFile(segPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		logger.Error("failed to open segment file for creation",
			zap.String("path", segPath),
			zap.Error(err),
		)
		return nil, nil, segmentInfo{}, err
	}

	idxFile, err := os.OpenFile(idxPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		segFile.Close()
		logger.Error("failed to open index file for creation",
			zap.String("path", idxPath),
			zap.Error(err),
		)
		return nil, nil, segmentInfo{}, err
	}

	header := make([]byte, segmentHeaderSize)
	binary.LittleEndian.PutUint64(header[baseSeqOffset:maxSeqOffset], baseSeq)
	binary.LittleEndian.PutUint64(header[maxSeqOffset:segmentHeaderSize], ^uint64(0))

	_, err = segFile.Write(header)
	if err != nil {
		segFile.Close()
		idxFile.Close()
		logger.Error("failed to write segment header",
			zap.String("path", segPath),
			zap.Error(err),
		)
		return nil, nil, segmentInfo{}, err
	}
	if err := segFile.Sync(); err != nil { // Ensure header is committed before we return
		logger.Error("failed to sync segment header",
			zap.String("path", segPath),
			zap.Error(err),
		)
	}

	info := segmentInfo{
		slotID:  slotID,
		path:    segPath,
		idxPath: idxPath,
		baseSeq: baseSeq,
		maxSeq:  ^uint64(0),
	}

	logger.Debug("segment created successfully",
		zap.Int("slot_id", slotID),
		zap.Uint64("base_seq", baseSeq),
	)

	return segFile, idxFile, info, nil
}

// Open initializes or recovers a Write-Ahead Log instance from the given directory path.
// It will parse all existing segment files in the directory to reconstruct the WAL state.
// If the directory does not exist or has no segments, it initializes a brand new WAL
// starting at sequence number 0.
//
// Recovery Process:
// During crash recovery or normal startup, Open looks at the old segments to find the
// max valid sequence, and initializes the "next sequence" to resume operations safely.
func Open(path string, opts Options) (*WAL, error) {
	logger := opts.Logger
	if logger == nil {
		logger = zap.L()
		if logger == nil {
			logger = zap.NewNop()
		}
	}
	logger = logger.With(zap.String("wal_path", path))

	wal := &WAL{path: path, options: &opts, logger: logger}
	err := os.MkdirAll(path, 0o755)
	if err != nil {
		logger.Error("failed to create WAL directory", zap.Error(err))
		return nil, err
	}

	logger.Info("opening WAL",
		zap.Bool("sync_on_write", opts.SyncOnWrite),
		zap.Duration("sync_interval", opts.SyncInterval),
		zap.Uint64("max_segment_size", opts.MaxSegmentSize),
		zap.Int8("max_global_segment", opts.MaxGlobalSegment),
	)

	pattern := filepath.Join(path, "slot-*.seg")
	matches, _ := filepath.Glob(pattern)

	var segs []segmentInfo
	for _, m := range matches {
		bSeq, mSeq, err := readSegmentHeader(m)
		if err != nil {
			logger.Warn("skipping corrupted or unreadable segment header",
				zap.String("path", m),
				zap.Error(err),
			)
			continue // ignore corrupted or empty slots
		}
		var slotID int
		baseName := filepath.Base(m)
		fmt.Sscanf(baseName, "slot-%d.seg", &slotID)

		idxPath := strings.TrimSuffix(m, ".seg") + ".idx"
		segs = append(segs, segmentInfo{slotID: slotID, path: m, idxPath: idxPath, baseSeq: bSeq, maxSeq: mSeq})
		logger.Debug("discovered segment",
			zap.Int("slot_id", slotID),
			zap.Uint64("base_seq", bSeq),
			zap.Uint64("max_seq", mSeq),
			zap.String("path", m),
		)
	}

	// Sort logically by baseSeq
	sort.Slice(segs, func(i, j int) bool {
		return segs[i].baseSeq < segs[j].baseSeq
	})

	if len(segs) == 0 {
		wal.nextSeq = 0
		segFile, idxFile, info, err := createSegment(logger, path, 0, wal.nextSeq)
		if err != nil {
			logger.Error("failed to create initial segment", zap.Error(err))
			return nil, err
		}
		wal.file = segFile
		wal.idxFile = idxFile
		wal.activeSeg = info
		wal.activeSize = segmentHeaderSize
		logger.Info("initialized new WAL",
			zap.Int("active_slot", 0),
			zap.Uint64("next_seq", wal.nextSeq),
		)
	} else {
		wal.oldSegments = segs[:len(segs)-1]
		wal.activeSeg = segs[len(segs)-1]

		f, err := os.OpenFile(wal.activeSeg.path, os.O_RDWR, 0o644)
		if err != nil {
			logger.Error("failed to open active segment file",
				zap.String("path", wal.activeSeg.path),
				zap.Error(err),
			)
			return nil, err
		}
		if _, err := f.Seek(0, io.SeekEnd); err != nil {
			logger.Error("failed to seek active segment file to end",
				zap.String("path", wal.activeSeg.path),
				zap.Error(err),
			)
			_ = f.Close()
			return nil, err
		}
		wal.file = f

		idxF, err := os.OpenFile(wal.activeSeg.idxPath, os.O_RDWR, 0o644)
		if err != nil {
			logger.Error("failed to open active index file",
				zap.String("path", wal.activeSeg.idxPath),
				zap.Error(err),
			)
			_ = f.Close()
			return nil, err
		}
		if _, err := idxF.Seek(0, io.SeekEnd); err != nil {
			logger.Error("failed to seek active index file to end",
				zap.String("path", wal.activeSeg.idxPath),
				zap.Error(err),
			)
			_ = f.Close()
			_ = idxF.Close()
			return nil, err
		}
		wal.idxFile = idxF

		stat, err := f.Stat()
		if err != nil {
			logger.Error("failed to stat active segment file",
				zap.String("path", wal.activeSeg.path),
				zap.Error(err),
			)
			return nil, err
		}
		wal.activeSize = uint64(stat.Size())

		idxStat, err := idxF.Stat()
		if err != nil {
			logger.Error("failed to stat active index file",
				zap.String("path", wal.activeSeg.idxPath),
				zap.Error(err),
			)
			return nil, err
		}

		// The number of 8-byte entries in the index file dictates our next sequence
		numEntries := idxStat.Size() / 8
		wal.nextSeq = wal.activeSeg.baseSeq + uint64(numEntries)

		logger.Info("recovered existing WAL state",
			zap.Int("total_segments", len(segs)),
			zap.Int("old_segments", len(wal.oldSegments)),
			zap.Int("active_slot", wal.activeSeg.slotID),
			zap.Uint64("active_base_seq", wal.activeSeg.baseSeq),
			zap.Uint64("next_seq", wal.nextSeq),
			zap.Uint64("active_size", wal.activeSize),
		)
	}

	wal.closeCh = make(chan struct{})
	if !opts.SyncOnWrite && opts.SyncInterval > 0 {
		wal.wg.Add(1)
		logger.Debug("starting background sync loop", zap.Duration("interval", opts.SyncInterval))
		go wal.syncLoop()
	}

	return wal, nil
}

func (w *WAL) syncLoop() {
	defer w.wg.Done()
	ticker := time.NewTicker(w.options.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := w.Sync(); err != nil {
				w.logger.Error("periodic background sync failed", zap.Error(err))
			}
		case <-w.closeCh:
			w.logger.Debug("stopping background sync loop")
			return
		}
	}
}

// Append writes a new record to the WAL and returns its globally unique sequence number.
//
// Theory:
// Append achieves high performance because it only does sequential writes to the active file.
// If the active segment grows beyond Options.MaxSegmentSize, it automatically handles
// "Segment Rotation". It seals the current file and opens a new one.
// The index file (.idx) is also updated concurrently so that this new record can be instantly
// found without scanning the segment file.
func (w *WAL) Append(data []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	seq := w.nextSeq
	encoded := record.Encode(seq, data)
	logSize := uint64(len(encoded))

	if w.activeSize+logSize > w.options.MaxSegmentSize {
		w.activeSeg.maxSeq = seq - 1
		w.logger.Info("rotating segment",
			zap.Int("sealed_slot", w.activeSeg.slotID),
			zap.Uint64("sealed_base_seq", w.activeSeg.baseSeq),
			zap.Uint64("sealed_max_seq", w.activeSeg.maxSeq),
			zap.Uint64("sealed_size", w.activeSize),
		)

		headerMax := make([]byte, 8)
		binary.LittleEndian.PutUint64(headerMax, w.activeSeg.maxSeq)
		if _, err := w.file.WriteAt(headerMax, maxSeqOffset); err != nil {
			w.logger.Error("failed to write max sequence in header on rotation",
				zap.Int("slot_id", w.activeSeg.slotID),
				zap.Error(err),
			)
		}
		_ = w.file.Sync()
		_ = w.file.Close()
		_ = w.idxFile.Sync()
		_ = w.idxFile.Close()

		w.oldSegments = append(w.oldSegments, w.activeSeg)

		// Determine the physical circular slot for the new segment
		nextSlot := (w.activeSeg.slotID + 1) % int(w.options.MaxGlobalSegment)

		// Garbage collect logically old segments if we exceed the MaxGlobalSegment threshold.
		// In a full ring buffer, this pops the oldest segment out of the logical view
		// since its physical slot is about to be overwritten by the new segment.
		if w.options.MaxGlobalSegment > 0 {
			for len(w.oldSegments) >= int(w.options.MaxGlobalSegment) {
				evicted := w.oldSegments[0]
				w.logger.Warn("evicting oldest segment due to max global segment limit",
					zap.Int("evicted_slot", evicted.slotID),
					zap.Uint64("evicted_base_seq", evicted.baseSeq),
					zap.Uint64("evicted_max_seq", evicted.maxSeq),
				)
				w.oldSegments = w.oldSegments[1:] // pop the oldest
			}
		}

		segFile, idxFile, info, err := createSegment(w.logger, w.path, nextSlot, seq)
		if err != nil {
			w.logger.Error("failed to create new segment during rotation",
				zap.Int("slot_id", nextSlot),
				zap.Error(err),
			)
			return 0, err
		}

		w.file = segFile
		w.idxFile = idxFile
		w.activeSeg = info
		w.activeSize = segmentHeaderSize
	}

	// 1. Write the offset to the index file (8 bytes)
	offsetBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(offsetBuf, w.activeSize)
	_, err := w.idxFile.Write(offsetBuf)
	if err != nil {
		w.logger.Error("failed to write index entry",
			zap.Uint64("seq", seq),
			zap.Uint64("offset", w.activeSize),
			zap.Error(err),
		)
		return 0, err
	}

	// 2. Write the actual data to the segment file
	_, err = w.file.Write(encoded)
	if err != nil {
		w.logger.Error("failed to write record data",
			zap.Uint64("seq", seq),
			zap.Error(err),
		)
		return 0, err
	}

	w.activeSize += logSize
	w.nextSeq++

	if w.options.SyncOnWrite {
		if err := w.idxFile.Sync(); err != nil {
			w.logger.Error("failed to sync index file on write", zap.Uint64("seq", seq), zap.Error(err))
		}
		if err := w.file.Sync(); err != nil {
			w.logger.Error("failed to sync segment file on write", zap.Uint64("seq", seq), zap.Error(err))
		}
	}

	w.logger.Debug("appended record",
		zap.Uint64("seq", seq),
		zap.Int("payload_bytes", len(data)),
		zap.Uint64("record_bytes", logSize),
		zap.Uint64("active_size", w.activeSize),
	)

	return seq, nil
}

// Read retrieves a record's data payload given its exact sequence number.
//
// Theory:
// Without an index, reading a specific record would require O(N) scanning of the file
// from the beginning. Instead, Read uses the index (.idx) file to achieve O(1) performance.
// It mathematically computes the byte position in the index file (seq * 8), reads the exact
// 8-byte offset, and then performs a single disk seek/read in the segment (.seg) file
// to fetch the desired record data.
func (w *WAL) Read(seq uint64) ([]byte, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	var targetSeg segmentInfo
	found := false

	for _, seg := range w.oldSegments {
		if seq >= seg.baseSeq && seq <= seg.maxSeq {
			targetSeg = seg
			found = true
			break
		}
	}

	if !found {
		if seq >= w.activeSeg.baseSeq && seq <= w.activeSeg.maxSeq {
			targetSeg = w.activeSeg
			found = true
		} else {
			w.logger.Debug("record sequence not found in any segment", zap.Uint64("seq", seq))
			return nil, ErrNotFound
		}
	}

	// 1. Read the offset from the .idx file
	// Each index entry is 8 bytes. The entry for 'seq' is at byte (seq - baseSeq) * 8
	idxOffset := int64((seq - targetSeg.baseSeq) * 8)

	idxF, err := os.Open(targetSeg.idxPath)
	if err != nil {
		w.logger.Error("failed to open index file for reading",
			zap.Uint64("seq", seq),
			zap.String("idx_path", targetSeg.idxPath),
			zap.Error(err),
		)
		return nil, err
	}
	defer idxF.Close()

	offsetBuf := make([]byte, 8)
	_, err = idxF.ReadAt(offsetBuf, idxOffset)
	if err != nil {
		if err == io.EOF {
			w.logger.Debug("index offset at EOF",
				zap.Uint64("seq", seq),
				zap.String("idx_path", targetSeg.idxPath),
			)
			return nil, ErrNotFound // Sequence is beyond the current index
		}
		w.logger.Error("failed to read offset from index file",
			zap.Uint64("seq", seq),
			zap.Error(err),
		)
		return nil, err
	}

	dataOffset := binary.LittleEndian.Uint64(offsetBuf)

	// 2. Read the record from the .seg file using the fast O(1) offset
	segF, err := os.Open(targetSeg.path)
	if err != nil {
		w.logger.Error("failed to open segment file for reading",
			zap.Uint64("seq", seq),
			zap.String("seg_path", targetSeg.path),
			zap.Error(err),
		)
		return nil, err
	}
	defer segF.Close()

	// Read header first
	header := make([]byte, record.HeaderSize)
	_, err = segF.ReadAt(header, int64(dataOffset))
	if err != nil {
		w.logger.Error("failed to read record header",
			zap.Uint64("seq", seq),
			zap.Uint64("offset", dataOffset),
			zap.Error(err),
		)
		return nil, err
	}

	recLen := binary.LittleEndian.Uint32(header[12:16])

	// Read full record
	recordBuf := make([]byte, uint32(record.HeaderSize)+recLen)
	_, err = segF.ReadAt(recordBuf, int64(dataOffset))
	if err != nil {
		w.logger.Error("failed to read record bytes",
			zap.Uint64("seq", seq),
			zap.Uint64("offset", dataOffset),
			zap.Error(err),
		)
		return nil, err
	}

	rec, err := record.Decode(recordBuf)
	if err != nil {
		w.logger.Error("failed to decode record",
			zap.Uint64("seq", seq),
			zap.Error(err),
		)
		return nil, err
	}

	w.logger.Debug("read record successfully",
		zap.Uint64("seq", seq),
		zap.Int("slot_id", targetSeg.slotID),
		zap.Uint32("data_len", rec.Length),
	)

	return rec.Data, nil
}

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.logger.Debug("syncing WAL to disk")

	if w.idxFile != nil {
		if err := w.idxFile.Sync(); err != nil {
			w.logger.Error("failed to sync active index file", zap.Error(err))
		}
	}
	if w.file != nil {
		if err := w.file.Sync(); err != nil {
			w.logger.Error("failed to sync active segment file", zap.Error(err))
			return err
		}
	}
	return nil
}

func (w *WAL) Close() error {
	w.logger.Info("closing WAL")

	if w.closeCh != nil {
		close(w.closeCh)
		w.wg.Wait()
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	var firstErr error
	if w.idxFile != nil {
		_ = w.idxFile.Sync()
		if err := w.idxFile.Close(); err != nil {
			w.logger.Error("failed to close active index file", zap.Error(err))
			firstErr = err
		}
	}
	if w.file != nil {
		_ = w.file.Sync()
		if err := w.file.Close(); err != nil {
			w.logger.Error("failed to close active segment file", zap.Error(err))
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	if firstErr != nil {
		return firstErr
	}
	w.logger.Info("WAL closed successfully")
	_ = w.logger.Sync()
	return nil
}
