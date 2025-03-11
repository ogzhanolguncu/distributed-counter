package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

const (
	syncInterval  = 500 * time.Millisecond
	segmentPrefix = "segment-"
)

// Entry represents a single log entry
type Entry struct {
	SequenceNumber uint64
	Data           []byte
	CRC            uint32
}

// WAL represents a Write-Ahead Log
type WAL struct {
	directory   string
	crrSeg      *os.File
	mu          sync.Mutex
	lastSeqNo   uint64
	bufWriter   *bufio.Writer
	shouldFsync bool
	maxFileSize int64
	crrSegIdx   int

	done chan struct{}
}

// OpenWAL creates a new WAL or opens an existing one
// During startup, it reads and validates all existing entries to ensure data integrity
func OpenWAL(directory string, enableFsync bool, maxFileSize int64) (*WAL, error) {
	// Create directory if not exists
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err
	}

	// Get all segment files
	files, err := filepath.Glob(filepath.Join(directory, segmentPrefix+"*"))
	if err != nil {
		return nil, err
	}

	crrSegmentIdx := 0
	filePath := ""

	// Find the highest segment or fallback to default "segment0"
	if len(files) > 0 {
		highestIdx := -1
		for _, file := range files {
			base := filepath.Base(file)
			idxStr := base[len(segmentPrefix):]
			idx, err := strconv.Atoi(idxStr)
			if err == nil && idx > highestIdx {
				highestIdx = idx
			}
		}

		if highestIdx >= 0 {
			crrSegmentIdx = highestIdx
		}
		filePath = filepath.Join(directory, fmt.Sprintf("%s%d", segmentPrefix, crrSegmentIdx))
	} else {
		filePath = filepath.Join(directory, segmentPrefix+"0")
	}

	// Try to recover from existing segments
	if _, err := os.Stat(filePath); err == nil {
		validEntries, lastValidSeq, err := performRecovery(filePath)
		if err != nil {
			if len(validEntries) > 0 {
				if err := rewriteWithValidEntries(filePath, validEntries); err != nil {
					return nil, fmt.Errorf("failed to repair WAL during recovery: %w", err)
				}
			} else {
				return nil, fmt.Errorf("recover failed: %v", err)
			}
		}

		// If we find a corruption, get rid of it then rewrite with valid entries
		if len(validEntries) > 0 && validEntries[len(validEntries)-1].SequenceNumber != lastValidSeq {
			if err := rewriteWithValidEntries(filePath, validEntries); err != nil {
				return nil, fmt.Errorf("failed to report WAL during recovery: %w", err)
			}
		}
	}

	// Open file for appending only
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	// Set offset to the end the file
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}

	wal := &WAL{
		directory:   directory,
		crrSeg:      file,
		bufWriter:   bufio.NewWriter(file),
		shouldFsync: enableFsync,
		maxFileSize: maxFileSize,
		crrSegIdx:   crrSegmentIdx,
		done:        make(chan struct{}),
	}

	// Get the last sequence number
	if lastSeqNo, err := wal.getLastSeqNo(); err == nil {
		wal.lastSeqNo = lastSeqNo
	}

	if enableFsync {
		go wal.periodicSync()
	}

	return wal, nil
}

func (wal *WAL) WriteEntry(data []byte) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	wal.lastSeqNo++
	entry := &Entry{
		SequenceNumber: wal.lastSeqNo,
		Data:           data,
		CRC:            crc32.ChecksumIEEE(append(data, byte(wal.lastSeqNo))),
	}

	return wal.writeEntryToBuf(entry)
}

func (wal *WAL) writeEntryToBuf(entry *Entry) error {
	// Calculate data length
	dataLen := len(entry.Data)

	// Write the entry header
	if err := binary.Write(wal.bufWriter, binary.LittleEndian, entry.SequenceNumber); err != nil {
		return err
	}
	if err := binary.Write(wal.bufWriter, binary.LittleEndian, uint32(dataLen)); err != nil {
		return err
	}
	if err := binary.Write(wal.bufWriter, binary.LittleEndian, entry.CRC); err != nil {
		return err
	}
	// Actual entry
	if _, err := wal.bufWriter.Write(entry.Data); err != nil {
		return err
	}

	// Check if file is too big, if so rotate decide to rotate/create new file
	if info, err := wal.crrSeg.Stat(); err == nil {
		// Actual file size + buffered data
		if info.Size()+int64(wal.bufWriter.Buffered()) >= wal.maxFileSize {
			wal.Sync() // Commit to buffer then force fsync
			wal.rotateLog()
		}
	}
	return nil
}

func (wal *WAL) rotateLog() error {
	// Close current segment before rotation
	wal.Sync()
	wal.crrSeg.Close()

	// Increment due to new segment creation
	wal.crrSegIdx++

	// New segment file name
	newPath := filepath.Join(wal.directory, fmt.Sprintf("%s%d", segmentPrefix, wal.crrSegIdx))
	// Open file for appending only
	file, err := os.OpenFile(newPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	wal.crrSeg = file
	wal.bufWriter = bufio.NewWriter(file)
	return nil
}

func (wal *WAL) Sync() error {
	if err := wal.bufWriter.Flush(); err != nil {
		return err
	}

	if wal.shouldFsync {
		wal.crrSeg.Sync()
	}

	return nil
}

// Periodicly fSync
func (wal *WAL) periodicSync() {
	ticker := time.NewTicker(syncInterval)

	for {
		select {
		case <-ticker.C:
			wal.mu.Lock()
			wal.Sync()
			wal.mu.Unlock()
		case <-wal.done:
			ticker.Stop()
		}
	}
}

func (wal *WAL) Close() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	select {
	case <-wal.done:
		return nil
	default:
	}

	if err := wal.Sync(); err != nil {
		return err
	}

	close(wal.done)
	return wal.crrSeg.Close()
}

func (wal *WAL) ReadAll() ([]*Entry, error) {
	file, err := os.OpenFile(wal.crrSeg.Name(), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return readAllEntries(file)
}

func readAllEntries(file *os.File) ([]*Entry, error) {
	var entries []*Entry

	for {
		entry, err := readEntry(file)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func readEntry(file *os.File) (*Entry, error) {
	var seqNum uint64
	if err := binary.Read(file, binary.LittleEndian, &seqNum); err != nil {
		return nil, err
	}

	var dataLen uint32
	if err := binary.Read(file, binary.LittleEndian, &dataLen); err != nil {
		return nil, err
	}

	var crc uint32
	if err := binary.Read(file, binary.LittleEndian, &crc); err != nil {
		return nil, err
	}

	data := make([]byte, dataLen)
	if _, err := io.ReadFull(file, data); err != nil {
		return nil, err
	}

	entry := &Entry{
		SequenceNumber: seqNum,
		Data:           data,
		CRC:            crc,
	}

	return entry, nil
}

func (wal *WAL) getLastSeqNo() (uint64, error) {
	entries, err := wal.ReadAll()
	if err != nil {
		return 0, err
	}

	if len(entries) > 0 {
		return entries[len(entries)-1].SequenceNumber, nil
	}
	return 0, nil
}

func performRecovery(filePath string) ([]*Entry, uint64, error) {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, 0, err
	}
	defer file.Close()

	var validEntries []*Entry
	var lastValidSeq uint64 = 0

	for {
		entry, err := readEntry(file)
		if err != nil {
			if err == io.EOF {
				break
			}
			return validEntries, lastValidSeq, fmt.Errorf("corrupted entry: %v", err)
		}

		// Check CRC
		expectedCRC := crc32.ChecksumIEEE(append(entry.Data, byte(entry.SequenceNumber)))
		if entry.CRC != expectedCRC {
			// CRC mistmatch found
			return validEntries, lastValidSeq, fmt.Errorf("entry CRC mismatch: expected %d, got %d", expectedCRC, entry.CRC)
		}

		// Verify sequence numbers are monotonically increasing
		if entry.SequenceNumber <= lastValidSeq && lastValidSeq > 0 {
			return validEntries, lastValidSeq, fmt.Errorf("non-monotonic sequence number: %d after %d",
				entry.SequenceNumber, lastValidSeq)
		}

		// Entry is valid, add it to our list
		validEntries = append(validEntries, entry)
		lastValidSeq = entry.SequenceNumber
	}

	return validEntries, lastValidSeq, nil
}

// rewriteWithValidEntries creates a new WAL file with only valid entries
func rewriteWithValidEntries(filePath string, validEntries []*Entry) error {
	// Create a temporary file
	tempPath := filePath + ".tmp"
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	writer := bufio.NewWriter(file)

	// Write all valid entries to the temporary file
	for _, entry := range validEntries {
		// Write sequence number
		if err := binary.Write(writer, binary.LittleEndian, entry.SequenceNumber); err != nil {
			file.Close()
			return err
		}

		// Write data length
		dataLen := uint32(len(entry.Data))
		if err := binary.Write(writer, binary.LittleEndian, dataLen); err != nil {
			file.Close()
			return err
		}

		// Write CRC
		if err := binary.Write(writer, binary.LittleEndian, entry.CRC); err != nil {
			file.Close()
			return err
		}

		// Write data
		if _, err := writer.Write(entry.Data); err != nil {
			file.Close()
			return err
		}
	}

	// Flush buffer
	if err := writer.Flush(); err != nil {
		file.Close()
		return err
	}

	// Ensure data is on disk
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}

	// Close file before rename
	if err := file.Close(); err != nil {
		return err
	}

	// Atomically replace the original file with our repaired version
	return os.Rename(tempPath, filePath)
}

func (wal *WAL) CurrentSegmentPath() string {
	return wal.crrSeg.Name()
}
