package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ogzhanolguncu/distributed-counter/part4/crdt"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	syncInterval  = 500 * time.Millisecond
	segmentPrefix = "segment-"

	// Debug mode controlled by environment variable
	debugEnvVar = "WAL_DEBUG"
)

// Record types for different kinds of WAL records
const (
	TypeCounterIncrement uint8 = iota + 1
	TypeCounterDecrement
	TypeCounterState // Full snapshot of counter state
	TypeMetadata     // Configuration/metadata records
)

// Entry represents a single log entry
type Entry struct {
	SequenceNumber uint64
	Type           uint8  // The type of record
	NodeID         string // The node that created this entry
	Data           []byte // Serialized data for the entry
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
	nodeID      string // ID of the owning node

	done chan struct{}
}

// Helper function to check if debug is enabled
func isDebugEnabled() bool {
	debug := os.Getenv(debugEnvVar)
	return debug == "1" || debug == "true" || debug == "yes"
}

// Debug print function
func debugPrint(format string, args ...interface{}) {
	if isDebugEnabled() {
		fmt.Printf("[WAL_DEBUG] "+format+"\n", args...)
	}
}

// OpenWAL creates a new WAL or opens an existing one
func OpenWAL(directory string, nodeID string, enableFsync bool, maxFileSize int64) (*WAL, error) {
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
				return nil, fmt.Errorf("failed to repair WAL during recovery: %w", err)
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
		nodeID:      nodeID,
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

// WriteCounterIncrement logs an increment operation for a specific node
func (wal *WAL) WriteCounterIncrement(nodeID string) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	wal.lastSeqNo++
	data, err := msgpack.Marshal(map[string]interface{}{
		"nodeID": nodeID,
	})
	if err != nil {
		return err
	}

	entry := &Entry{
		SequenceNumber: wal.lastSeqNo,
		Type:           TypeCounterIncrement,
		NodeID:         wal.nodeID,
		Data:           data,
		CRC:            crc32.ChecksumIEEE(append(data, byte(wal.lastSeqNo))),
	}

	return wal.writeEntryToBuf(entry)
}

// WriteCounterDecrement logs a decrement operation for a specific node
func (wal *WAL) WriteCounterDecrement(nodeID string) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	wal.lastSeqNo++
	data, err := msgpack.Marshal(map[string]interface{}{
		"nodeID": nodeID,
	})
	if err != nil {
		return err
	}

	entry := &Entry{
		SequenceNumber: wal.lastSeqNo,
		Type:           TypeCounterDecrement,
		NodeID:         wal.nodeID,
		Data:           data,
		CRC:            crc32.ChecksumIEEE(append(data, byte(wal.lastSeqNo))),
	}

	return wal.writeEntryToBuf(entry)
}

// WriteCounterState logs a complete snapshot of the counter state
func (wal *WAL) WriteCounterState(counter *crdt.PNCounter) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	wal.lastSeqNo++

	// Extract both increment and decrement maps
	increments, decrements := counter.Counters()

	// Ensure we're properly serializing the maps - convert to native types if needed
	incMap := make(map[string]uint64)
	for k, v := range increments {
		incMap[k] = v
	}

	decMap := make(map[string]uint64)
	for k, v := range decrements {
		decMap[k] = v
	}

	data, err := msgpack.Marshal(map[string]interface{}{
		"increments": incMap,
		"decrements": decMap,
	})
	if err != nil {
		return err
	}

	entry := &Entry{
		SequenceNumber: wal.lastSeqNo,
		Type:           TypeCounterState,
		NodeID:         wal.nodeID,
		Data:           data,
		CRC:            crc32.ChecksumIEEE(append(data, byte(wal.lastSeqNo))),
	}

	return wal.writeEntryToBuf(entry)
}

// WriteMetadata logs metadata information
func (wal *WAL) WriteMetadata(metadata map[string]interface{}) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	wal.lastSeqNo++

	data, err := msgpack.Marshal(metadata)
	if err != nil {
		return err
	}

	entry := &Entry{
		SequenceNumber: wal.lastSeqNo,
		Type:           TypeMetadata,
		NodeID:         wal.nodeID,
		Data:           data,
		CRC:            crc32.ChecksumIEEE(append(data, byte(wal.lastSeqNo))),
	}

	return wal.writeEntryToBuf(entry)
}

func (wal *WAL) writeEntryToBuf(entry *Entry) error {
	// Recalculate CRC to ensure consistency
	entry.CRC = crc32.ChecksumIEEE(append(entry.Data, byte(entry.SequenceNumber)))

	// Write the entry header
	if err := binary.Write(wal.bufWriter, binary.LittleEndian, entry.SequenceNumber); err != nil {
		return err
	}

	// Write entry type
	if err := binary.Write(wal.bufWriter, binary.LittleEndian, entry.Type); err != nil {
		return err
	}

	// Write node ID length then node ID
	nodeIDBytes := []byte(entry.NodeID)
	nodeIDLen := uint16(len(nodeIDBytes))
	if err := binary.Write(wal.bufWriter, binary.LittleEndian, nodeIDLen); err != nil {
		return err
	}
	if _, err := wal.bufWriter.Write(nodeIDBytes); err != nil {
		return err
	}

	// Write data length
	dataLen := uint32(len(entry.Data))
	if err := binary.Write(wal.bufWriter, binary.LittleEndian, dataLen); err != nil {
		return err
	}

	// Write CRC
	if err := binary.Write(wal.bufWriter, binary.LittleEndian, entry.CRC); err != nil {
		return err
	}

	// Write the actual data
	if _, err := wal.bufWriter.Write(entry.Data); err != nil {
		return err
	}

	// Check if file is too big, if so rotate/create new file
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

// Periodically fsync
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
			return
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

	var entryType uint8
	if err := binary.Read(file, binary.LittleEndian, &entryType); err != nil {
		return nil, err
	}

	var nodeIDLen uint16
	if err := binary.Read(file, binary.LittleEndian, &nodeIDLen); err != nil {
		return nil, err
	}

	nodeIDBytes := make([]byte, nodeIDLen)
	if _, err := io.ReadFull(file, nodeIDBytes); err != nil {
		return nil, err
	}
	nodeID := string(nodeIDBytes)

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
		Type:           entryType,
		NodeID:         nodeID,
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
			// CRC mismatch found
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

		// Write entry type
		if err := binary.Write(writer, binary.LittleEndian, entry.Type); err != nil {
			file.Close()
			return err
		}

		// Write node ID length and bytes
		nodeIDBytes := []byte(entry.NodeID)
		nodeIDLen := uint16(len(nodeIDBytes))
		if err := binary.Write(writer, binary.LittleEndian, nodeIDLen); err != nil {
			file.Close()
			return err
		}
		if _, err := writer.Write(nodeIDBytes); err != nil {
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

// RecoverCounter recreates a PNCounter from the WAL entries
func RecoverCounter(walPath string, nodeID string) (*crdt.PNCounter, error) {
	// Open the WAL
	counter := crdt.New(nodeID)

	// Get all segment files, sorted by name (which includes sequence numbers)
	files, err := filepath.Glob(filepath.Join(walPath, segmentPrefix+"*"))
	if err != nil {
		return counter, err
	}

	if len(files) == 0 {
		// No existing logs, return new counter
		return counter, nil
	}

	debugPrint("RecoverCounter found %d segment files", len(files))

	// Sort files to process them in the correct order (oldest first)
	sort.Slice(files, func(i, j int) bool {
		iIdx := extractSegmentIndex(files[i])
		jIdx := extractSegmentIndex(files[j])
		return iIdx < jIdx // Ascending order
	})

	// Track last counter state segment for better recovery
	lastStateSeqNum := uint64(0)
	var lastFullCounter *crdt.PNCounter

	// First pass: Find the newest full counter state
	for _, filePath := range files {
		file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
		if err != nil {
			debugPrint("Warning: Could not open segment %s: %v", filePath, err)
			continue
		}

		entries, err := readAllEntries(file)
		file.Close()
		if err != nil {
			debugPrint("Warning: Error reading entries from %s: %v", filePath, err)
			continue
		}

		// Look for the newest counter state in this segment
		for _, entry := range entries {
			if entry.Type == TypeCounterState && entry.SequenceNumber > lastStateSeqNum {
				var state map[string]interface{}
				if err := msgpack.Unmarshal(entry.Data, &state); err != nil {
					continue
				}

				// Extract counter state
				increments, ok1 := state["increments"].(map[string]interface{})
				decrements, ok2 := state["decrements"].(map[string]interface{})

				if !ok1 || !ok2 {
					continue
				}

				// Convert map[string]interface{} to map[string]uint64
				incMap := make(map[string]uint64)
				for k, v := range increments {
					if val, ok := v.(uint64); ok {
						incMap[k] = val
					} else if val, ok := v.(int64); ok {
						incMap[k] = uint64(val)
					} else if val, ok := v.(float64); ok {
						incMap[k] = uint64(val)
					}
				}

				decMap := make(map[string]uint64)
				for k, v := range decrements {
					if val, ok := v.(uint64); ok {
						decMap[k] = val
					} else if val, ok := v.(int64); ok {
						decMap[k] = uint64(val)
					} else if val, ok := v.(float64); ok {
						decMap[k] = uint64(val)
					}
				}

				// Create a new counter with this state
				tempCounter := crdt.New(nodeID)
				tempCounter.MergeIncrements(incMap)
				tempCounter.MergeDecrements(decMap)

				lastFullCounter = tempCounter
				lastStateSeqNum = entry.SequenceNumber
				debugPrint("Found counter state snapshot at seq %d in %s", entry.SequenceNumber, filePath)
			}
		}
	}

	// If we found a full state, start with that
	if lastFullCounter != nil {
		counter = lastFullCounter
		debugPrint("Using counter from latest state snapshot, value: %d", counter.Value())
	}

	// Second pass: Apply all operations after the last full state
	for _, filePath := range files {
		file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
		if err != nil {
			continue
		}

		entries, err := readAllEntries(file)
		file.Close()
		if err != nil {
			continue
		}

		// Sort entries by sequence number to ensure correct order
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].SequenceNumber < entries[j].SequenceNumber
		})

		// Process entries with sequence numbers greater than our last state
		for _, entry := range entries {
			if entry.SequenceNumber <= lastStateSeqNum {
				continue // Skip entries covered by our last full state
			}

			switch entry.Type {
			case TypeCounterIncrement:
				var data map[string]interface{}
				if err := msgpack.Unmarshal(entry.Data, &data); err != nil {
					continue
				}

				if targetNodeID, ok := data["nodeID"].(string); ok {
					counter.Increment(targetNodeID)
					debugPrint("Applied increment op for %s at seq %d", targetNodeID, entry.SequenceNumber)
				}

			case TypeCounterDecrement:
				var data map[string]interface{}
				if err := msgpack.Unmarshal(entry.Data, &data); err != nil {
					continue
				}

				if targetNodeID, ok := data["nodeID"].(string); ok {
					counter.Decrement(targetNodeID)
					debugPrint("Applied decrement op for %s at seq %d", targetNodeID, entry.SequenceNumber)
				}
			}
		}
	}

	debugPrint("Final recovered counter value: %d", counter.Value())
	return counter, nil
}

// CleanupConfig defines parameters for WAL segment cleanup
type CleanupConfig struct {
	// Maximum number of segments to keep
	MaxSegments int

	// Minimum number of segments to always keep, regardless of other settings
	MinSegments int

	// Maximum age of segments to keep
	MaxAge time.Duration
}

// DefaultCleanupConfig returns sensible defaults for cleanup
func DefaultCleanupConfig() CleanupConfig {
	return CleanupConfig{
		MaxSegments: 50,                 // Keep at most 50 segments
		MinSegments: 5,                  // Always keep at least 5 segments
		MaxAge:      7 * 24 * time.Hour, // Keep segments up to 7 days old
	}
}

// CleanupSegments removes old WAL segments based on the provided configuration
func (wal *WAL) CleanupSegments(config CleanupConfig) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// Get the current segment path to ensure we don't delete the active segment
	currentSegment := wal.CurrentSegmentPath()

	// Find all segment files
	files, err := filepath.Glob(filepath.Join(wal.directory, segmentPrefix+"*"))
	if err != nil {
		return fmt.Errorf("failed to list segment files: %w", err)
	}

	// If we have fewer than MinSegments, don't delete anything
	if len(files) <= config.MinSegments {
		return nil
	}

	// Sort files by segment number to ensure predictable cleanup
	// Higher numbers are newer segments
	sort.Slice(files, func(i, j int) bool {
		iIdx := extractSegmentIndex(files[i])
		jIdx := extractSegmentIndex(files[j])
		return iIdx < jIdx // Ascending order - oldest first
	})

	debugPrint("Segment sorting check: first=%s, last=%s",
		filepath.Base(files[0]), filepath.Base(files[len(files)-1]))

	segmentsToKeep := make(map[string]bool)

	// Always keep the current segment
	segmentsToKeep[currentSegment] = true
	debugPrint("Keeping current segment: %s", currentSegment)

	// Find segments with a complete counter state - we need at least one
	var mostRecentStateSegment string
	var mostRecentStateSeqNum uint64

	// Look through segments for counter state, newest first
	for i := len(files) - 1; i >= 0; i-- {
		file := files[i]
		if file == currentSegment {
			continue // Skip current segment, we're already keeping it
		}

		f, err := os.OpenFile(file, os.O_RDONLY, 0644)
		if err != nil {
			continue
		}

		entries, err := readAllEntries(f)
		f.Close()
		if err != nil {
			continue
		}

		// Look for counter state entries
		for _, entry := range entries {
			if entry.Type == TypeCounterState && entry.SequenceNumber > mostRecentStateSeqNum {
				mostRecentStateSegment = file
				mostRecentStateSeqNum = entry.SequenceNumber
				break
			}
		}

		// If we found one, we can stop looking
		if mostRecentStateSegment != "" {
			break
		}
	}

	// Keep the segment with the most recent state snapshot
	if mostRecentStateSegment != "" && mostRecentStateSegment != currentSegment {
		segmentsToKeep[mostRecentStateSegment] = true
		debugPrint("Keeping segment with most recent state: %s", mostRecentStateSegment)
	}

	// Keep the newest segments up to MaxSegments total
	remainingSlots := config.MaxSegments - len(segmentsToKeep)
	if remainingSlots > 0 {
		// Start from newest (end of the sorted array) and work backwards
		for i := len(files) - 1; i >= 0 && remainingSlots > 0; i-- {
			if !segmentsToKeep[files[i]] {
				segmentsToKeep[files[i]] = true
				remainingSlots--
				debugPrint("Keeping newest segment: %s", files[i])
			}
		}
	}

	// CRITICAL: Make absolutely sure we have at least MinSegments
	if len(segmentsToKeep) < config.MinSegments {
		// Add the newest segments that we're not already keeping
		for i := len(files) - 1; i >= 0 && len(segmentsToKeep) < config.MinSegments; i-- {
			if !segmentsToKeep[files[i]] {
				segmentsToKeep[files[i]] = true
				debugPrint("Keeping extra segment to meet min: %s", files[i])
			}
		}
	}

	// Print summary of what we're doing
	debugPrint("Total segments: %d, keeping: %d, deleting: %d",
		len(files), len(segmentsToKeep), len(files)-len(segmentsToKeep))

	// Actually delete segments that don't need to be kept
	deletedCount := 0
	for _, file := range files {
		if !segmentsToKeep[file] {
			debugPrint("Deleting segment: %s", file)
			if err := os.Remove(file); err != nil {
				return fmt.Errorf("failed to delete segment %s: %w", file, err)
			}
			deletedCount++
		}
	}

	debugPrint("Deleted %d segments", deletedCount)
	return nil
}

func extractSegmentIndex(path string) int {
	filename := filepath.Base(path)
	if !strings.HasPrefix(filename, segmentPrefix) {
		return -1
	}

	indexStr := filename[len(segmentPrefix):]
	index, err := strconv.Atoi(indexStr)
	if err != nil {
		// Handle error more gracefully - log it but try to continue
		debugPrint("Warning: Could not parse segment index from %s: %v", filename, err)
		return -1
	}

	debugPrint("Extracted index %d from %s", index, filename)
	return index
}

// StartPeriodicCleanup starts a goroutine that periodically cleans up old segments
func (wal *WAL) StartPeriodicCleanup(interval time.Duration, config CleanupConfig) {
	go func() {
		// Run cleanup immediately once before starting the ticker
		if err := wal.CleanupSegments(config); err != nil {
			debugPrint("Error during initial WAL segments cleanup: %v", err)
		}

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := wal.CleanupSegments(config); err != nil {
					// Log error but continue
					debugPrint("Error cleaning up WAL segments: %v", err)
				}
			case <-wal.done:
				return
			}
		}
	}()
}
