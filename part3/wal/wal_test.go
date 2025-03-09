package wal

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWAL_BasicOperations(t *testing.T) {
	// Create a temporary directory for testing
	dir, err := os.MkdirTemp("", "wal-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// Create a new WAL
	w, err := OpenWAL(dir, false, 1024*1024) // 1MB file size
	require.NoError(t, err)
	defer w.Close()

	// Write a few entries
	testData := [][]byte{
		[]byte("entry 1"),
		[]byte("entry 2"),
		[]byte("entry 3"),
	}

	for _, data := range testData {
		err := w.WriteEntry(data)
		require.NoError(t, err)
	}

	// Sync to ensure all entries are written
	err = w.Sync()
	require.NoError(t, err)

	// Read all entries back and verify
	entries, err := w.ReadAll()
	require.NoError(t, err)
	require.Equal(t, len(testData), len(entries))

	for i, entry := range entries {
		require.Equal(t, testData[i], entry.Data)
		require.Equal(t, uint64(i+1), entry.SequenceNumber)
	}
}

func TestWAL_Persistence(t *testing.T) {
	// Create a temporary directory for testing
	dir, err := os.MkdirTemp("", "wal-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// Test data
	testData := [][]byte{
		[]byte("persistence test 1"),
		[]byte("persistence test 2"),
		[]byte("persistence test 3"),
	}

	// Write data to WAL
	{
		w, err := OpenWAL(dir, true, 1024*1024) // 1MB file size with fsync
		require.NoError(t, err)

		for _, data := range testData {
			err := w.WriteEntry(data)
			require.NoError(t, err)
		}

		err = w.Close()
		require.NoError(t, err)
	}

	// Reopen WAL and verify data
	{
		w, err := OpenWAL(dir, false, 1024*1024)
		require.NoError(t, err)
		defer w.Close()

		entries, err := w.ReadAll()
		require.NoError(t, err)
		require.Equal(t, len(testData), len(entries))

		for i, entry := range entries {
			require.Equal(t, testData[i], entry.Data)
			require.Equal(t, uint64(i+1), entry.SequenceNumber)
		}
	}
}

func TestWAL_Rotation(t *testing.T) {
	// Create a temporary directory for testing
	dir, err := os.MkdirTemp("", "wal-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// Create a WAL with small max file size to trigger rotation
	// 100 bytes should be enough for only a few entries
	w, err := OpenWAL(dir, false, 100)
	require.NoError(t, err)
	defer w.Close()

	// Write several entries to trigger rotation
	for i := 0; i < 10; i++ {
		data := []byte(fmt.Sprintf("rotation test entry %d", i))
		err := w.WriteEntry(data)
		require.NoError(t, err)
	}

	// Sync to ensure all writes are complete
	err = w.Sync()
	require.NoError(t, err)

	// Check that multiple segment files were created
	files, err := filepath.Glob(filepath.Join(dir, "segment-*"))
	require.NoError(t, err)
	require.True(t, len(files) > 1, "Expected multiple segment files after rotation")
}

func TestWAL_LargeEntries(t *testing.T) {
	// Create a temporary directory for testing
	dir, err := os.MkdirTemp("", "wal-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// Create a WAL
	w, err := OpenWAL(dir, false, 5*1024*1024) // 5MB file size
	require.NoError(t, err)
	defer w.Close()

	// Generate a large entry (1MB)
	largeData := make([]byte, 1024*1024)
	_, err = rand.Read(largeData)
	require.NoError(t, err)

	// Write the large entry
	err = w.WriteEntry(largeData)
	require.NoError(t, err)

	// Sync to ensure it's written
	err = w.Sync()
	require.NoError(t, err)

	// Read it back and verify
	entries, err := w.ReadAll()
	require.NoError(t, err)
	require.Equal(t, 1, len(entries))
	require.True(t, bytes.Equal(largeData, entries[0].Data))
}

func TestWAL_ConcurrentWrites(t *testing.T) {
	// Create a temporary directory for testing
	dir, err := os.MkdirTemp("", "wal-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// Create a WAL
	w, err := OpenWAL(dir, true, 1024*1024)
	require.NoError(t, err)
	defer w.Close()

	// Number of concurrent writers
	numWriters := 10
	// Entries per writer
	entriesPerWriter := 100

	// Channel to collect errors
	errCh := make(chan error, numWriters)

	// Start concurrent writers
	for i := 0; i < numWriters; i++ {
		go func(writerID int) {
			var writeErr error
			for j := 0; j < entriesPerWriter; j++ {
				data := []byte(fmt.Sprintf("writer-%d-entry-%d", writerID, j))
				if err := w.WriteEntry(data); err != nil {
					writeErr = err
					break
				}
			}
			errCh <- writeErr
		}(i)
	}

	// Collect results
	for i := 0; i < numWriters; i++ {
		err := <-errCh
		require.NoError(t, err)
	}

	// Sync to ensure all writes are complete
	err = w.Sync()
	require.NoError(t, err)

	// Read all entries and verify count
	entries, err := w.ReadAll()
	require.NoError(t, err)
	require.Equal(t, numWriters*entriesPerWriter, len(entries))

	// Verify sequence numbers are sequential
	for i, entry := range entries {
		require.Equal(t, uint64(i+1), entry.SequenceNumber)
	}
}

func TestWAL_Recovery(t *testing.T) {
	// Create a temporary directory for testing
	dir, err := os.MkdirTemp("", "wal-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// Create test data
	testData := [][]byte{
		[]byte("recovery test 1"),
		[]byte("recovery test 2"),
		[]byte("recovery test 3"),
	}

	// Write entries to WAL
	w, err := OpenWAL(dir, true, 1024*1024)
	require.NoError(t, err)

	for _, data := range testData {
		err := w.WriteEntry(data)
		require.NoError(t, err)
	}

	// Sync and close properly
	err = w.Sync()
	require.NoError(t, err)
	err = w.Close()
	require.NoError(t, err)

	// Find the segment file
	files, err := filepath.Glob(filepath.Join(dir, "segment-*"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	segmentPath := files[0]

	// Add a properly formatted but corrupted entry
	f, err := os.OpenFile(segmentPath, os.O_APPEND|os.O_WRONLY, 0644)
	require.NoError(t, err)
	defer f.Close()

	// Prepare a corrupted entry - with the wrong CRC
	corruptedData := []byte("corrupted entry")
	corruptedSeqNum := uint64(len(testData) + 1)

	// Write sequence number
	err = binary.Write(f, binary.LittleEndian, corruptedSeqNum)
	require.NoError(t, err)

	// Write data length
	err = binary.Write(f, binary.LittleEndian, uint32(len(corruptedData)))
	require.NoError(t, err)

	// Calculate correct CRC according to the implementation
	// In the WAL implementation: crc32.ChecksumIEEE(append(data, byte(wal.lastSeqNo)))
	correctCRC := crc32.ChecksumIEEE(append(corruptedData, byte(corruptedSeqNum)))

	// Use an intentionally wrong CRC
	wrongCRC := correctCRC + 1

	// Write the wrong CRC
	err = binary.Write(f, binary.LittleEndian, wrongCRC)
	require.NoError(t, err)

	// Write the data
	_, err = f.Write(corruptedData)
	require.NoError(t, err)

	// Close the file
	f.Close()

	// Get the original file size before opening the WAL again
	originalFileInfo, err := os.Stat(segmentPath)
	require.NoError(t, err)
	originalSize := originalFileInfo.Size()

	// Back up the original file to verify later
	backupPath := segmentPath + ".backup"
	err = copyFile(segmentPath, backupPath)
	require.NoError(t, err)

	// Reopen the WAL to trigger recovery
	w, err = OpenWAL(dir, false, 1024*1024)
	require.NoError(t, err)
	defer w.Close()

	// Verify we can read the entries
	entries, err := w.ReadAll()
	require.NoError(t, err)

	// Should only contain the original valid entries
	require.Equal(t, len(testData), len(entries))

	// Check content of entries
	for i, entry := range entries {
		require.Equal(t, testData[i], entry.Data)
		require.Equal(t, uint64(i+1), entry.SequenceNumber)
	}

	// Get the current segment file
	currentSegPath := w.CurrentSegmentPath()

	// Verify the file size has changed - should be smaller without corruption
	currentFileInfo, err := os.Stat(currentSegPath)
	require.NoError(t, err)

	// Since we removed corrupted data, new file should be smaller than original
	require.Less(t, currentFileInfo.Size(), originalSize,
		"File should be truncated to remove corruption")
}

// Helper function to copy a file
func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
}
