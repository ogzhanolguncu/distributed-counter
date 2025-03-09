package wal

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Helper function to create a temporary WAL for testing
func setupWAL(t *testing.T, enableFsync bool, maxFileSize int64) (*WAL, string) {
	dir, err := os.MkdirTemp("", "wal-test")
	require.NoError(t, err)

	w, err := OpenWAL(dir, enableFsync, maxFileSize)
	require.NoError(t, err)

	return w, dir
}

func TestWAL_BasicOperations(t *testing.T) {
	w, dir := setupWAL(t, false, 1024*1024)
	defer os.RemoveAll(dir)
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

	require.NoError(t, w.Sync())

	// Read and verify
	entries, err := w.ReadAll()
	require.NoError(t, err)
	require.Equal(t, len(testData), len(entries))

	for i, entry := range entries {
		require.Equal(t, testData[i], entry.Data)
		require.Equal(t, uint64(i+1), entry.SequenceNumber)
	}
}

func TestWAL_Persistence(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	testData := [][]byte{
		[]byte("persistence test 1"),
		[]byte("persistence test 2"),
		[]byte("persistence test 3"),
	}

	// First session: write data
	{
		w, err := OpenWAL(dir, true, 1024*1024)
		require.NoError(t, err)

		for _, data := range testData {
			require.NoError(t, w.WriteEntry(data))
		}

		require.NoError(t, w.Close())
	}

	// Second session: verify data
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
	w, dir := setupWAL(t, false, 100) // Small size to force rotation
	defer os.RemoveAll(dir)
	defer w.Close()

	// Write entries to trigger rotation
	for i := range 10 {
		data := fmt.Appendf(nil, "rotation test entry %d", i)
		require.NoError(t, w.WriteEntry(data))
	}

	require.NoError(t, w.Sync())

	// Check for multiple segment files
	files, err := filepath.Glob(filepath.Join(dir, "segment-*"))
	require.NoError(t, err)
	require.True(t, len(files) > 1, "Expected multiple segment files after rotation")
}

func TestWAL_LargeEntries(t *testing.T) {
	w, dir := setupWAL(t, false, 5*1024*1024)
	defer os.RemoveAll(dir)
	defer w.Close()

	// Generate and write a 1MB entry
	largeData := make([]byte, 1024*1024)
	_, err := rand.Read(largeData)
	require.NoError(t, err)

	require.NoError(t, w.WriteEntry(largeData))
	require.NoError(t, w.Sync())

	// Verify
	entries, err := w.ReadAll()
	require.NoError(t, err)
	require.Equal(t, 1, len(entries))
	require.True(t, bytes.Equal(largeData, entries[0].Data))
}

func TestWAL_ConcurrentWrites(t *testing.T) {
	w, dir := setupWAL(t, true, 1024*1024)
	defer os.RemoveAll(dir)
	defer w.Close()

	numWriters := 10
	entriesPerWriter := 100
	errCh := make(chan error, numWriters)

	// Concurrent writers
	for i := range numWriters {
		go func(writerID int) {
			var writeErr error
			for j := range entriesPerWriter {
				data := fmt.Appendf(nil, "writer-%d-entry-%d", writerID, j)
				if err := w.WriteEntry(data); err != nil {
					writeErr = err
					break
				}
			}
			errCh <- writeErr
		}(i)
	}

	// Check results
	for range numWriters {
		require.NoError(t, <-errCh)
	}

	require.NoError(t, w.Sync())

	// Verify entries
	entries, err := w.ReadAll()
	require.NoError(t, err)
	require.Equal(t, numWriters*entriesPerWriter, len(entries))

	for i, entry := range entries {
		require.Equal(t, uint64(i+1), entry.SequenceNumber)
	}
}

func TestWAL_Recovery(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// Generate 10 test entries
	var testData [][]byte
	for i := 1; i <= 10; i++ {
		testData = append(testData, fmt.Appendf(nil, "test entry %d", i))
	}

	// First WAL session: Write all 10 entries
	{
		w, err := OpenWAL(dir, false, 1024*1024)
		require.NoError(t, err)

		for _, data := range testData {
			require.NoError(t, w.WriteEntry(data))
		}

		require.NoError(t, w.Sync())
		require.NoError(t, w.Close())
	}

	// Get the WAL file
	files, err := filepath.Glob(filepath.Join(dir, "segment-*"))
	require.NoError(t, err)
	require.NotEmpty(t, files)

	// Read the file content
	content, err := os.ReadFile(files[0])
	require.NoError(t, err)

	// Calculate approximate position of the last entry
	// We'll truncate the file to keep only the first 9 entries
	// and then append some corrupt data

	// Create a corrupted version - truncate most of the last entry and append garbage
	corruptedSize := int(float64(len(content)) * 0.9) // Approximate position before the last entry
	corruptedContent := append(content[:corruptedSize], []byte{0xFF, 0xFE, 0xFD, 0xFC}...)

	// Write the corrupted content back
	err = os.WriteFile(files[0], corruptedContent, 0644)
	require.NoError(t, err)

	// Second WAL session: Verify recovery keeps only the valid entries
	{
		w, err := OpenWAL(dir, false, 1024*1024)
		require.NoError(t, err)
		defer w.Close()

		entries, err := w.ReadAll()
		require.NoError(t, err)

		// Debug info
		t.Logf("Found %d entries after recovery", len(entries))
		for i, entry := range entries {
			t.Logf("Entry %d: Seq=%d, Data=%s", i, entry.SequenceNumber, string(entry.Data))
		}

		// We should have 9 valid entries (the 10th was corrupted)
		require.Equal(t, 9, len(entries), "Should recover exactly 9 valid entries")

		// Verify entries 1-9 match our test data
		for i := range 9 {
			require.Equal(t, testData[i], entries[i].Data,
				fmt.Sprintf("Entry %d data should match original", i+1))
			require.Equal(t, uint64(i+1), entries[i].SequenceNumber,
				fmt.Sprintf("Entry %d sequence number should be %d", i+1, i+1))
		}
	}
}
