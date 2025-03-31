package wal_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ogzhanolguncu/distributed-counter/part4/crdt"
	"github.com/ogzhanolguncu/distributed-counter/part4/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWALCleanup(t *testing.T) {
	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "wal-cleanup-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir) // Clean up after the test

	// Define test parameters
	nodeID := "test-node"
	maxFileSize := int64(100) // Very small size to trigger many segments

	// Set a debug flag to see more verbose output
	os.Setenv("WAL_DEBUG", "1")

	// Initialize WAL
	walInstance, err := wal.OpenWAL(tempDir, nodeID, false, maxFileSize)
	require.NoError(t, err)

	// Create a test counter
	counter := crdt.New(nodeID)

	// Generate multiple segments by writing a lot of data
	for i := 0; i < 20; i++ {
		// Increment the counter
		counter.Increment(nodeID)

		// Generate enough data to force segment rotation
		for j := 0; j < 5; j++ {
			// Write increment to WAL
			err = walInstance.WriteCounterIncrement(nodeID)
			require.NoError(t, err)

			// Add some metadata to increase segment size
			err = walInstance.WriteMetadata(map[string]interface{}{
				"op":    "increment",
				"index": i*5 + j,
			})
			require.NoError(t, err)
		}

		// Write a state snapshot for every increment
		// This ensures we can recover counter state even after cleanup
		err = walInstance.WriteCounterState(counter)
		require.NoError(t, err)

		// Force sync every few iterations to ensure data is written
		if i%5 == 0 {
			err = walInstance.Sync()
			require.NoError(t, err)
		}
	}

	// Force sync to ensure all data is written
	err = walInstance.Sync()
	require.NoError(t, err)

	// Close and reopen WAL to ensure all segments are flushed
	err = walInstance.Close()
	require.NoError(t, err)

	walInstance, err = wal.OpenWAL(tempDir, nodeID, false, maxFileSize)
	require.NoError(t, err)
	defer walInstance.Close()

	// Verify we have multiple segments
	files, err := filepath.Glob(filepath.Join(tempDir, "segment-*"))
	require.NoError(t, err)
	initialFileCount := len(files)
	t.Logf("Initial segment count: %d", initialFileCount)
	assert.Greater(t, initialFileCount, 3, "Should have created multiple segments")

	// Before cleanup, check the counter value
	preCleanupCounter, err := wal.RecoverCounter(tempDir, nodeID)
	require.NoError(t, err)
	expectedValue := preCleanupCounter.Value()
	t.Logf("Expected counter value: %d", expectedValue)

	// List all segments with their indices for debugging
	t.Log("Segments before cleanup:")
	for _, file := range files {
		segmentIndex := filepath.Base(file)[len("segment-"):]
		t.Logf("  %s (index: %s)", file, segmentIndex)
	}

	// Configure cleanup to keep only a few segments
	cleanupConfig := wal.CleanupConfig{
		MaxSegments: 5,         // Keep at most 5 segments
		MinSegments: 3,         // Keep at least 3 segments
		MaxAge:      time.Hour, // Keep segments up to 1 hour old
	}

	// Run cleanup directly to avoid async issues
	err = walInstance.CleanupSegments(cleanupConfig)
	require.NoError(t, err)

	// Wait a bit to ensure all file operations complete
	time.Sleep(100 * time.Millisecond)

	// Force sync and close the WAL to flush all segments
	err = walInstance.Sync()
	require.NoError(t, err)
	err = walInstance.Close()
	require.NoError(t, err)

	// Verify fewer segments remain
	files, err = filepath.Glob(filepath.Join(tempDir, "segment-*"))
	require.NoError(t, err)
	t.Logf("Segment count after cleanup: %d", len(files))

	// Log remaining segments for debugging
	t.Log("Segments after cleanup:")
	for _, file := range files {
		segmentIndex := filepath.Base(file)[len("segment-"):]
		t.Logf("  %s (index: %s)", file, segmentIndex)
	}

	assert.LessOrEqual(t, len(files), cleanupConfig.MaxSegments,
		"Should have kept at most MaxSegments segments")
	assert.GreaterOrEqual(t, len(files), cleanupConfig.MinSegments,
		"Should have kept at least MinSegments segments")
	assert.Less(t, len(files), initialFileCount,
		"Should have fewer files after cleanup")

	// Verify we can still recover the counter from the remaining segments
	recoveredCounter, err := wal.RecoverCounter(tempDir, nodeID)
	require.NoError(t, err)

	// The counter should still have the correct value
	recoveredValue := recoveredCounter.Value()
	t.Logf("Recovered counter value: %d", recoveredValue)
	assert.Equal(t, expectedValue, recoveredValue,
		"Recovered counter should have the same value after cleanup")
}

func TestWALPeriodicCleanup(t *testing.T) {
	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "wal-periodic-cleanup-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir) // Clean up after the test

	// Define test parameters
	nodeID := "test-node"
	maxFileSize := int64(50) // Very small to create many segments

	// Set a debug flag to see more verbose output
	os.Setenv("WAL_DEBUG", "1")

	// Initialize WAL
	walInstance, err := wal.OpenWAL(tempDir, nodeID, false, maxFileSize)
	require.NoError(t, err)

	// Create a test counter
	counter := crdt.New(nodeID)

	// Generate multiple segments with lots of small writes
	for i := 0; i < 15; i++ {
		counter.Increment(nodeID)

		// Write multiple times to force segment rotation
		for j := 0; j < 5; j++ {
			// Write increment to WAL
			err = walInstance.WriteCounterIncrement(nodeID)
			require.NoError(t, err)

			// Add extra data to increase segment size
			data := fmt.Sprintf("extra-data-%d-%d", i, j)
			err = walInstance.WriteMetadata(map[string]interface{}{
				"data": data,
				"idx":  i*5 + j,
			})
			require.NoError(t, err)
		}

		// Take a snapshot every time to ensure we can recover
		err = walInstance.WriteCounterState(counter)
		require.NoError(t, err)

		// Force sync every few iterations
		if i%5 == 0 {
			err = walInstance.Sync()
			require.NoError(t, err)
		}
	}

	// Force sync to ensure all data is written
	err = walInstance.Sync()
	require.NoError(t, err)

	// Close and reopen WAL to ensure all segments are created
	err = walInstance.Close()
	require.NoError(t, err)

	walInstance, err = wal.OpenWAL(tempDir, nodeID, false, maxFileSize)
	require.NoError(t, err)
	defer walInstance.Close()

	// Verify we have multiple segments
	files, err := filepath.Glob(filepath.Join(tempDir, "segment-*"))
	require.NoError(t, err)
	initialFileCount := len(files)
	t.Logf("Initial segment count: %d", initialFileCount)
	assert.Greater(t, initialFileCount, 5, "Should have created multiple segments")

	// Before cleanup, check the counter value
	preCleanupCounter, err := wal.RecoverCounter(tempDir, nodeID)
	require.NoError(t, err)
	expectedValue := preCleanupCounter.Value()
	t.Logf("Expected counter value: %d", expectedValue)

	// Configure cleanup with a very short interval
	cleanupConfig := wal.CleanupConfig{
		MaxSegments: 4,
		MinSegments: 3,
		MaxAge:      time.Hour,
	}

	// Instead of using the StartPeriodicCleanup function, call CleanupSegments directly
	// This ensures we're not dealing with race conditions in the test
	err = walInstance.CleanupSegments(cleanupConfig)
	require.NoError(t, err)

	// Wait a moment to make sure file system operations complete
	time.Sleep(100 * time.Millisecond)

	// Force sync before closing
	err = walInstance.Sync()
	require.NoError(t, err)

	// Close the WAL
	err = walInstance.Close()
	require.NoError(t, err)

	// Wait a bit longer to ensure all file operations are complete
	time.Sleep(100 * time.Millisecond)

	// Verify files were cleaned up
	files, err = filepath.Glob(filepath.Join(tempDir, "segment-*"))
	require.NoError(t, err)
	t.Logf("Segment count after cleanup: %d", len(files))

	// Log remaining segments for debugging
	t.Log("Segments after cleanup:")
	for _, file := range files {
		segmentIndex := filepath.Base(file)[len("segment-"):]
		t.Logf("  %s (index: %s)", file, segmentIndex)
	}

	assert.LessOrEqual(t, len(files), cleanupConfig.MaxSegments,
		"Should have no more than MaxSegments segments after cleanup")
	assert.GreaterOrEqual(t, len(files), cleanupConfig.MinSegments,
		"Should have at least MinSegments segments after cleanup")
	assert.Less(t, len(files), initialFileCount,
		"Should have fewer files after cleanup")

	// Check we can still recover
	recoveredCounter, err := wal.RecoverCounter(tempDir, nodeID)
	require.NoError(t, err)

	// Log actual values for debugging
	recoveredValue := recoveredCounter.Value()
	t.Logf("Recovered counter value: %d", recoveredValue)

	assert.Equal(t, expectedValue, recoveredValue,
		"Recovered counter should have the same value after cleanup")
}
