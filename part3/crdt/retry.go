package crdt

import (
	"math/rand"
	"time"
)

type Retry[T any] struct {
	MaxAttempts int
	Delay       time.Duration
}

// RetryResult wraps the result and a boolean indicating success
type RetryResult[T any] struct {
	Value T
	Done  bool
}

func (r *Retry[T]) Do(fn func() RetryResult[T]) T {
	var result RetryResult[T]
	attempts := 0
	currentDelay := r.Delay

	maxBackoff := 100 * time.Millisecond

	for attempts < r.MaxAttempts {
		result = fn()
		if result.Done {
			return result.Value
		}

		attempts++
		if attempts >= r.MaxAttempts {
			break
		}

		jitter := time.Duration(rand.Int63n(int64(currentDelay)))
		newDelay := currentDelay * 2
		if newDelay > maxBackoff {
			newDelay = maxBackoff
		}
		currentDelay = newDelay + jitter

		time.Sleep(currentDelay)
	}

	return result.Value
}
