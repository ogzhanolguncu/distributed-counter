package assertions

import (
	"fmt"
	"runtime"
)

// Assert checks if the condition is true and panics with the given message if not.
// This function is meant to be removed in production builds.
func Assert(condition bool, msg string) {
	if !condition {
		_, file, line, _ := runtime.Caller(1)
		errorMsg := fmt.Sprintf("Assertion failed: %s\nFile: %s:%d", msg, file, line)
		panic(errorMsg)
	}
}

// AssertEqual checks if expected equals actual and panics with a message if not.
// This function is meant to be removed in production builds.
func AssertEqual(expected, actual any, msg string) {
	if expected != actual {
		_, file, line, _ := runtime.Caller(1)
		errorMsg := fmt.Sprintf("Assertion failed: %s\nExpected: %v, Got: %v\nFile: %s:%d",
			msg, expected, actual, file, line)
		panic(errorMsg)
	}
}

func AssertNotNil(value any, msg string) {
	if value == nil {
		_, file, line, _ := runtime.Caller(1)
		errorMsg := fmt.Sprintf("Assertion failed: %s\nExpected non-nil value\nFile: %s:%d",
			msg, file, line)
		panic(errorMsg)
	}
}
