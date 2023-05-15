package fleetctrl

import (
	"math/rand"
	"time"
)

// insureDurationGreaterProc calculates the time to wait for two concurrent processes on different sides to be guaranteed to complete.
// It does not guarantee that such a process will not be started by other participants while waiting, but it allows to get rid of
// collisions in the end after some number of iterations.
func insureDurationGreaterProc(dur time.Duration) time.Duration {
	return getRandDuration(halfDurationOf(dur)) + doubleDurationOf(dur)
}

// getRandDuration will return a random duration limited by the input argument.
func getRandDuration(rng time.Duration) time.Duration {
	return time.Duration(rand.Intn(int(rng))) //nolint:gosec
}

// halfDurationOf will return half the duration.
func halfDurationOf(dur time.Duration) time.Duration {
	return dur / 2 //nolint:gomnd
}

// doubleDurationOf will return the double duration.
func doubleDurationOf(dur time.Duration) time.Duration {
	return dur * 2 //nolint:gomnd
}
