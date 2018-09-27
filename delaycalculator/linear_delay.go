package delaycalculator

import (
	"time"
)

type (
	// LinearDelayCalculator will calculate linear delay duration based on number of attempts
	LinearDelayCalculator struct {
		initialInterval time.Duration
	}
)

// CalculateDelay returns delay duration
func (c *LinearDelayCalculator) CalculateDelay(numAttempts int) time.Duration {
	return time.Duration(numAttempts) * c.initialInterval
}

// NewLinearDelayCalculator returns an instance of LinearDelayCalculator
func NewLinearDelayCalculator(initialInterval time.Duration) *LinearDelayCalculator {
	return &LinearDelayCalculator{
		initialInterval: initialInterval,
	}
}
