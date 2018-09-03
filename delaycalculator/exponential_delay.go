package delaycalculator

import (
	"math"
	"time"
)

type (
	// ExponentialDelayCalculator will calculate delay using a coefficient to compute the next delay.
	// Formula used to compute the next delay is: initialInterval * math.Pow(backoffCoefficient, currentAttempt)
	ExponentialDelayCalculator struct {
		initialInterval    time.Duration
		backoffCoefficient float64
	}
)

// CalculateDelay returns delay duration
func (c *ExponentialDelayCalculator) CalculateDelay(numAttempts int) time.Duration {
	nextInterval := float64(c.initialInterval) * math.Pow(c.backoffCoefficient, float64(numAttempts))
	return time.Duration(nextInterval)
}

// NewExponentialDelayCalculator returns an instance of ExponentialDelayCalculator
func NewExponentialDelayCalculator(initialInterval time.Duration, backoffCoefficient float64) *ExponentialDelayCalculator {
	return &ExponentialDelayCalculator{
		initialInterval:    initialInterval,
		backoffCoefficient: backoffCoefficient,
	}
}
