package delaycalculator_test

import (
	"testing"
	"time"

	"github.com/honestbee/kafkaclient/delaycalculator"
	"github.com/honestbee/kafkaclient/testingutil"
)

func TestExponentialCalculateDelay(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		initialInterval    time.Duration
		backoffCoefficient float64
		attemp             int
		expectedDuration   time.Duration
	}{
		{0 * time.Second, 2, 0, 0 * time.Second},
		{0 * time.Second, 2, 1, 0 * time.Second},
		{1 * time.Second, 2, 0, 1 * time.Second},
		{1 * time.Second, 2, 1, 2 * time.Second},
		{2 * time.Second, 2, 3, 16 * time.Second},
	}

	for _, tc := range testCases {
		calc := delaycalculator.NewExponentialDelayCalculator(tc.initialInterval, tc.backoffCoefficient)
		duration := calc.CalculateDelay(tc.attemp)

		testingutil.Equals(t, tc.expectedDuration, duration)
	}
}
