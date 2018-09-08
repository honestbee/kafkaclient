package delaycalculator_test

import (
	"testing"
	"time"

	"github.com/honestbee/kafkaclient/delaycalculator"
	"github.com/honestbee/kafkaclient/testingutil"
)

func TestCalculateDelay(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		initialInterval  time.Duration
		attemp           int
		expectedDuration time.Duration
	}{
		{0 * time.Second, 0, 0 * time.Second},
		{0 * time.Second, 1, 0 * time.Second},
		{1 * time.Second, 0, 0 * time.Second},
		{1 * time.Second, 1, 1 * time.Second},
		{2 * time.Second, 3, 6 * time.Second},
	}

	for _, tc := range testCases {
		calc := delaycalculator.NewLinearDelayCalculator(tc.initialInterval)
		duration := calc.CalculateDelay(tc.attemp)

		testingutil.Equals(t, tc.expectedDuration, duration)
	}
}
