// Code generated by mockery v1.0.0. DO NOT EDIT.
package kafkaclient

import mock "github.com/stretchr/testify/mock"

// MockMonitorer is an autogenerated mock type for the Monitorer type
type MockMonitorer struct {
	mock.Mock
}

// Counter provides a mock function with given fields: name
func (_m *MockMonitorer) Counter(name string) Counter {
	ret := _m.Called(name)

	var r0 Counter
	if rf, ok := ret.Get(0).(func(string) Counter); ok {
		r0 = rf(name)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(Counter)
		}
	}

	return r0
}
