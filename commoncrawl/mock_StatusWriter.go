package commoncrawl

import (
	mock "github.com/stretchr/testify/mock"
)

// MockStatusWriter is an autogenerated mock type for the StatusWriter type
type MockStatusWriter struct {
	mock.Mock
}

// WriteStatus provides a mock function with given fields: status
func (_m *MockStatusWriter) WriteStatus(status ProcessStatus) {
	_m.Called(status)
}

// GetStatusCalls returns a list of all ProcessStatus objects passed into WriteStatus
func (_m *MockStatusWriter) GetStatusCalls() []ProcessStatus {
	result := make([]ProcessStatus, len(_m.Calls))
	for i := 0; i < len(result); i++ {
		call := _m.Calls[i]
		status := call.Arguments.Get(0).(ProcessStatus)
		result[i] = status
	}
	return result
}