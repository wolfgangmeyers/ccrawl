package commoncrawl

import http "net/http"
import mock "github.com/stretchr/testify/mock"

// MockHTTPClient is an autogenerated mock type for the HTTPClient type
type MockHTTPClient struct {
	mock.Mock
}

// Get provides a mock function with given fields: url
func (_m *MockHTTPClient) Get(url string) (*http.Response, error) {
	ret := _m.Called(url)

	var r0 *http.Response
	if rf, ok := ret.Get(0).(func(string) *http.Response); ok {
		r0 = rf(url)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*http.Response)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(url)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
