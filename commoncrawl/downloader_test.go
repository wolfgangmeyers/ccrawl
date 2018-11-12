package commoncrawl

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"bytes"

	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
)

type mockResponseBody struct {
	*bytes.Reader
}

func (body *mockResponseBody) Close() error {
	return nil
}

type DownloaderTestSuite struct {
	suite.Suite
	downloader               *Downloader
	httpClient               *MockHTTPClient
	clock                    clockwork.FakeClock
	workItemsInput           chan *WorkItem
	workItemReceivedListener chan *WorkItem
	downloadOutput           chan FileDownload
}

func (suite *DownloaderTestSuite) SetupTest() {
	suite.httpClient = new(MockHTTPClient)
	suite.clock = clockwork.NewFakeClock()
	suite.workItemsInput = make(chan *WorkItem)
	suite.workItemReceivedListener = make(chan *WorkItem, 1)
	suite.downloadOutput = make(chan FileDownload)
	suite.downloader = NewDownloader(
		NewHTTPFileDownloadFactory("https://commoncrawl.s3.amazonaws.com/", suite.httpClient, suite.clock),
		suite.clock,
		suite.workItemsInput,
		suite.workItemReceivedListener,
		suite.downloadOutput,
		logrus.New(),
	)
}

// Verify that a FileDownload object fetches the correct response
func (suite *DownloaderTestSuite) TestFileDownload() {
	mockData := []byte("<h1>Hello, world!</h1>")
	mockResp := &http.Response{
		Body: &mockResponseBody{bytes.NewReader(mockData)},
	}
	suite.httpClient.On("Get", "https://commoncrawl.s3.amazonaws.com/test1.txt").Return(mockResp, nil)
	suite.downloader.Run()
	workItem := &WorkItem{
		WorkItemID:  "test-item",
		SourceFiles: []string{"test1.txt"},
	}
	go func() {
		suite.workItemsInput <- workItem
	}()
	tick()
	// Get a handle on the FileDownload so that it can be tested
	var download FileDownload
	select {
	case d := <-suite.downloadOutput:
		download = d
	default:
		suite.Fail("Expected FileDownload")
	}
	// Open the FileDownload and see what's inside!
	file, err := download.Open()
	suite.Require().Nil(err, "Expected no error")
	contents, err := ioutil.ReadAll(file)
	suite.Require().Nil(err, "Expected no error")
	suite.Require().Equal("test1.txt", download.Filename())
	suite.Require().Equal(mockData, contents)

	// Make sure work item received listener fired
	select {
	case workItemReceived := <-suite.workItemReceivedListener:
		suite.Require().Equal(workItem, workItemReceived)
	default:
		suite.Fail("Expected work item received")
	}
}

func (suite *DownloaderTestSuite) TestMultipleFileDownloads() {
	mockData := []byte("<h1>Hello, world!</h1>")

	suite.httpClient.On("Get", "https://commoncrawl.s3.amazonaws.com/test1.txt").Return(&http.Response{
		Body: &mockResponseBody{bytes.NewReader(mockData)},
	}, nil)
	suite.httpClient.On("Get", "https://commoncrawl.s3.amazonaws.com/test2.txt").Return(&http.Response{
		Body: &mockResponseBody{bytes.NewReader(mockData)},
	}, nil)
	suite.httpClient.On("Get", "https://commoncrawl.s3.amazonaws.com/test3.txt").Return(&http.Response{
		Body: &mockResponseBody{bytes.NewReader(mockData)},
	}, nil)

	suite.downloader.Run()
	workItem := &WorkItem{
		WorkItemID:  "test-item",
		SourceFiles: []string{"test1.txt", "test2.txt", "test3.txt"},
	}
	go func() {
		suite.workItemsInput <- workItem
	}()
	for _, filename := range []string{"test1.txt", "test2.txt", "test3.txt"} {
		tick()
		suite.clock.Advance(DownloadDelay)
		tick()
		// Get a handle on the FileDownload so that it can be tested
		select {
		case d := <-suite.downloadOutput:
			download := d
			// Open the FileDownload and see what's inside!
			file, err := download.Open()
			suite.Require().Nil(err, "Expected no error")
			contents, err := ioutil.ReadAll(file)
			suite.Require().Nil(err, "Expected no error")
			suite.Require().Equal(filename, download.Filename())
			suite.Require().Equal(mockData, contents)
		default:
			suite.Require().Fail(fmt.Sprintf("Expected FileDownload for '%v'", filename))
		}
	}
}

// Verify backoff behavior when 1 failure occurs when attempting to download
func (suite *DownloaderTestSuite) TestFileDownload1Retry() {
	mockData := []byte("<h1>Hello, world!</h1>")
	mockResp := &http.Response{
		Body: &mockResponseBody{bytes.NewReader(mockData)},
	}
	suite.httpClient.On("Get", "https://commoncrawl.s3.amazonaws.com/test1.txt").Return(
		nil, errors.New("???")).Times(1)
	suite.httpClient.On("Get", "https://commoncrawl.s3.amazonaws.com/test1.txt").Return(
		mockResp, nil).Times(1)

	suite.downloader.Run()
	go func() {
		suite.workItemsInput <- &WorkItem{
			WorkItemID:  "test-item",
			SourceFiles: []string{"test1.txt"},
		}
	}()
	tick()
	// Get a handle on the FileDownload so that it can be tested
	var download FileDownload
	select {
	case d := <-suite.downloadOutput:
		download = d
	default:
		suite.Fail("Expected FileDownload")
	}

	// Each error encountered during call to Open() will cause the
	// FileDownload to block and try again before returning. So this needs
	// to be run in a separate goroutine.
	resultChan := make(chan io.ReadCloser)
	resultErrChan := make(chan error)
	go func() {
		file, err := download.Open()
		if err != nil {
			resultErrChan <- err
		} else {
			resultChan <- file
		}
	}()

	// Verify that the open call is currently blocking
	tick()
	select {
	case result := <-resultChan:
		suite.Fail("Unexpected result: %v", result)
	case err := <-resultErrChan:
		suite.Fail("Unexpected error: %v", err)
	default:
		// do nothing
		tick()
	}
	// A single retry will wait 2 seconds
	suite.clock.Advance(time.Second * 2)
	tick()
	// A result should now be available
	select {
	case result := <-resultChan:
		contents, err := ioutil.ReadAll(result)
		suite.Require().Nil(err, "Expected no error")
		suite.Require().Equal(mockData, contents)
	case err := <-resultErrChan:
		suite.Fail("Unexpected error: %v", err)
	default:
		suite.Fail("Call to Open() should no longer be blocked")
	}
}

// Verify backoff behavior when 5 failures occur when attempting to download
// The download will try up to 5 times. So this should succeed.
func (suite *DownloaderTestSuite) TestFileDownload5Retries() {
	mockData := []byte("<h1>Hello, world!</h1>")
	mockResp := &http.Response{
		Body: &mockResponseBody{bytes.NewReader(mockData)},
	}
	suite.httpClient.On("Get", "https://commoncrawl.s3.amazonaws.com/test1.txt").Return(
		nil, errors.New("???")).Times(5)
	suite.httpClient.On("Get", "https://commoncrawl.s3.amazonaws.com/test1.txt").Return(
		mockResp, nil).Times(1)

	suite.downloader.Run()
	go func() {
		suite.workItemsInput <- &WorkItem{
			WorkItemID:  "test-item",
			SourceFiles: []string{"test1.txt"},
		}
	}()
	tick()
	// Get a handle on the FileDownload so that it can be tested
	var download FileDownload
	select {
	case d := <-suite.downloadOutput:
		download = d
	default:
		suite.Fail("Expected FileDownload")
	}

	// Each error encountered during call to Open() will cause the
	// FileDownload to block and try again before returning. So this needs
	// to be run in a separate goroutine.
	resultChan := make(chan io.ReadCloser)
	resultErrChan := make(chan error)
	go func() {
		file, err := download.Open()
		if err != nil {
			resultErrChan <- err
		} else {
			resultChan <- file
		}
	}()

	// advance clock at an exponential backoff to make
	// it past all of the retries
	backoff := time.Second * 2
	for i := 0; i < 5; i++ {
		// Verify that the open call is currently blocking
		tick()
		select {
		case <-resultChan:
			suite.Fail("Unexpected result")
		case err := <-resultErrChan:
			suite.Fail("Unexpected error: %v", err)
		default:
			// fmt.Printf("Blocked. backoff=%v\n", backoff)
			// do nothing
			tick()
		}
		// A single retry will wait 2 seconds
		suite.clock.Advance(backoff)
		tick()
		backoff *= 2
	}
	tick()

	// A result should now be available
	select {
	case result := <-resultChan:
		contents, err := ioutil.ReadAll(result)
		suite.Require().Nil(err, "Expected no error")
		suite.Require().Equal(mockData, contents)
	case err := <-resultErrChan:
		suite.Fail("Unexpected error: %v", err)
	default:
		suite.Fail("Call to Open() should no longer be blocked")
	}
}

// Verify backoff behavior when 5 failures occur when attempting to download
// The download will try up to 5 times, and an error will occur on try 6.
func (suite *DownloaderTestSuite) TestFileDownload6Retries() {
	mockData := []byte("<h1>Hello, world!</h1>")
	mockResp := &http.Response{
		Body: &mockResponseBody{bytes.NewReader(mockData)},
	}
	suite.httpClient.On("Get", "https://commoncrawl.s3.amazonaws.com/test1.txt").Return(
		nil, errors.New("???")).Times(6)
	// The FileDownload will not receive this result, because it gives up after error #6
	suite.httpClient.On("Get", "https://commoncrawl.s3.amazonaws.com/test1.txt").Return(
		mockResp, nil).Times(1)

	suite.downloader.Run()
	go func() {
		suite.workItemsInput <- &WorkItem{
			WorkItemID:  "test-item",
			SourceFiles: []string{"test1.txt"},
		}
	}()
	tick()
	// Get a handle on the FileDownload so that it can be tested
	var download FileDownload
	select {
	case d := <-suite.downloadOutput:
		download = d
	default:
		suite.Fail("Expected FileDownload")
	}

	// Each error encountered during call to Open() will cause the
	// FileDownload to block and try again before returning. So this needs
	// to be run in a separate goroutine.
	resultChan := make(chan io.ReadCloser)
	resultErrChan := make(chan error)
	go func() {
		file, err := download.Open()
		if err != nil {
			resultErrChan <- err
		} else {
			resultChan <- file
		}
	}()

	// advance clock at an exponential backoff to make
	// it past all of the retries
	backoff := time.Second * 2
	for i := 0; i < 5; i++ {
		// Verify that the open call is currently blocking
		tick()
		select {
		case <-resultChan:
			suite.Fail("Unexpected result")
		case err := <-resultErrChan:
			suite.Fail("Unexpected error: %v", err)
		default:
			// fmt.Printf("Blocked. backoff=%v\n", backoff)
			// do nothing
			tick()
		}
		// A single retry will wait 2 seconds
		suite.clock.Advance(backoff)
		tick()
		backoff *= 2
	}
	tick()

	// An error should have been returned.
	select {
	case <-resultChan:
		suite.Fail("Unexpected result")
	case err := <-resultErrChan:
		suite.Require().Equal(errors.New("???"), err)
	default:
		suite.Fail("Call to Open() should no longer be blocked")
	}
}

func TestDownloaderTestSuite(t *testing.T) {
	suite.Run(t, new(DownloaderTestSuite))
}
