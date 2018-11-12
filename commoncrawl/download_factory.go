package commoncrawl

import (
	"io"
	"net/http"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus"
)

// FileDownload represents a data file that can be downloaded
type FileDownload interface {
	// Open gets a handle on an open data file
	Open() (io.ReadCloser, error)
	// Filename returns the name of the data file
	Filename() string
	// WorkItemID returns the work item associated with the file
	WorkItemID() string
}

// HTTPFileDownload represents a connection to a WARC file in the common crawl public dataset
type HTTPFileDownload struct {
	datasetHost string          // Host name of the server hosting the file
	filename    string          // Name of the file to be downloaded
	workItemID  string          // Work item associated with the file
	httpClient  HTTPClient      // HTTP client that will be used to download
	clock       clockwork.Clock // Used for exponential backoff
}

// Open gets a handle on an open WARC file from S3
func (fileDownload *HTTPFileDownload) Open() (io.ReadCloser, error) {
	var resp *http.Response
	var err error
	err = WithRetries(fileDownload.clock, 5, func() error {
		resp, err = fileDownload.httpClient.Get(fileDownload.datasetHost + fileDownload.filename)
		return err
	})
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// Filename returns the name of the WARC file
func (fileDownload *HTTPFileDownload) Filename() string {
	return fileDownload.filename
}

// WorkItemID returns the work item associated with the file
func (fileDownload *HTTPFileDownload) WorkItemID() string {
	return fileDownload.workItemID
}

// A FileDownloadFactory creates FileDownload instances
type FileDownloadFactory interface {
	// NewFileDownload returns a new FileDownload object
	//
	// * workItemID - Work item associated with the file
	// * filename - Full path to the file to download
	NewFileDownload(workItemID string, filename string) FileDownload
}

// HTTPFileDownloadFactory creates HTTPFileDownload instances
type HTTPFileDownloadFactory struct {
	datasetHost string          // Hostname of the server that hosts files to download
	httpClient  HTTPClient      // HTTP client that will be used to download
	clock       clockwork.Clock // Used for exponential backoff
}

// NewHTTPFileDownloadFactory returns a new instance of HTTPFileDownloadFactory
//
// * httpClient - HTTP client that will be used to download
// * clock - Used for exponential backoff
func NewHTTPFileDownloadFactory(datasetHost string, httpClient HTTPClient, clock clockwork.Clock) *HTTPFileDownloadFactory {
	factory := new(HTTPFileDownloadFactory)
	factory.datasetHost = datasetHost
	factory.httpClient = httpClient
	factory.clock = clock
	return factory
}

// NewFileDownload returns a new HTTPFileDownload object
//
// * filename - Full path to the file to download
func (factory *HTTPFileDownloadFactory) NewFileDownload(workItemID string, filename string) FileDownload {
	return &HTTPFileDownload{
		datasetHost: factory.datasetHost,
		filename:    filename,
		workItemID:  workItemID,
		httpClient:  factory.httpClient,
		clock:       factory.clock,
	}
}

// S3FileDownloadFactory creates S3FileDownload instances
type S3FileDownloadFactory struct {
	s3Client     *s3.S3          // S3 client that will be used to download
	s3BucketName string          // S3 Input bucket name
	clock        clockwork.Clock // Used for exponential backoff
}

// NewS3FileDownloadFactory returns a new instance of S3FileDownloadFactory
//
// * regionName - AWS Region name of the S3 bucket
// * s3BucketName - S3 Input bucket name
// * clock - Used for exponential backoff
func NewS3FileDownloadFactory(regionName string, s3BucketName string, clock clockwork.Clock) *S3FileDownloadFactory {
	factory := new(S3FileDownloadFactory)
	factory.s3Client = s3.New(session.New(), &aws.Config{Region: aws.String(regionName)})
	factory.s3BucketName = s3BucketName
	factory.clock = clock
	return factory
}

// NewFileDownload returns a new S3FileDownload object
//
// * filename - Full path to the file to download
func (factory *S3FileDownloadFactory) NewFileDownload(workItemID string, filename string) FileDownload {
	return &S3FileDownload{
		clock:        factory.clock,
		s3Client:     factory.s3Client,
		s3BucketName: factory.s3BucketName,
		filename:     filename,
		workItemID:   workItemID,
	}
}

// S3FileDownload represents a handle on a file in an S3 bucket
type S3FileDownload struct {
	clock        clockwork.Clock
	s3Client     *s3.S3
	s3BucketName string
	filename     string
	workItemID   string
}

// Open gets a handle on an open data file from S3
func (fileDownload *S3FileDownload) Open() (io.ReadCloser, error) {
	input := s3.GetObjectInput{
		Bucket: aws.String(fileDownload.s3BucketName),
		Key:    aws.String(fileDownload.filename),
	}
	var resp *s3.GetObjectOutput
	var err error
	err = WithRetries(fileDownload.clock, 5, func() error {
		resp, err = fileDownload.s3Client.GetObject(&input)
		return err
	})
	if err != nil {
		return nil, err
	}
	return resp.Body, err
}

// Filename returns the name of the S3 file
func (fileDownload *S3FileDownload) Filename() string {
	return fileDownload.filename
}

// WorkItemID returns the work item associated with the file
func (fileDownload *S3FileDownload) WorkItemID() string {
	return fileDownload.workItemID
}

// LocalFileDownloadFactory creates LocalFileDownload instances
type LocalFileDownloadFactory struct {
}

// NewLocalFileDownloadFactory returns a new instance of LocalFileDownloadFactory
func NewLocalFileDownloadFactory() *LocalFileDownloadFactory {
	return new(LocalFileDownloadFactory)
}

// NewFileDownload returns a new LocalFileDownload object
//
// * workItemID - the associated work item
// * filename - Full path to the file to download
func (factory *LocalFileDownloadFactory) NewFileDownload(workItemID string, filename string) FileDownload {
	return &LocalFileDownload{
		filename:   filename,
		workItemID: workItemID,
	}
}

// LocalFileDownload points to a source file in the local file system
type LocalFileDownload struct {
	filename   string
	workItemID string
}

// Open gets a handle on an open data file
func (fileDownload *LocalFileDownload) Open() (io.ReadCloser, error) {
	return os.Open(fileDownload.filename)
}

// Filename returns the name of the file
func (fileDownload *LocalFileDownload) Filename() string {
	return fileDownload.filename
}

// WorkItemID returns the work item associated with the file
func (fileDownload *LocalFileDownload) WorkItemID() string {
	return fileDownload.workItemID
}

// NewFileDownloadFactory returns a new FileDownloadFactory based on configuration
func NewFileDownloadFactory(region string, conf *TaskConfiguration, clock clockwork.Clock, httpClient HTTPClient, logger logrus.FieldLogger) FileDownloadFactory {
	if conf.InputBucket != "" {
		logger.Info("Using S3 file downloads")
		return NewS3FileDownloadFactory(region, conf.InputBucket, clock)
	}
	logger.Info("Using HTTP file downloads")
	var datasetHost string
	if conf.DatasetHost == "" {
		datasetHost = PublicDatasetsPrefix
	} else {
		datasetHost = conf.DatasetHost
	}
	return NewHTTPFileDownloadFactory(datasetHost, httpClient, clock)
}
