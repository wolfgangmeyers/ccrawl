package commoncrawl

import (
	"errors"
	"fmt"
	"log"

	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// A WorkItem is a single task for the process, and
// will have one to many source files that need to be processed.
type WorkItem struct {
	WorkItemID  string   // ID of the work item
	SourceFiles []string // Source files to be processed
}

// WorkItemReader parses raw messages from a queue
// and outputs `WorkItem` instances.
type WorkItemReader interface {
	// ReadWorkItem parses the message into a `WorkItem` instance
	ReadWorkItem(message string) (*WorkItem, error)
}

// Read the message as a list of source file paths, delimited by newline character
func readMessageAsFileList(message string) *WorkItem {
	// workItemID := uuid.NewV4().String()
	lines := strings.Split(message, "\n")
	sourceFiles := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			sourceFiles = append(sourceFiles, line)
		}
	}
	return &WorkItem{
		WorkItemID:  lines[0],
		SourceFiles: sourceFiles,
	}
}

// DefaultWorkItemReader implements the WorkItemReader interface.
// WorkItems are produced with a WorkItemId and a single source file that
// are the same as the input message.
type DefaultWorkItemReader struct{}

// NewDefaultWorkItemReader returns a new instance of DefaultWorkItemReader
func NewDefaultWorkItemReader() *DefaultWorkItemReader {
	return new(DefaultWorkItemReader)
}

// ReadWorkItem parses the message into a `WorkItem` instance.
func (workItemReader *DefaultWorkItemReader) ReadWorkItem(message string) (*WorkItem, error) {
	return readMessageAsFileList(message), nil
}

// S3WorkItemReader implements the WorkItemReader interface for S3 source files.
type S3WorkItemReader struct {
	s3Bucket     string
	s3Connection *s3.S3
}

// NewS3WorkItemReader returns a new instance of S3WorkItemReader
//
// * conf - AWSRegionName and S3InputBucket are required.
func NewS3WorkItemReader(regionName string, conf *TaskConfiguration) (*S3WorkItemReader, error) {
	if regionName == "" {
		return nil, errors.New("AWSRegionName is required for S3WorkItemReader")
	}
	bucket := conf.InputBucket
	if bucket == "" {
		return nil, errors.New("S3InputBucket is required for S3WorkItemReader")
	}
	workItemReader := new(S3WorkItemReader)
	workItemReader.s3Bucket = bucket
	workItemReader.s3Connection = s3.New(session.New(), &aws.Config{Region: aws.String(regionName)})
	return workItemReader, nil
}

// ReadWorkItem parses the message into a `WorkItem` instance.
// It queries S3 by prefix in the message and sets the result list of keys as source files in the `WorkItem`
func (workItemReader *S3WorkItemReader) ReadWorkItem(message string) (*WorkItem, error) {
	log.Printf("Reading work item: '%v'", message)
	if strings.HasSuffix(message, "/") {
		sourceFiles := []string{}
		var marker *string
		counter := 0
		for {
			listKeysInput := &s3.ListObjectsInput{
				Bucket:    aws.String(workItemReader.s3Bucket),
				Prefix:    aws.String(message),
				Delimiter: aws.String("/"),
				Marker:    marker,
			}
			listKeysOutput, err := workItemReader.s3Connection.ListObjects(listKeysInput)
			if err != nil {
				return nil, fmt.Errorf("Error fetching list of objects from S3: %v", err.Error())
			}
			for i := 0; i < len(listKeysOutput.Contents); i++ {
				sourceFiles = append(sourceFiles, *listKeysOutput.Contents[i].Key)
				counter++
			}
			log.Printf("Read %v source files", counter)
			if *listKeysOutput.IsTruncated {
				marker = listKeysOutput.NextMarker
			} else {
				break
			}
		}
		return &WorkItem{
			WorkItemID:  message,
			SourceFiles: sourceFiles,
		}, nil
	}
	return readMessageAsFileList(message), nil
}
