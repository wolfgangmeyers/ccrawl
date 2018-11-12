package commoncrawl

import (
	"github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
)

// SingleInputSource implements the InputSource interface, and is designed
// to provide a single work item from SQS.
type SingleInputSource struct {
	filename                string
	workItemsOutput         chan *WorkItem
	completedWorkItemsInput chan string
	logger                  logrus.FieldLogger
	workItemReader          WorkItemReader // Lists files in S3 folders
}

// NewSingleInputSource returns a new instance of `SingleInputSource`
//
// * receiptHandle - Receipt handle from SQS message. This is used as the work item id.
// * filenames - List of files to process
func NewSingleInputSource(
	region string,
	conf *TaskConfiguration,
	logger logrus.FieldLogger,
) *SingleInputSource {
	input := new(SingleInputSource)
	input.filename = conf.InputFilename
	input.workItemsOutput = make(chan *WorkItem, 1)
	input.completedWorkItemsInput = make(chan string)
	input.logger = logger
	if conf.InputBucket == "" {
		input.workItemReader = NewDefaultWorkItemReader()
	} else {
		// This indicates possible reduce phase, and S3 work item reader
		// can enumerate files to reduce in S3
		workItemReader, err := NewS3WorkItemReader(region, conf)
		if err != nil {
			logger.Fatalf("Error creating work item reader: '%v'", err.Error())
		}
		input.workItemReader = workItemReader
	}

	return input
}

// Run launches its own goroutine.
// Begins reading input and producing WorkItem instances for processing.
func (input *SingleInputSource) Run() {
	go func() {
		workItem, err := input.workItemReader.ReadWorkItem(input.filename)
		if err != nil {
			input.logger.Fatalf("Error reading work item: %v", err.Error())
		}
		workItem.WorkItemID = uuid.Must(uuid.NewV4()).String()
		// queue up single work item first
		input.logger.Info("Sending work item out")
		input.workItemsOutput <- workItem
		input.logger.Info("Work item sent")
		for {
			select {
			case workItemID := <-input.completedWorkItemsInput:
				if workItemID == "" {
					return // We're done here
				}
				// No need to delete the work item. The lambda handler will delete the task
			}
		}
	}()
}

// WorkItemsOutput returns an output channel of WorkItem
func (input *SingleInputSource) WorkItemsOutput() <-chan *WorkItem {
	return input.workItemsOutput
}

// CompletedWorkItemsInput returns an input channel of completed work item ids
func (input *SingleInputSource) CompletedWorkItemsInput() chan<- string {
	return input.completedWorkItemsInput
}
