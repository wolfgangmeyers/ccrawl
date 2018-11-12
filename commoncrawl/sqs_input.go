package commoncrawl

import (
	"fmt"

	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/sirupsen/logrus"
)

// SQSInput reads in a list of WARC file paths from a local file
// and pushes each file path to an output channel for downstream processing.
// Files that have already been marked as completed from previous checkpoints
// will not be sent.
type SQSInput struct {
	regionName              string             // Name of the AWS region where the queue is located
	sqsQueueName            string             // Name of the queue that will provide input file names
	workItemsOutput         chan *WorkItem     // Output channel of WorkItem
	completedWorkItemsInput chan string        // Input channel of completed WARC file paths
	abortedWorkItemsInput   chan string        // Input channel of aborted files
	logger                  logrus.FieldLogger // Log events
	workItemReader          WorkItemReader     // Reads work items from queue messages
}

// NewSQSInput returns a new instance of SQSInput
//
// * regionName - Name of the AWS region where the queue is located
// * sqsQueueName - Name of SQS queue that should be polled for files
// * workItemsOutput - Output channel of work items
// * completedWorkItemsInput - Used to mark work items as completed so that they are not processed multiple times
// * abortedWorkItemsInput - Input channel of aborted work items
// * logger - Log events
func NewSQSInput(
	regionName string,
	sqsQueueName string,
	logger logrus.FieldLogger,
	workItemReader WorkItemReader,
) *SQSInput {
	input := new(SQSInput)
	input.regionName = regionName
	input.sqsQueueName = sqsQueueName
	input.workItemsOutput = make(chan *WorkItem)
	input.completedWorkItemsInput = make(chan string)
	input.abortedWorkItemsInput = make(chan string)
	input.logger = logger.WithField("component", "input_reader")
	input.workItemReader = workItemReader
	return input
}

// WorkItemsOutput returns an output channel of WorkItem
func (input *SQSInput) WorkItemsOutput() <-chan *WorkItem {
	return input.workItemsOutput
}

// CompletedWorkItemsInput returns an input channel of completed work item ids
func (input *SQSInput) CompletedWorkItemsInput() chan<- string {
	return input.completedWorkItemsInput
}

// AbortedWorkItemsInput returns an input channel of aborted work item ids
func (input *SQSInput) AbortedWorkItemsInput() chan<- string {
	return input.abortedWorkItemsInput
}

func (input *SQSInput) nextWorkItem(queueURL *string, sqsConnection *sqs.SQS, messages map[string]string) *WorkItem {
	messageInput := sqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: aws.Int64(int64(1)),
	}
	messageOutput, err := sqsConnection.ReceiveMessage(&messageInput)
	if err != nil {
		input.logger.Fatalf("Error getting items from the queue: '%v'", err.Error())
	}

	for _, msg := range messageOutput.Messages {
		workItem, err := input.workItemReader.ReadWorkItem(*msg.Body)
		if err == nil {
			messages[workItem.WorkItemID] = *msg.ReceiptHandle
			return workItem
		}
		input.logger.Errorf("Error reading work item for '%v': '%v'", *msg.Body, err.Error())
	}
	time.Sleep(time.Second)
	return nil
}

// Run launches its own goroutine. It will read the list of file paths
// found in `indexFile` and emit each path to `filenamesOutput`.
func (input *SQSInput) Run() {
	go func() {
		defer CatchFatalError(input.logger)()

		input.logger.Info("sqs input starting up")
		sqsConnection := sqs.New(session.New(), &aws.Config{Region: aws.String(input.regionName)})
		// set up queue listener
		getURLInput := sqs.GetQueueUrlInput{
			QueueName: aws.String(input.sqsQueueName),
		}
		getURLOutput, err := sqsConnection.GetQueueUrl(&getURLInput)
		if err != nil {
			input.logger.WithField("queueName", input.sqsQueueName).Fatal(fmt.Sprintf("Error getting queue url: '%v'", err.Error()))
		}
		queueURL := getURLOutput.QueueUrl
		// Keep track of queue messages so that they can be deleted
		// once the files have been processed
		messages := map[string]string{}
		workItem := input.nextWorkItem(queueURL, sqsConnection, messages)
		for workItem == nil {
			workItem = input.nextWorkItem(queueURL, sqsConnection, messages)
		}
		for {
			select {
			case abortedFile := <-input.abortedWorkItemsInput:
				messageReceipt, ok := messages[abortedFile]
				if ok {
					input := sqs.ChangeMessageVisibilityInput{
						QueueUrl:          queueURL,
						ReceiptHandle:     aws.String(messageReceipt),
						VisibilityTimeout: aws.Int64(0),
					}
					sqsConnection.ChangeMessageVisibility(&input)
					delete(messages, abortedFile)
				} else {
					input.logger.Errorf("Received aborted file without matching receipt: '%v'", abortedFile)
				}

			case workItemID := <-input.completedWorkItemsInput:
				if workItemID == "" {
					input.logger.Infof("SQS input shutting down")
					return
				}
				input.logger.Infof("Received completed work item '%v'", workItemID)
				// For every file completed, mark its corresponding task as
				// completed as well so that no duplicate work is done.
				messageReceipt, ok := messages[workItemID]
				if ok {
					deleteRequest := sqs.DeleteMessageInput{
						QueueUrl:      queueURL,
						ReceiptHandle: aws.String(messageReceipt),
					}
					_, err := sqsConnection.DeleteMessage(&deleteRequest)
					if err != nil {
						input.logger.Errorf("Error deleting message from queue: '%v'", err.Error())
					}
					delete(messages, workItemID)
				} else {
					input.logger.Errorf("Received completed work item without matching receipt: '%v'", workItemID)
				}
			case input.workItemsOutput <- workItem:
				workItem = input.nextWorkItem(queueURL, sqsConnection, messages)
			}
		}
	}()
}
