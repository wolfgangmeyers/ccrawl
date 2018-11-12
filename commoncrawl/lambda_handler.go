package commoncrawl

import (
	"encoding/json"
	"log"
	"os"

	"github.com/sirupsen/logrus"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// LambdaHandler provides functionality to pull tasks off of an SQS
// queue that will configure a common crawl lambda task
type LambdaHandler struct {
	sqsClient *sqs.SQS
	queueURL  string
	task      func(*TaskConfiguration)
	logger    logrus.FieldLogger
}

// NewLambdaHandler returns a new instance of `LambdaHandler`
func NewLambdaHandler(task func(*TaskConfiguration), logger logrus.FieldLogger) *LambdaHandler {
	handler := new(LambdaHandler)
	handler.task = task
	handler.logger = logger
	handler.queueURL = os.Getenv("QUEUE_URL")
	if handler.queueURL == "" {
		log.Fatalf("QUEUE_URL environment variable is not set")
	}
	sqsConfig := &aws.Config{
		Region: aws.String(os.Getenv("AWS_REGION")), // This is provided in lambda
	}
	sqsEndpoint := os.Getenv("SQS_ENDPOINT")
	if sqsEndpoint != "" {
		sqsConfig.Endpoint = aws.String(sqsEndpoint)
	}
	handler.sqsClient = sqs.New(session.Must(session.NewSession()), sqsConfig)
	return handler
}

// Handle pulls a message off of SQS and parses it into a `TaskConfiguration` struct
func (handler *LambdaHandler) Handle() error {
	receiveMessageInput := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(handler.queueURL),
		MaxNumberOfMessages: aws.Int64(1),
	}
	receiveMessageOutput, err := handler.sqsClient.ReceiveMessage(receiveMessageInput)
	if err != nil {
		log.Fatalf("Error receiving message from sqs: %v", err.Error())
	}
	for _, message := range receiveMessageOutput.Messages {
		cfg := &TaskConfiguration{}
		err = json.Unmarshal([]byte(*message.Body), cfg)
		if err != nil {
			log.Fatalf("Error reading config '%v' from sqs: '%v'", *message.Body, err.Error())
		}
		receiptHandle := *message.ReceiptHandle
		handler.task(cfg)
		// If the code reaches this point, the task completed normally
		deleteMessageInput := &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(handler.queueURL),
			ReceiptHandle: aws.String(receiptHandle),
		}
		_, err = handler.sqsClient.DeleteMessage(deleteMessageInput)
		if err != nil {
			handler.logger.Errorf("Error deleting message: receipt handle='%v', err='%v'", receiptHandle, err.Error())
		}
	}
	handler.logger.Info("Lambda task completed")
	return nil
}
