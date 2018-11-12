package commoncrawl

// An InputSource represents the source of work items to be processed
type InputSource interface {
	// Run launches its own goroutine.
	// Begins reading input and producing WorkItem instances for processing.
	Run()
	// WorkItemsOutput returns an output channel of WorkItem
	WorkItemsOutput() <-chan *WorkItem
	// CompletedWorkItemsInput returns an input channel of completed work item ids
	CompletedWorkItemsInput() chan<- string
}

// // NewInputSource returns an InputSource instance based on configuration.
// func NewInputSource(conf *Configuration, logger logrus.FieldLogger) (InputSource, error) {
// 	var workItemReader WorkItemReader
// 	var err error
// 	if conf.LocalSourceFiles {
// 		workItemReader = NewLocalWorkItemReader()
// 	} else if conf.S3InputBucket != "" {
// 		workItemReader, err = NewS3WorkItemReader(conf)
// 		if err != nil {
// 			return nil, err
// 		}
// 	} else {
// 		// Either testing or reading from public common crawl bucket
// 		workItemReader = NewDefaultWorkItemReader()
// 	}
// 	if conf.AWSRegionName != "" && conf.SQSQueueName != "" {
// 		return NewSQSInput(conf.AWSRegionName, conf.SQSQueueName, logger, workItemReader), nil
// 	} else if conf.InputFileName != "" {
// 		return NewFilesystemInput(conf.InputFileName, workItemReader, logger), nil
// 	} else {
// 		return nil, errors.New("Could not determine correct input source from configuration")
// 	}
// }
