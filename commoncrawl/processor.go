package commoncrawl

import (
	"runtime/debug"

	"github.com/sirupsen/logrus"
)

// A RecordMapper is responsible for converting a record object
// into the desired output form.
//
// The RecordMapper should be able to accept input for the configured RecordParser
type RecordMapper interface {
	// Maps a record into zero to many OutputItems.
	//
	// * record - A record object containing data about a webpage
	// * output - Each OutputItem that is sent to this callback will be reduced in the output.
	MapRecord(record interface{}, output func(OutputItem))
}

// A RecordOutput wraps and OutputItem and refers to the source file
type RecordOutput struct {
	WorkItemID string     // Name of the work item associated with the file
	SourceFile string     // Name of the file where the data originated
	OutputItem OutputItem // Output data
}

// A RecordProcessor is responsible for converting a stream of
// WARCRecord objects into a stream of output data.
type RecordProcessor struct {
	mappers                 []RecordMapper      // Used to convert each record into an OutputItem
	recordInput             <-chan ParsedRecord // Input channel of record objects
	dataOutput              chan<- RecordOutput // Output channel of OutputItem objects
	recordProcessedListener chan<- string       // Listener for completed records
	outputMappedListener    chan<- string       // Listener for mapped OutputItems
	logger                  logrus.FieldLogger  // Log events
}

// NewRecordProcessor returns a new instance of RecordProcessor
//
// * mappers - Map of data type to mapper. Used to convert each WARCRecord object into `OutputItems``
// * recordInput - Input channel of record objects
// * dataOutput - Output channel of OutputItem objects
// * recordProcessedListener - Listener for completed records
// * outputMappedListener - Listener for mapped OutputItems
// * logger - Log events
func NewRecordProcessor(
	mappers []RecordMapper,
	recordInput <-chan ParsedRecord,
	dataOutput chan<- RecordOutput,
	recordProcessedListener chan<- string,
	outputMappedListener chan<- string,
	logger logrus.FieldLogger,
) *RecordProcessor {
	processor := new(RecordProcessor)
	processor.mappers = mappers
	processor.recordInput = recordInput
	processor.dataOutput = dataOutput
	processor.logger = logger.WithField("component", "processor")
	processor.recordProcessedListener = recordProcessedListener
	processor.outputMappedListener = outputMappedListener
	return processor
}

// Run launches its own goroutine. It will begin pulling WARCRecord objects
// from recordInput, converting them to OutputItem objects, and sending each
// OutputItem object to dataOutput. Listeners will be notified when each
// record is processed.
func (processor *RecordProcessor) Run() {
	go func() {
		defer func() {
			err := recover()
			if err != nil {
				stack := debug.Stack()
				processor.logger.Fatalf("Fatal error in processing: %v\n%v", err, string(stack))
			}
		}()
		for {
			select {
			case record := <-processor.recordInput:
				for _, mapper := range processor.mappers {
					mapper.MapRecord(record.Record, func(outputItem OutputItem) {
						processor.dataOutput <- RecordOutput{
							WorkItemID: record.WorkItemID,
							SourceFile: record.SourceFile,
							OutputItem: outputItem,
						}
						processor.outputMappedListener <- record.SourceFile
					})
				}
				processor.recordProcessedListener <- record.SourceFile
			}
		}
	}()
}
