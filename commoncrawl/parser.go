package commoncrawl

import (
	"runtime/debug"

	"io"

	"github.com/sirupsen/logrus"
)

// A RecordReader is responsible for outputting a stream
// of records from an open file.
type RecordReader interface {
	// Reads a file and sends a stream of record objects to callback
	//
	// * file - The open file that should be read
	// * callback - Should be called for each record that is read
	Read(file io.ReadCloser, filename string, callback func(interface{}))
}

// A ParsedRecord is the output of a Parser
type ParsedRecord struct {
	WorkItemID string      // Name of the work item associated with the file
	SourceFile string      // Name of the file where the record originated
	Record     interface{} // Contents of the record
}

// A Parser is responsible for parsing raw file data
// into record objects.
type Parser struct {
	reader       RecordReader        // Reads files into records
	fileInput    <-chan FileDownload // Input channel of FileDownload instances
	recordOutput chan<- ParsedRecord // Output channel of record instances

	fileParsingListener chan<- string      // Listener for begin of file parsing
	fileParsedListener  chan<- string      // Listener for completion of file parsing
	recordReadListener  chan<- string      // Listener for completed record reads
	logger              logrus.FieldLogger // Log events
}

// NewParser returns a new instance of Parser
//
// * reader = Reads files into records
// * fileInput - Input channel of FileDownload instances
// * recordOutput - Output channel of record instances
// * fileParsingListener - Notified when a file begins parsing
// * fileParsedListener - Notified when a file is completely parsed
// * recordReadListener - Notified for each record that is read from the file
// * logger - Log events
func NewParser(
	reader RecordReader,
	fileInput <-chan FileDownload,
	recordOutput chan<- ParsedRecord,
	fileParsingListener chan<- string,
	fileParsedListener chan<- string,
	recordReadListener chan<- string,
	logger logrus.FieldLogger,
) *Parser {
	parser := new(Parser)
	parser.reader = reader
	parser.fileInput = fileInput
	parser.recordOutput = recordOutput
	parser.fileParsingListener = fileParsingListener
	parser.fileParsedListener = fileParsedListener
	parser.recordReadListener = recordReadListener
	parser.logger = logger.WithField("component", "parser")
	return parser
}

// parseFile accepts a FileDownload object and emits
// a series of WARCRecord objects through recordOutput.
// All listeners are notified when a record is parsed, and
// when the file has been completely parsed.
func (parser *Parser) parseFile(file FileDownload) {
	logger := parser.logger.WithFields(logrus.Fields{
		"filename": file.Filename(),
	})
	handle, err := file.Open()
	if err != nil {
		logger.Fatalf("Error opening file: '%v'", err.Error())
	}
	defer handle.Close()
	parser.fileParsingListener <- file.Filename()
	parser.reader.Read(handle, file.Filename(), func(record interface{}) {
		parser.recordOutput <- ParsedRecord{
			WorkItemID: file.WorkItemID(),
			SourceFile: file.Filename(),
			Record:     record,
		}
		parser.recordReadListener <- file.Filename()
	})
	parser.fileParsedListener <- file.Filename()
}

// Run launches its own goroutine. It will begin pulling FileDownload objects from
// fileInput and sending parsed WARCRecord objects to recordOutput.
func (parser *Parser) Run() {
	go func() {
		defer func() {
			err := recover()
			if err != nil {
				stack := debug.Stack()
				parser.logger.Fatalf("Fatal error in processing: %v\n%v", err, string(stack))
			}
		}()
		for {
			select {
			case file := <-parser.fileInput:
				// parser.logger.Info(fmt.Sprintf("Parsing file '%v'", file.Filename()))
				parser.parseFile(file)
				// parser.logger.Info(fmt.Sprintf("Parsing complete for file '%v'", file.Filename()))
			}
		}
	}()
}
