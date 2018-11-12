package commoncrawl

import (
	"encoding/json"
	"io"

	"github.com/sirupsen/logrus"
)

// JSONReader implements the Reader interface. It will read an open
// file and output a stream of records. It is designed to capture the output
// of the default OutputWriter implementations that are provided for
// writing data extracted from the common crawl. This allows for
// additional processing to be done on extracted data.
type JSONReader struct {
	*BlobReader
	recordFactory func() interface{} // Create new records to receive json data
}

// NewJSONReader returns a new instance of JSONReader
//
// * logger - Log events
func NewJSONReader(recordFactory func() interface{}, logger logrus.FieldLogger) *JSONReader {
	reader := new(JSONReader)
	reader.BlobReader = NewBlobReader(logger)
	reader.recordFactory = recordFactory
	reader.logger = logger.WithField("component", "json_reader")
	return reader
}

// Reads a file and sends a stream of record objects to callback
//
// * file - The open file that should be read
// * callback - Should be called for each record that is read
func (reader *JSONReader) Read(file io.ReadCloser, filename string, callback func(interface{})) {
	var err error
	parseErrCount := 0
	parseErrString := ""
	reader.BlobReader.Read(file, filename, func(blob interface{}) {
		line := blob.([]byte)
		record := reader.recordFactory()
		parseErr := json.Unmarshal(line, record)
		if parseErr == nil {
			callback(record)
		} else {
			if parseErrCount == 0 {
				parseErrString = parseErr.Error()
			}
			parseErrCount++
		}
	})

	if parseErrCount > 0 {
		reader.logger.Errorf("%v errors when parsing records. First error is '%v'", parseErrCount, parseErrString)
	}
	if err != nil {
		reader.logger.Errorf("Error reading file: '%v'", err.Error())
	}
}
