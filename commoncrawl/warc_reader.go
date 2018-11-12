package commoncrawl

import (
	"fmt"
	"io"

	"github.com/sirupsen/logrus"
	"github.com/wolfgangmeyers/go-warc/warc"
)

// WARCReader implements the Reader interface. It will read an open
// file and output a stream of WARCRecord objects.
type WARCReader struct {
	logger logrus.FieldLogger // Log events
}

// NewWARCReader returns a new instance of WARCReader
//
// * logger - Log events
func NewWARCReader(logger logrus.FieldLogger) *WARCReader {
	reader := new(WARCReader)
	reader.logger = logger
	return reader
}

// Reads a file and sends a stream of record objects to callback
//
// * file - The open file that should be read
// * callback - Should be called for each record that is read
func (reader *WARCReader) Read(file io.ReadCloser, filename string, callback func(interface{})) {
	warcFile, err := warc.NewWARCFile(file)
	if err != nil {
		reader.logger.Fatalf("Error parsing file '%v': '%v'", filename, err.Error())
	}
	wr := warcFile.GetReader()
	wr.Iterate(func(record *warc.WARCRecord, err error) {
		if err == nil {
			callback(record)
		} else {
			if err.Error() != "EOF" {
				reader.logger.Error(fmt.Sprintf("Error reading record: %v", err.Error()))
			}
		}
	})
}
