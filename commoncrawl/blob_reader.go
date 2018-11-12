package commoncrawl

import (
	"compress/gzip"
	"io"

	"bytes"

	"io/ioutil"

	"github.com/sirupsen/logrus"
)

// BlobReader implements the Reader interface. It will read an open
// file and output a stream of byte slices. It expects input files to be
// in gzip format as newline delimited text.
// Newline characters are not included in the output.
type BlobReader struct {
	logger logrus.FieldLogger // Log events
}

// NewBlobReader returns a new instance of BlobReader
//
// * logger - Log events
func NewBlobReader(logger logrus.FieldLogger) *BlobReader {
	reader := new(BlobReader)
	reader.logger = logger.WithField("component", "blob_reader")
	return reader
}

// Reads a file and sends a stream of record objects to callback
//
// * file - The open file that should be read
// * callback - Should be called for each record that is read
func (reader *BlobReader) Read(file io.ReadCloser, filename string, callback func(interface{})) {
	gzReader, err := gzip.NewReader(file)
	if err != nil {
		// TODO: this happens occasionally when connection is reset by S3
		// need to engineer a retry mechanism
		reader.logger.Errorf("Error parsing file '%v': '%v'", filename, err.Error())
		return
	}
	// Mystery: why won't the bufio.Reader work for reading the file into lines?
	// If uncommented, the code inside of the reader block does not execute
	data, _ := ioutil.ReadAll(gzReader)
	lines := bytes.Split(data, []byte{'\n'})
	// lineReader := bufio.NewReader(bytes.NewReader(data))

	for _, line := range lines {
		// for line, err := lineReader.ReadBytes('\n'); err != nil; {
		line = bytes.TrimSpace(line)
		callback(line)
	}

}
