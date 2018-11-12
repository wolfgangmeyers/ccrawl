package commoncrawl

import (
	"bytes"
	"io"
	"os"
	"path"
)

// LocalOutputFileWriter implements the OutputFileWriter interface.
// Writes files to the local file system
type LocalOutputFileWriter struct{}

// WriteOutputFile writes out the specified file to the local filesystem
func (fileWriter *LocalOutputFileWriter) WriteOutputFile(filepath string, data []byte) error {
	parentFolder := path.Dir(filepath)
	_, err := os.Stat(parentFolder)
	if err != nil {
		os.MkdirAll(parentFolder, 0755)
	}
	file, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.Copy(file, bytes.NewReader(data))
	return err
}
