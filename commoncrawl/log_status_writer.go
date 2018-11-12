package commoncrawl

import (
	"fmt"

	"github.com/sirupsen/logrus"
)

// LogStatusWriter sends status notifications to the console
type LogStatusWriter struct {
	logger logrus.FieldLogger
}

// NewLogStatusWriter returns a new instance of LogStatusWriter
func NewLogStatusWriter(logger logrus.FieldLogger) *LogStatusWriter {
	writer := new(LogStatusWriter)
	writer.logger = logger.WithField("component", "status_writer")
	return writer
}

// LastUpdate       time.Time // Last time the process received any kind of update
// 	RecordsParsed    int       // Total # of records parsed
// 	RecordsProcessed int       // Total # of records fully processed
// 	OutputsMapped    int       // Total # of outputs mapped
// 	OutputsReduced   int       // Total # of outputs reduced
// 	FilesInProcess   int       // # of files currently being processed
// 	FilesParsed      int       // Total # of files parsed
// 	FilesReady       int       // # of files currently ready to be saved
// 	FilesCompleted   int       // Total # of files that have data saved
// 	OutputFilesSaved int       // Total # of output files that have been saved
// 	ProcessComplete  bool      // True if process is completed

// WriteStatus sends a notification about the current state of the system to the console.
func (writer *LogStatusWriter) WriteStatus(status ProcessStatus) {
	writer.logger.WithFields(
		logrus.Fields{
			"RecordsParsed":    status.RecordsParsed,
			"RecordsProcessed": status.RecordsProcessed,
			"OutputsMapped":    status.OutputsMapped,
			"OutputsReduced":   status.OutputsReduced,
			"FilesInProcess":   status.FilesInProcess,
			"FilesParsed":      status.FilesParsed,
			"OutputFilesSaved": status.OutputFilesSaved,
		}).Infof(
		"RecordsParsed=%v, RecordsProcessed=%v, OutputsMapped=%v, OutputsReduced=%v, FilesInProcess=%v, FilesParsed=%v, OutputFilesSaved=%v\n",
		status.RecordsParsed,
		status.RecordsProcessed,
		status.OutputsMapped,
		status.OutputsReduced,
		status.FilesInProcess,
		status.FilesParsed,
		status.OutputFilesSaved)
	if status.ProcessComplete {
		fmt.Println("Process complete")
	}
}
