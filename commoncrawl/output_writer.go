package commoncrawl

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"sort"

	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
)

const fileWriterPoolSize = 10

// Container struct to write sharded data using a worker pool
type shardedDataItem struct {
	shardNum    int
	shardedData []OutputItem
}

// The OutputWriter is responsible for persisting the output of common crawl processing.
// Specific storage medium, output format, file or record name, is decided by the implementation.
type OutputWriter struct {
	fileWriter   OutputFileWriter   // Write output files
	outputFolder string             // Folder that will contain output files
	logger       logrus.FieldLogger // Log events
	shards       int                // Number of shards for output
}

// NewOutputWriter returns a new instance of OutputWriter
//
// * fileWriter - Write output files
// * outputFolder - Folder that will contain output files
// * logger - Log events
// * shards - Number of shards for output
func NewOutputWriter(
	fileWriter OutputFileWriter,
	outputFolder string,
	logger logrus.FieldLogger,
	shards int,
) *OutputWriter {
	outputWriter := new(OutputWriter)
	outputWriter.fileWriter = fileWriter
	outputWriter.outputFolder = outputFolder
	outputWriter.logger = logger
	outputWriter.shards = shards
	return outputWriter
}

// WriteOutputData  Writes a segment of output data to a destination.
//
// * data - Data that should be persisted.
func (writer *OutputWriter) WriteOutputData(outputSet string, data []OutputItem) {
	// Split data into N shards
	dataShards := make([][]OutputItem, 0, writer.shards)

	for shardNum := 0; shardNum < writer.shards; shardNum++ {
		dataShards = append(dataShards, make([]OutputItem, 0, len(data)/writer.shards))
	}
	for _, outputItem := range data {
		shardNum := int(hash(outputItem.Key()) % uint32(writer.shards))
		dataShards[shardNum] = append(dataShards[shardNum], outputItem)
	}

	queue := make(chan *shardedDataItem)
	worker := func() {
		for {
			select {
			case item := <-queue:
				if item == nil {
					return
				}
				shardNum := item.shardNum
				shardedData := item.shardedData

				// By sorting the output items by key, a later sort-merge
				// can be used to efficiently combine files.
				filename := fmt.Sprintf("%v/%v/%05v/%v.txt.gz", writer.outputFolder, outputSet, shardNum, uuid.Must(uuid.NewV4()).String())
				logger := writer.logger.WithFields(logrus.Fields{
					"filename": filename,
				})
				// logger.Info("Sorting items")
				sort.Sort(OutputItems(shardedData))
				// logger.Info("Sorting complete. converting items to json...")
				// S3 requires a readseeker, which means caching the entire file in memory
				// prior to sending it to S3.
				buffer := new(bytes.Buffer)
				gzWriter := gzip.NewWriter(buffer)
				fileWriter := bufio.NewWriter(gzWriter)
				// magic number: each segment will be at most 10K items large...
				SegmentOutputItems(shardedData, 10000, func(segment []OutputItem) {
					blobs := SerializeOutputItemsInParallel(segment, logger)
					for _, blob := range blobs {
						fileWriter.Write(blob)
						fileWriter.WriteString("\n")
					}
				})
				// logger.Info("json serialization complete")
				err := fileWriter.Flush()
				if err != nil {
					logger.Error(fmt.Sprintf("Error flushing file writer: '%v'", err.Error()))
				}
				err = gzWriter.Close()
				if err != nil {
					logger.Error(fmt.Sprintf("Error flushing gzip writer: '%v'", err.Error()))
				}
				filedata := buffer.Bytes()
				// logger.Info(fmt.Sprintf("Output file data created - len=%v", len(filedata)))

				// Write the file
				err = writer.fileWriter.WriteOutputFile(filename, filedata)
				if err != nil {
					logger.Fatalf("Error writing file: '%v'", err.Error())
				}
			}
		}
	}
	for i := 0; i < fileWriterPoolSize; i++ {
		go worker()
	}
	for shardNum, shardedData := range dataShards {
		queue <- &shardedDataItem{
			shardedData: shardedData,
			shardNum:    shardNum,
		}
	}
	for i := 0; i < fileWriterPoolSize; i++ {
		queue <- nil
	}
	writer.logger.Infof("Data has been written")
}

// OutputFileWriter writes specific output files.
type OutputFileWriter interface {
	// Write out the specified file
	WriteOutputFile(filepath string, data []byte) error
}

// NewOutputFileWriter returns a new instance of OutputFileWriter based on configuration
func NewOutputFileWriter(region string, conf *TaskConfiguration) OutputFileWriter {
	return NewS3OutputFileWriter(region, conf.OutputBucket)
}
