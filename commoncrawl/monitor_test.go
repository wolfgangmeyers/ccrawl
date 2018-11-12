package commoncrawl

import (
	"fmt"
	"testing"

	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

// Tests for the Monitor component
type MonitorTestSuite struct {
	suite.Suite
	monitor               *Monitor
	statusWriter          *MockStatusWriter
	clock                 clockwork.FakeClock
	recordsParsedChan     chan string
	recordsProcessedChan  chan string
	outputsMappedChan     chan string
	outputsReducedChan    chan string
	workItemsReceivedChan chan *WorkItem
	filesParsingChan      chan string
	filesParsedChan       chan string
	workItemsOutputChan   chan string
	endOfInput            chan bool
}

func (suite *MonitorTestSuite) SetupTest() {
	suite.recordsParsedChan = make(chan string)
	suite.recordsProcessedChan = make(chan string)
	suite.outputsMappedChan = make(chan string)
	suite.outputsReducedChan = make(chan string)
	suite.workItemsReceivedChan = make(chan *WorkItem)
	suite.filesParsingChan = make(chan string)
	suite.filesParsedChan = make(chan string)
	suite.workItemsOutputChan = make(chan string, 1)
	suite.endOfInput = make(chan bool, 1)
	suite.statusWriter = new(MockStatusWriter)
	suite.clock = clockwork.NewFakeClock()
	suite.statusWriter.On("WriteStatus", mock.Anything).Return()
	suite.monitor = NewMonitor(
		suite.statusWriter,
		suite.clock,
		suite.recordsParsedChan,
		suite.recordsProcessedChan,
		suite.outputsMappedChan,
		suite.outputsReducedChan,
		suite.workItemsReceivedChan,
		suite.filesParsingChan,
		suite.filesParsedChan,
		suite.workItemsOutputChan,
		suite.endOfInput,
		logrus.New(),
	)
}

// The monitor should count the number of records parsed
// and emit that in the status object at the log interval.
func (suite *MonitorTestSuite) TestMonitorRecordsParsed() {
	// All other fields will have default values
	expectedStatus := ProcessStatus{
		RecordsParsed:  10,
		FilesInProcess: 1,
		LastUpdate:     suite.clock.Now().Add(MonitorLogInterval * 2),
	}
	suite.monitor.Run()
	suite.beginWorkItemSingleFile("test-item", "test.txt.gz")
	suite.beginFile("test.txt.gz")
	suite.parseRecords("test.txt.gz", 10)
	// Cause monitor to emit a status update
	suite.clock.Advance(MonitorLogInterval * 2)
	tick()
	calls := suite.statusWriter.GetStatusCalls()
	suite.Require().Equal(1, len(calls), "Expected exactly one call to WriteStatus")
	suite.Require().Equal(expectedStatus, calls[0], "Expected 10 records parsed")
}

// The monitor should count the number of records processed
// and emit that in the status object at the log interval.
func (suite *MonitorTestSuite) TestMonitorRecordsProcessed() {
	// All other fields will have default values
	expectedStatus := ProcessStatus{
		RecordsProcessed: 10,
		FilesInProcess:   1,
		LastUpdate:       suite.clock.Now().Add(MonitorLogInterval * 2),
	}
	suite.monitor.Run()
	suite.beginWorkItemSingleFile("test-item", "test.txt.gz")
	suite.beginFile("test.txt.gz")
	suite.processRecords("test.txt.gz", 10)
	// Cause monitor to emit a status update
	suite.clock.Advance(MonitorLogInterval * 2)
	tick()
	calls := suite.statusWriter.GetStatusCalls()
	suite.Require().Equal(1, len(calls), "Expected exactly one call to WriteStatus")
	suite.Require().Equal(expectedStatus, calls[0], "Expected 10 records processed")
}

// The monitor should count the number of outputs mapped
// and emit that in the status object at the log interval.
func (suite *MonitorTestSuite) TestMonitorOutputsMapped() {
	// All other fields will have default values
	expectedStatus := ProcessStatus{
		OutputsMapped:  10,
		FilesInProcess: 1,
		LastUpdate:     suite.clock.Now().Add(MonitorLogInterval * 2),
	}
	suite.monitor.Run()
	suite.beginWorkItemSingleFile("test-item", "test.txt.gz")
	suite.beginFile("test.txt.gz")
	suite.mapOutputs("test.txt.gz", 10)
	// Cause monitor to emit a status update
	suite.clock.Advance(MonitorLogInterval * 2)
	tick()
	calls := suite.statusWriter.GetStatusCalls()
	suite.Require().Equal(1, len(calls), "Expected exactly one call to WriteStatus")
	suite.Require().Equal(expectedStatus, calls[0], "Expected 10 outputs mapped")
}

func (suite *MonitorTestSuite) TestMonitorOutputsReduced() {
	// All other fields will have default values
	expectedStatus := ProcessStatus{
		OutputsReduced: 10,
		FilesInProcess: 1,
		LastUpdate:     suite.clock.Now().Add(MonitorLogInterval * 2),
	}
	suite.monitor.Run()
	suite.beginWorkItemSingleFile("test-item", "test.txt.gz")
	suite.beginFile("test.txt.gz")
	suite.reduceOutputs("test.txt.gz", 10)
	// Cause monitor to emit a status update
	suite.clock.Advance(MonitorLogInterval * 2)
	tick()
	calls := suite.statusWriter.GetStatusCalls()
	suite.Require().Equal(1, len(calls), "Expected exactly one call to WriteStatus")
	suite.Require().Equal(expectedStatus, calls[0], "Expected 10 outputs reduced")
}

// The monitor should count the number of files parsed
// and emit that in the status object at the log interval.
// Each parsed file is moved out of in process
func (suite *MonitorTestSuite) TestMonitorFilesParsed() {
	// All other fields will have default values
	expectedStatus := ProcessStatus{
		FilesParsed:    7,
		FilesInProcess: 3,
		LastUpdate:     suite.clock.Now(),
	}
	suite.monitor.Run()
	suite.beginWorkItemMultiFile("test-item", 10)
	tick()
	suite.beginFiles(10)
	suite.processFiles(7)
	tick()
	// Cause monitor to emit a status update
	suite.clock.Advance(MonitorLogInterval * 2)
	tick()
	calls := suite.statusWriter.GetStatusCalls()
	suite.Require().Equal(1, len(calls), "Expected exactly one call to WriteStatus")
	suite.Require().Equal(expectedStatus, calls[0], "Expected 7 files parsed")
}

// The monitor should not output any files if none have been fully parsed.
func (suite *MonitorTestSuite) TestMonitorCheckpointNoFilesProcessed() {
	suite.monitor.Run()
	suite.beginWorkItemSingleFile("test-item", "test.txt.gz")
	suite.beginFile("test.txt.gz")
	suite.parseRecords("test.txt.gz", 10)
	suite.processRecords("test.txt.gz", 10)
	suite.mapOutputs("test.txt.gz", 20)
	suite.reduceOutputs("test.txt.gz", 20)
	tick()
	// wait until monitor fires a status update
	suite.clock.Advance(MonitorLogInterval)
	tick()
	select {
	case <-suite.workItemsOutputChan:
		suite.Require().Fail("Unexpected output of files")
	default:
		break
	}
}

// The monitor should not output any files if not all have been fully parsed.
func (suite *MonitorTestSuite) TestMonitorCheckpoint9FilesProcessed() {
	suite.monitor.Run()
	suite.beginWorkItemMultiFile("test-item", 10)
	// begin parsing of 10 files
	suite.beginFiles(10)
	filenames := suite.filenames(10)
	suite.parseRecords2(filenames, 10)
	suite.processRecords2(filenames, 10)
	suite.mapOutputs2(filenames, 20)
	// only complete parsing of 9 files
	suite.processFiles(9)

	suite.reduceOutputs2(filenames, 20)
	tick()

	// wait until monitor fires a status update
	suite.clock.Advance(MonitorLogInterval)
	tick()
	select {
	case <-suite.workItemsOutputChan:
		suite.Require().Fail("Unexpected output of files")
	default:
		break
	}
}

// The monitor should output a completed work item id
// once all records have been parsed and processed, and all files have been parsed.
func (suite *MonitorTestSuite) TestMonitorCompletedWorkItem() {
	suite.monitor.Run()
	suite.beginWorkItemMultiFile("test-item", 10)
	// begin parsing of files
	suite.beginFiles(10)
	filenames := suite.filenames(10)
	suite.parseRecords2(filenames, 10)
	suite.processRecords2(filenames, 10)
	suite.mapOutputs2(filenames, 20)
	// complete parsing of files
	suite.processFiles(10)
	// complete reducing of outputs
	suite.reduceOutputs2(filenames, 20)
	tick()

	// wait until monitor fires a status update
	suite.clock.Advance(MonitorLogInterval)
	tick()
	select {
	case workItem := <-suite.workItemsOutputChan:
		suite.Require().Equal("test-item", workItem)
	default:
		suite.Require().Fail("No output files")
	}
}

// The monitor should wait until all records are processed before emitting
// a completed work item id
func (suite *MonitorTestSuite) TestMonitorCheckpoint10FilesProcessedOneRecordShort() {
	suite.monitor.Run()
	suite.beginWorkItemMultiFile("test-item", 10)
	// begin parsing file
	suite.beginFiles(10)
	filenames := suite.filenames(10)
	suite.parseRecords2(filenames, 10)
	suite.processRecords2(filenames[:9], 10)
	suite.processRecords(filenames[9], 9)
	suite.mapOutputs2(filenames, 20)

	suite.processFiles(10)

	suite.reduceOutputs2(filenames, 20)
	tick()
	// wait until monitor fires a status update
	suite.clock.Advance(MonitorLogInterval)
	tick()
	select {
	case <-suite.workItemsOutputChan:
		suite.Require().Fail("Unexpected output of files")
	default:
		break
	}
}

// The monitor should wait until all outputs are reduced before emitting
// a completed work item id
func (suite *MonitorTestSuite) TestMonitorCheckpoint10FilesProcessedOneOutputShort() {
	suite.monitor.Run()
	suite.beginWorkItemMultiFile("test-item", 10)
	// begin parsing file
	suite.beginFiles(10)
	filenames := suite.filenames(10)
	suite.parseRecords2(filenames, 10)
	suite.processRecords2(filenames, 10)
	suite.mapOutputs2(filenames, 20)

	suite.processFiles(10)

	suite.reduceOutputs2(filenames[:9], 20)
	suite.reduceOutputs(filenames[9], 19)
	tick()
	// wait until monitor fires a status update
	suite.clock.Advance(MonitorLogInterval)
	tick()
	select {
	case <-suite.workItemsOutputChan:
		suite.Require().Fail("Unexpected output of files")
	default:
		break
	}
}

// If any files remain that have not yet been parsed, the monitor should
// not emit an end of input signal
func (suite *MonitorTestSuite) TestMonitorAdditionalFilesNoEndOfInput() {
	suite.monitor.Run()
	suite.beginWorkItemMultiFile("test-item", 10)
	suite.beginWorkItemSingleFile("test-item-2", "file10.txt")
	// begin parsing of files
	suite.beginFiles(11)
	filenames := suite.filenames(10)
	suite.parseRecords2(filenames, 10)
	suite.processRecords2(filenames, 10)
	suite.mapOutputs2(filenames, 20)

	// complete parsing of files
	suite.processFiles(10)

	suite.reduceOutputs2(filenames, 20)
	tick()
	// wait until monitor fires a status update
	suite.clock.Advance(MonitorLogInterval)
	tick()
	select {
	case <-suite.workItemsOutputChan:
		break
	default:
		suite.Require().Fail("No output files")
	}
	tick()
	// check for end of input signal
	select {
	case <-suite.endOfInput:
		suite.Require().Fail("unexpected end of input signal")
	default:
		break
	}
}

// If all files are fully processed, the monitor should
// emit an end of input signal
func (suite *MonitorTestSuite) TestMonitorEndOfInput() {
	expectedStatus := ProcessStatus{
		ProcessComplete:  true,
		LastUpdate:       suite.clock.Now().Add(MonitorLogInterval),
		RecordsParsed:    100,
		RecordsProcessed: 100,
		OutputsMapped:    200,
		OutputsReduced:   200,
		FilesParsed:      10,
		OutputFilesSaved: 1,
	}
	suite.monitor.Run()
	suite.beginWorkItemMultiFile("test-item", 10)

	// begin parsing of files
	suite.beginFiles(10)
	filenames := suite.filenames(10)
	suite.parseRecords2(filenames, 10)
	suite.processRecords2(filenames, 10)
	suite.mapOutputs2(filenames, 20)

	// complete parsing of files
	suite.processFiles(10)

	suite.reduceOutputs2(filenames, 20)
	tick()
	// wait until monitor fires a status update
	suite.clock.Advance(MonitorLogInterval)
	tick()
	tick()
	select {
	case <-suite.workItemsOutputChan:
		break
	default:
		suite.Require().Fail("No output files")
	}
	suite.clock.Advance(MonitorLogInterval)
	tick()
	tick()
	// check for end of input signal
	select {
	case <-suite.endOfInput:
		break
	default:
		suite.Require().Fail("expected end of input signal")
	}
	tick()
	// check calls to status writer
	calls := suite.statusWriter.GetStatusCalls()
	suite.Require().Equal(2, len(calls), "Expected exactly two calls to WriteStatus")
	suite.Require().Equal(expectedStatus, calls[1], "Expected status.ProcessComplete to be true")
}

// Stuck files are seen as completed. This means possible data loss,
// but it also means not retrying the file.
func (suite *MonitorTestSuite) TestStuckFiles() {

	suite.monitor.Run()
	suite.beginWorkItemMultiFile("test-item", 10)
	// begin parsing file
	suite.beginFiles(10)
	filenames := suite.filenames(10)
	suite.parseRecords2(filenames, 10)
	suite.processRecords2(filenames, 10)
	suite.mapOutputs2(filenames, 20)

	suite.processFiles(10)

	suite.reduceOutputs2(filenames[:9], 20)
	suite.reduceOutputs(filenames[9], 19)
	tick()
	// wait until monitor fires a status update
	suite.clock.Advance(MonitorLogInterval)
	tick()
	select {
	case <-suite.workItemsOutputChan:
		suite.Require().Fail("Unexpected output of files")
	default:
		break
	}

	// advance past stuck timeout
	suite.clock.Advance(FileStuckTimeout)
	tick()

	select {
	case workItem := <-suite.workItemsOutputChan:
		suite.Require().Equal("test-item", workItem)
	default:
		suite.Require().Fail("No output files")
	}
}

func (suite *MonitorTestSuite) beginWorkItemSingleFile(workItemID string, filename string) {
	suite.workItemsReceivedChan <- &WorkItem{
		WorkItemID:  workItemID,
		SourceFiles: []string{filename},
	}
	tick()
}

func (suite *MonitorTestSuite) beginWorkItemMultiFile(workItemID string, numFiles int) {
	suite.workItemsReceivedChan <- &WorkItem{
		WorkItemID:  workItemID,
		SourceFiles: suite.filenames(numFiles),
	}
	tick()
}

func (suite *MonitorTestSuite) beginFile(filename string) {
	suite.filesParsingChan <- filename
	tick()
}

func (suite *MonitorTestSuite) filenames(num int) []string {
	filenames := make([]string, num)
	for i := 0; i < num; i++ {
		filenames[i] = fmt.Sprintf("file%v.txt", i)
	}
	return filenames
}

func (suite *MonitorTestSuite) beginFiles(num int) {
	for _, filename := range suite.filenames(num) {

		suite.filesParsingChan <- filename
	}
	tick()
}

func (suite *MonitorTestSuite) mapOutputs(filename string, num int) {
	for i := 0; i < num; i++ {
		tick()
		suite.outputsMappedChan <- filename
	}

}

func (suite *MonitorTestSuite) mapOutputs2(filenames []string, num int) {
	for _, filename := range filenames {
		for i := 0; i < num; i++ {
			suite.outputsMappedChan <- filename
		}
	}
	tick()
}

func (suite *MonitorTestSuite) reduceOutputs(filename string, num int) {
	for i := 0; i < num; i++ {
		tick()
		suite.outputsReducedChan <- filename
	}
}

func (suite *MonitorTestSuite) reduceOutputs2(filenames []string, num int) {
	for _, filename := range filenames {
		for i := 0; i < num; i++ {
			suite.outputsReducedChan <- filename
		}
	}
	tick()
}
func (suite *MonitorTestSuite) parseRecords(filename string, num int) {
	for i := 0; i < num; i++ {
		tick()
		suite.recordsParsedChan <- filename
	}
}

func (suite *MonitorTestSuite) parseRecords2(filenames []string, num int) {
	for _, filename := range filenames {
		for i := 0; i < num; i++ {
			tick()
			suite.recordsParsedChan <- filename
		}
	}
}

func (suite *MonitorTestSuite) processRecords(filename string, num int) {
	for i := 0; i < num; i++ {
		tick()
		suite.recordsProcessedChan <- filename
	}
}

func (suite *MonitorTestSuite) processRecords2(filenames []string, num int) {
	for _, filename := range filenames {
		for i := 0; i < num; i++ {
			suite.recordsProcessedChan <- filename
		}
	}
	tick()
}

func (suite *MonitorTestSuite) processFiles(num int) {
	for _, filename := range suite.filenames(num) {
		tick()
		suite.filesParsedChan <- filename
	}
}

func TestMonitorTestSuite(t *testing.T) {
	suite.Run(t, new(MonitorTestSuite))
}
