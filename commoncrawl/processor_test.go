package commoncrawl

// import (
// 	"os"
// 	"testing"
// 	"time"

// 	"github.com/sirupsen/logrus"
// 	"github.com/stretchr/testify/suite"
// 	"github.com/wolfgangmeyers/go-warc/warc"
// )

// // Dummy output from the mapper
// type DummyOutputItem struct{}

// func (item *DummyOutputItem) Key() string {
// 	return ""
// }

// func (item *DummyOutputItem) Value() interface{} {
// 	return nil
// }

// // Bare-bones implementation to test the RecordProcessor
// type DummyMapper struct {
// 	itemsPerRecord int // Number of DummyOutputItem emitted per record
// }

// func (mapper *DummyMapper) MapRecord(record interface{}, output func(OutputItem)) {
// 	for i := 0; i < mapper.itemsPerRecord; i++ {
// 		output(&DummyOutputItem{})
// 	}
// }

// type ProcessorTestSuite struct {
// 	suite.Suite
// 	recordChan             chan ParsedRecord
// 	dataChan               chan RecordOutput
// 	recordCompleteListener chan string
// 	outputMappedListener   chan string
// 	processor              *RecordProcessor
// 	mapper                 *DummyMapper
// 	warcFile               *warc.WARCFile
// }

// func (suite *ProcessorTestSuite) SetupTest() {
// 	file, err := os.Open("../testdata/test.warc.gz")
// 	if err != nil {
// 		panic(err)
// 	}
// 	warcFile, err := warc.NewWARCFile(file)
// 	if err != nil {
// 		panic(err)
// 	}
// 	suite.warcFile = warcFile

// 	suite.recordChan = make(chan ParsedRecord, 10)
// 	suite.dataChan = make(chan RecordOutput, 10)
// 	suite.recordCompleteListener = make(chan string, 10)
// 	suite.outputMappedListener = make(chan string, 100)
// 	suite.mapper = &DummyMapper{}
// 	suite.processor = NewRecordProcessor(
// 		suite.mapper,
// 		suite.recordChan,
// 		suite.dataChan,
// 		suite.recordCompleteListener,
// 		suite.outputMappedListener,
// 		logrus.New())
// 	suite.processor.Run()
// }

// // When the mapper yields 0 output items per record,
// // the result is 10 records processed and 0 output items
// func (suite *ProcessorTestSuite) TestYieldZeroPerRecord() {
// 	suite.sendRecords()
// 	outputItems, recordCount, outputCount := suite.getResults()
// 	suite.Require().Equal(0, len(outputItems))
// 	suite.Require().Equal(10, recordCount)
// 	suite.Require().Equal(0, outputCount)
// }

// // When the mapper yields 1 output items per record,
// // the result is 10 records processed and 10 output items
// func (suite *ProcessorTestSuite) TestYieldOnePerRecord() {
// 	suite.mapper.itemsPerRecord = 1
// 	suite.sendRecords()
// 	outputItems, recordCount, outputCount := suite.getResults()
// 	suite.Require().Equal(10, len(outputItems))
// 	suite.Require().Equal(10, recordCount)
// 	suite.Require().Equal(10, outputCount)
// }

// // When the mapper yields 10 output items per record,
// // the result is 10 records processed and 100 output items
// func (suite *ProcessorTestSuite) TestYieldTenPerRecord() {
// 	suite.mapper.itemsPerRecord = 10
// 	suite.sendRecords()
// 	outputItems, recordCount, outputCount := suite.getResults()
// 	suite.Require().Equal(100, len(outputItems))
// 	suite.Require().Equal(10, recordCount)
// 	suite.Require().Equal(100, outputCount)
// }

// // Verify that source file and work item id are set
// func (suite *ProcessorTestSuite) TestRecordOutputMetadata() {
// 	suite.mapper.itemsPerRecord = 1
// 	suite.sendRecords()
// 	outputItems, _, _ := suite.getResults()
// 	for _, recordOutput := range outputItems {
// 		suite.Require().Equal("test-item", recordOutput.WorkItemID)
// 		suite.Require().Equal("test.txt.gz", recordOutput.SourceFile)
// 	}
// }

// // Pull results from the processor
// func (suite *ProcessorTestSuite) getResults() (outputItems []RecordOutput, recordCount int, outputCount int) {
// 	outputItems = []RecordOutput{}
// 	recordCount = 0
// 	for {
// 		select {
// 		case <-suite.recordCompleteListener:
// 			recordCount++
// 		case <-suite.outputMappedListener:
// 			outputCount++
// 		case item := <-suite.dataChan:
// 			outputItems = append(outputItems, item)
// 		case <-time.After(time.Millisecond * 50):
// 			return
// 		}
// 	}
// }

// // Push input to the processor
// func (suite *ProcessorTestSuite) sendRecords() {
// 	for {
// 		record, err := suite.warcFile.ReadRecord()
// 		if err != nil {
// 			break
// 		}
// 		suite.recordChan <- ParsedRecord{
// 			WorkItemID: "test-item",
// 			SourceFile: "test.txt.gz",
// 			Record:     record,
// 		}
// 	}
// }

// func TestProcessorTestSuite(t *testing.T) {
// 	suite.Run(t, new(ProcessorTestSuite))
// }
