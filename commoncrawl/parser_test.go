package commoncrawl

import (
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
)

// Tests for Parser component
type ParserTestSuite struct {
	suite.Suite
	fileChan            chan FileDownload
	recordChan          chan ParsedRecord
	fileParsingListener chan string
	fileParsedListener  chan string
	recordReadListener  chan string
	parser              *Parser
	httpClient          HTTPClient
}

func (suite *ParserTestSuite) SetupTest() {
	suite.fileChan = make(chan FileDownload)
	suite.recordChan = make(chan ParsedRecord, 10)
	suite.fileParsingListener = make(chan string, 1)
	suite.fileParsedListener = make(chan string, 1)
	suite.recordReadListener = make(chan string, 10)
	reader := NewWARCReader(logrus.New())
	suite.parser = NewParser(
		reader,
		suite.fileChan,
		suite.recordChan,
		suite.fileParsingListener,
		suite.fileParsedListener,
		suite.recordReadListener,
		logrus.New())
	suite.parser.Run()

	suite.httpClient = &FileMockHTTPClient{}
}

// Verify that the parser can parse a WARC file
func (suite *ParserTestSuite) TestParseWARCFile() {
	fileDownload := suite.newFileDownload("test-item", "test.warc.gz")
	filesParsing := []string{}
	filesParsed := []string{}
	recordReadCount := 0
	recordsParsed := []ParsedRecord{}
	done := make(chan bool)
	suite.fileChan <- fileDownload
	go func() {
		for {
			select {
			case filename := <-suite.fileParsingListener:
				filesParsing = append(filesParsing, filename)
			case filename := <-suite.fileParsedListener:
				filesParsed = append(filesParsed, filename)
			case <-suite.recordReadListener:
				recordReadCount++
			case record := <-suite.recordChan:
				recordsParsed = append(recordsParsed, record)
			case <-time.After(time.Millisecond * 10):
				done <- true
				return
			}
		}
	}()
	tick()
	select {
	case <-done:
		break
	case <-time.After(time.Millisecond * 100):
		suite.Fail("Process did not complete in 100ms")
	}
	suite.Require().Equal([]string{"test.warc.gz"}, filesParsing)
	suite.Require().Equal([]string{"test.warc.gz"}, filesParsed)
	suite.Require().Equal(10, recordReadCount)
	suite.Require().Equal(10, len(recordsParsed))
	suite.verifyRecordMetadata(recordsParsed, "test-item", "test.warc.gz")

}

// Verify that the parser can parse a WET file
func (suite *ParserTestSuite) TestParseWETFile() {
	fileDownload := suite.newFileDownload("test-item", "test.warc.wet.gz")
	filesParsing := []string{}
	filesParsed := []string{}
	recordReadCount := 0
	recordsParsed := []ParsedRecord{}
	done := make(chan bool)
	suite.fileChan <- fileDownload
	go func() {
		for {
			select {
			case filename := <-suite.fileParsingListener:
				filesParsing = append(filesParsing, filename)
			case filename := <-suite.fileParsedListener:
				filesParsed = append(filesParsed, filename)
			case <-suite.recordReadListener:
				recordReadCount++
			case record := <-suite.recordChan:
				recordsParsed = append(recordsParsed, record)
			case <-time.After(time.Millisecond * 10):
				done <- true
				return
			}
		}
	}()
	tick()
	select {
	case <-done:
		break
	case <-time.After(time.Millisecond * 100):
		suite.Fail("Process did not complete in 100ms")
	}
	suite.Require().Equal([]string{"test.warc.wet.gz"}, filesParsing)
	suite.Require().Equal([]string{"test.warc.wet.gz"}, filesParsed)
	suite.Require().Equal(10, recordReadCount)
	suite.Require().Equal(10, len(recordsParsed))
	suite.verifyRecordMetadata(recordsParsed, "test-item", "test.warc.wet.gz")
}

// Verify that the parser can parse a WAT file
func (suite *ParserTestSuite) TestParseWATFile() {
	fileDownload := suite.newFileDownload("test-item", "test.warc.wat.gz")
	filesParsing := []string{}
	filesParsed := []string{}
	recordReadCount := 0
	recordsParsed := []ParsedRecord{}
	done := make(chan bool)
	suite.fileChan <- fileDownload
	go func() {
		for {
			select {
			case filename := <-suite.fileParsingListener:
				filesParsing = append(filesParsing, filename)
			case filename := <-suite.fileParsedListener:
				filesParsed = append(filesParsed, filename)
			case <-suite.recordReadListener:
				recordReadCount++
			case record := <-suite.recordChan:
				recordsParsed = append(recordsParsed, record)
			case <-time.After(time.Millisecond * 10):
				done <- true
				return
			}
		}
	}()
	tick()
	select {
	case <-done:
		break
	case <-time.After(time.Millisecond * 100):
		suite.Fail("Process did not complete in 100ms")
	}
	suite.Require().Equal([]string{"test.warc.wat.gz"}, filesParsing)
	suite.Require().Equal([]string{"test.warc.wat.gz"}, filesParsed)
	suite.Require().Equal(10, recordReadCount)
	suite.Require().Equal(10, len(recordsParsed))
	suite.verifyRecordMetadata(recordsParsed, "test-item", "test.warc.wat.gz")
}

func (suite *ParserTestSuite) verifyRecordMetadata(records []ParsedRecord, workItemID string, filename string) {
	for _, parsedRecord := range records {
		suite.Require().Equal(workItemID, parsedRecord.WorkItemID)
		suite.Require().Equal(filename, parsedRecord.SourceFile)
	}
}

func (suite *ParserTestSuite) newFileDownload(workItemID string, filename string) FileDownload {
	return &HTTPFileDownload{
		datasetHost: "https://commoncrawl.s3.amazonaws.com/",
		clock:       clockwork.NewRealClock(),
		filename:    filename,
		workItemID:  workItemID,
		httpClient:  suite.httpClient,
	}
}

func TestParserTestSuite(t *testing.T) {
	suite.Run(t, new(ParserTestSuite))
}
