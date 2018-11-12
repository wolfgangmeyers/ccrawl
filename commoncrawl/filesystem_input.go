package commoncrawl

import (
	"fmt"

	"time"

	"github.com/sirupsen/logrus"
)

const (
	// CompletedFilenamesPath is the path to the file containing the list of completed WARC files
	CompletedFilenamesPath = "completed-files.txt"
)

// FilesystemInput reads in a list of WARC file paths from a local file
// and pushes each file path to an output channel for downstream processing.
// Files that have already been marked as completed from previous checkpoints
// will not be sent.
type FilesystemInput struct {
	indexFile               string             // Path to the file containing the list of WARC file paths
	completedFilenames      []string           // List of files that have been marked completed in previous checkpoints
	completedFileLookup     map[string]bool    // Set of completed files, included for efficient lookup
	workItemsOutput         chan *WorkItem     // Output channel of WorkItem
	completedWorkItemsInput chan string        // Input channel of completed WARC file paths
	abortedWorkItemsInput   chan string        // Input channel of aborted files
	workItemReader          WorkItemReader     // Reads work items from queue messages
	logger                  logrus.FieldLogger // Log events
	lineIndex               int                // Keep track of place in file
}

// NewFilesystemInput returns a new instance of FilesystemInput
//
// * filenamesOutput Output channel of WARC file paths
func NewFilesystemInput(indexFile string, workItemReader WorkItemReader, logger logrus.FieldLogger) *FilesystemInput {
	input := new(FilesystemInput)
	input.indexFile = indexFile
	input.workItemsOutput = make(chan *WorkItem)
	input.completedWorkItemsInput = make(chan string)
	input.abortedWorkItemsInput = make(chan string)
	input.completedFileLookup = map[string]bool{}
	input.workItemReader = workItemReader
	input.logger = logger
	return input
}

// WorkItemsOutput returns an output channel of WorkItem
func (input *FilesystemInput) WorkItemsOutput() <-chan *WorkItem {
	return input.workItemsOutput
}

// CompletedWorkItemsInput returns an input channel of completed work item ids
func (input *FilesystemInput) CompletedWorkItemsInput() chan<- string {
	return input.completedWorkItemsInput
}

// AbortedWorkItemsInput returns an input channel of aborted work item ids
func (input *FilesystemInput) AbortedWorkItemsInput() chan<- string {
	return input.abortedWorkItemsInput
}

// loadCompletedFiles loads the list of WARC files that have already been processed.
func (input *FilesystemInput) loadCompletedFiles() {
	err := CreateFileIfAbsent(CompletedFilenamesPath)
	if err != nil {
		panic(err)
	}
	completedFilenames, err := LoadFileLines(CompletedFilenamesPath)
	if err != nil {
		panic(err)
	}
	input.completedFilenames = completedFilenames
	for _, filename := range input.completedFilenames {
		input.completedFileLookup[filename] = true
	}
}

func (input *FilesystemInput) addCompletedFile(completedFile string) {
	input.completedFilenames = append(input.completedFilenames, completedFile)
	input.completedFileLookup[completedFile] = true
	WriteFileLines(CompletedFilenamesPath, input.completedFilenames)
}

func (input *FilesystemInput) nextWorkItem(filePaths []string) *WorkItem {
	for input.lineIndex < len(filePaths) {
		// TODO: if already completed ignore
		filepath := filePaths[input.lineIndex]
		input.lineIndex++
		if !input.completedFileLookup[filepath] {
			workItem, err := input.workItemReader.ReadWorkItem(filepath)
			if err == nil {
				return workItem
			}
			input.logger.Errorf("Error reading work item '%v': '%v'", filepath, err.Error())
		}

	}
	time.Sleep(time.Second)

	return nil
}

// Run launches its own goroutine. It will read the list of file paths
// found in `indexFile` and emit each path to `filenamesOutput`.
func (input *FilesystemInput) Run() {
	go func() {
		input.loadCompletedFiles()
		warcFilePaths, err := LoadFileLines(input.indexFile)
		if err != nil {
			fmt.Println(warcFilePaths)
			fmt.Println(err)
			panic(err)
		}
		workItem := input.nextWorkItem(warcFilePaths)
		for {
			select {
			case completedFile := <-input.completedWorkItemsInput:
				if completedFile == "" {
					input.logger.Info("FilesystemInput shutting down")
					return
				}
				input.addCompletedFile(completedFile)
			case input.workItemsOutput <- workItem:
				workItem = input.nextWorkItem(warcFilePaths)
			}
		}
	}()
}
