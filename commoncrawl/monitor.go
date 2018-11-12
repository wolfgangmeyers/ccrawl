package commoncrawl

import (
	"runtime/debug"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus"
)

const (
	// MonitorLogInterval is interval between each log message from the monitor
	MonitorLogInterval = time.Second * 10
	// MonitorCompletedFilesOutputInterval - The amount of time between each check for completed work items
	// If the monitor detects completed files, an attempt is made to send them
	// through the completed work items output channel. If the send is blocked,
	// the monitor tries again after the next duration.
	MonitorCompletedFilesOutputInterval = time.Millisecond * 100
	// ProcessStuckTimeout is the amount of time the monitor will wait for some update
	// before killing the process
	ProcessStuckTimeout = time.Minute * 10
	// FileStuckTimeout is the amount of time the monitor will wait for some update to
	// a file before giving up on processing it.
	FileStuckTimeout = time.Minute * 5
	// FileCompletionCooldown is the minimum time since last update before marking a file as completed
	FileCompletionCooldown = time.Second * 9
)

// A StatusWriter is responsible for conveying the current state of the system.
// The details of how this is done is left up to the implementation.
type StatusWriter interface {
	// WriteStatus sends a notification about the current state of the system.
	WriteStatus(status ProcessStatus)
}

// ProcessStatus represents the current state of the common crawl process
type ProcessStatus struct {
	LastUpdate       time.Time // Last time the process received any kind of update
	RecordsParsed    int       // Total # of records parsed
	RecordsProcessed int       // Total # of records fully processed
	OutputsMapped    int       // Total # of outputs mapped
	OutputsReduced   int       // Total # of outputs reduced
	FilesInProcess   int       // # of files currently being processed
	FilesParsed      int       // Total # of files parsed
	OutputFilesSaved int       // Total # of output files that have been saved
	ProcessComplete  bool      // True if process is completed
}

// FileStatus represents the current state of an individual file that is being processed.
type FileStatus struct {
	LastUpdate       time.Time
	RecordsParsed    int
	RecordsProcessed int
	OutputsMapped    int
	OutputsReduced   int
	EndOfFile        bool
}

// WorkItemStatus represents the current state of a work item
type WorkItemStatus struct {
	WorkItem       *WorkItem
	CompletedFiles []string
}

// The Monitor receives signals from other parts in the system, prints out
// aggregated metrics at a regular interval, and emits a signal when the system
// is ready to perform a save of collected data.
type Monitor struct {
	statusWriter             StatusWriter       // Sends status notifications
	clock                    clockwork.Clock    // Emits events at an interval
	recordsParsedInput       <-chan string      // Input channel of parsed records count
	recordsProcessedInput    <-chan string      // Input channel of processed records count
	outputsMappedInput       <-chan string      // Input channel of mapped outputs
	outputsReducedInput      <-chan string      // Input channel of reduced outputs
	workItemReceivedInput    <-chan *WorkItem   // Input channel of work items
	filesParsingInput        <-chan string      // Input channel of files being parsed
	filesParsedInput         <-chan string      // Input channel of files fully parsed
	completedWorkItemsOutput chan<- string      // Output channel of files completed since last checkpoint
	endOfInput               chan<- bool        // Output channel indicating end of input (process complete)
	logger                   logrus.FieldLogger // Log events
}

// NewMonitor returns a new instance of Monitor
//
// * statusWriter - Sends status notifications
// * clock - Emits events at an interval
// * checkpointSize - Maximum number of files to complete before saving
// * recordsParsedInput - Input channel of parsed records count
// * recordsProcessedInput - Input channel of processed records count
// * outputsMappedInput - Input channel of mapped outputs
// * outputsReducedInput - Input channel of reduced outputs
// * workItemReceivedInput - Input channel of work items
// * filesParsingInput - Input channel of files being parsed
// * filesParsedInput - Input channel of files fully parsed
// * checkpointFilesOutput - Output channel of files completed since last checkpoint
// * endOfInput - Output channel indicating end of input (process complete)
// * logger - Log events
func NewMonitor(
	statusWriter StatusWriter,
	clock clockwork.Clock,
	recordsParsedInput <-chan string,
	recordsProcessedInput <-chan string,
	outputsMappedInput <-chan string,
	outputsReducedInput <-chan string,
	workItemReceivedInput <-chan *WorkItem,
	filesParsingInput <-chan string,
	filesParsedInput <-chan string,
	completedWorkItemsOutput chan<- string,
	endOfInput chan<- bool,
	logger logrus.FieldLogger,
) *Monitor {
	monitor := new(Monitor)
	monitor.statusWriter = statusWriter
	monitor.clock = clock
	monitor.recordsParsedInput = recordsParsedInput
	monitor.recordsProcessedInput = recordsProcessedInput
	monitor.outputsMappedInput = outputsMappedInput
	monitor.outputsReducedInput = outputsReducedInput
	monitor.workItemReceivedInput = workItemReceivedInput
	monitor.filesParsingInput = filesParsingInput
	monitor.filesParsedInput = filesParsedInput
	monitor.completedWorkItemsOutput = completedWorkItemsOutput
	monitor.endOfInput = endOfInput
	monitor.logger = logger.WithField("component", "monitor")
	return monitor
}

// Run launches its own goroutine. It will proceed to monitor inputs and
// emit outputs as designed, while printing out a summary of the state of
// the process to the console at a regular interval.
func (monitor *Monitor) Run() {
	go func() {
		defer func() {
			err := recover()
			if err != nil {
				stack := debug.Stack()
				monitor.logger.Fatalf("Fatal error in processing: %v\n%v", err, string(stack))
			}
		}()
		// Keep track of state during the process
		processStatus := ProcessStatus{}
		fileStatus := map[string]*FileStatus{}
		// map filename to work item
		workItemStatus := map[string]*WorkItemStatus{}

		ticker := monitor.clock.After(MonitorLogInterval)
		completedFilesTicker := monitor.clock.After(MonitorCompletedFilesOutputInterval)
		processStatus.LastUpdate = monitor.clock.Now()
		completedWorkItems := []string{}
		for {
			select {
			case workItem := <-monitor.workItemReceivedInput:
				stat := &WorkItemStatus{
					WorkItem:       workItem,
					CompletedFiles: []string{},
				}
				for _, filename := range workItem.SourceFiles {
					workItemStatus[filename] = stat
				}
				monitor.logger.Infof("Work item received: '%v'", workItem.WorkItemID)
			// A file is being parsed
			case filename := <-monitor.filesParsingInput:
				processStatus.LastUpdate = monitor.clock.Now()
				processStatus.FilesInProcess++
				// Files without a work item can happen due to stuck files. Ignore
				// any additional input for the files, because the output has already
				// been saved for them.
				_, ok := workItemStatus[filename]
				if !ok {
					break
				}
				stat, ok := fileStatus[filename]
				if !ok {
					fileStatus[filename] = &FileStatus{}
					stat = fileStatus[filename]
				}
				stat.LastUpdate = monitor.clock.Now()

				fileStatus[filename].LastUpdate = monitor.clock.Now()
				// monitor.logger.Infof("Beginning file parse: '%v'", filename)

			// A file was fully parsed
			case filename := <-monitor.filesParsedInput:
				processStatus.LastUpdate = monitor.clock.Now()
				// monitor.logger.Infof("File fully parsed: '%v'", filename)
				processStatus.FilesParsed++
				// Files without a work item can happen due to stuck files. Ignore
				// any additional input for the files, because the output has already
				// been saved for them.
				_, ok := workItemStatus[filename]
				if !ok {
					break
				}

				stat, ok := fileStatus[filename]
				if !ok {
					fileStatus[filename] = &FileStatus{}
					stat = fileStatus[filename]
				}
				stat.LastUpdate = monitor.clock.Now()

				stat.EndOfFile = true

			// A record was parsed
			case filename := <-monitor.recordsParsedInput:
				processStatus.LastUpdate = monitor.clock.Now()
				processStatus.RecordsParsed++
				// Files without a work item can happen due to stuck files. Ignore
				// any additional input for the files, because the output has already
				// been saved for them.
				_, ok := workItemStatus[filename]
				if !ok {
					break
				}
				stat, ok := fileStatus[filename]
				if !ok {
					fileStatus[filename] = &FileStatus{}
					stat = fileStatus[filename]
				}
				stat.LastUpdate = monitor.clock.Now()

				stat.RecordsParsed++

			// A record was processed
			case filename := <-monitor.recordsProcessedInput:
				processStatus.LastUpdate = monitor.clock.Now()
				processStatus.RecordsProcessed++
				// Files without a work item can happen due to stuck files. Ignore
				// any additional input for the files, because the output has already
				// been saved for them.
				_, ok := workItemStatus[filename]
				if !ok {
					break
				}
				stat, ok := fileStatus[filename]
				if !ok {
					fileStatus[filename] = &FileStatus{}
					stat = fileStatus[filename]
				}
				stat.LastUpdate = monitor.clock.Now()

				stat.RecordsProcessed++

			case filename := <-monitor.outputsMappedInput:
				processStatus.LastUpdate = monitor.clock.Now()
				processStatus.OutputsMapped++
				// Files without a work item can happen due to stuck files. Ignore
				// any additional input for the files, because the output has already
				// been saved for them.
				_, ok := workItemStatus[filename]
				if !ok {
					break
				}
				stat, ok := fileStatus[filename]
				if !ok {
					fileStatus[filename] = &FileStatus{}
					stat = fileStatus[filename]
				}
				stat.LastUpdate = monitor.clock.Now()

				stat.OutputsMapped++

			case filename := <-monitor.outputsReducedInput:
				processStatus.LastUpdate = monitor.clock.Now()
				processStatus.OutputsReduced++
				// Files without a work item can happen due to stuck files. Ignore
				// any additional input for the files, because the output has already
				// been saved for them.
				_, ok := workItemStatus[filename]
				if !ok {
					break
				}
				stat, ok := fileStatus[filename]
				if !ok {
					fileStatus[filename] = &FileStatus{}
					stat = fileStatus[filename]
				}
				stat.LastUpdate = monitor.clock.Now()

				stat.OutputsReduced++
			case <-completedFilesTicker:
			CompletedWorkItems:
				for len(completedWorkItems) > 0 {
					select {
					case monitor.completedWorkItemsOutput <- completedWorkItems[0]:
						processStatus.LastUpdate = monitor.clock.Now()
						completedWorkItems = completedWorkItems[1:]
						processStatus.OutputFilesSaved++
					default:
						break CompletedWorkItems
					}
				}
				completedFilesTicker = monitor.clock.After(MonitorCompletedFilesOutputInterval)
			// Time to print out state and check if it's time to save.
			case <-ticker:
				if monitor.clock.Since(processStatus.LastUpdate) > ProcessStuckTimeout {
					monitor.logger.Fatal("Stuck process detected, Exiting")
				}
				// Audit files in progress and see if any have completed or are stuck

				completedFiles := []string{}
				for filename, stat := range fileStatus {
					fileCompleted := stat.EndOfFile && stat.RecordsParsed == stat.RecordsProcessed &&
						stat.OutputsMapped == stat.OutputsReduced && monitor.clock.Since(stat.LastUpdate) > FileCompletionCooldown
					fileStuck := monitor.clock.Since(stat.LastUpdate) >= FileStuckTimeout
					if fileStuck {
						monitor.logger.WithFields(
							logrus.Fields{
								"RecordsParsed":    stat.RecordsParsed,
								"RecordsProcessed": stat.RecordsProcessed,
								"OutputsMapped":    stat.OutputsMapped,
								"OutputsReduced":   stat.OutputsReduced,
								"LastUpdate":       stat.LastUpdate,
								"EndOfFile":        stat.EndOfFile,
							},
						).Errorf("Stuck file detected: '%v'", filename)
					}
					if fileCompleted || fileStuck {
						completedFiles = append(completedFiles, filename)
						workItemStat := workItemStatus[filename]
						workItemStat.CompletedFiles = append(workItemStat.CompletedFiles, filename)
						if len(workItemStat.CompletedFiles) == len(workItemStat.WorkItem.SourceFiles) {
							completedWorkItems = append(completedWorkItems, workItemStat.WorkItem.WorkItemID)
						}
						delete(workItemStatus, filename)
					}
				}
				for _, filename := range completedFiles {
					delete(fileStatus, filename)
					processStatus.FilesInProcess--
				}

				// for _, workItemID := range completedWorkItems {
				// 	monitor.logger.Infof("Work item completed: '%v'", workItemID)
				// 	monitor.completedWorkItemsOutput <- workItemID
				// 	monitor.logger.Infof("Controller notified of completion for '%v'", workItemID)
				// 	processStatus.OutputFilesSaved++
				// }

				if len(fileStatus) == 0 && processStatus.FilesParsed > 0 && len(completedWorkItems) == 0 {
					select {
					case monitor.endOfInput <- true:
						monitor.logger.Info("End of input detected")
						processStatus.ProcessComplete = true
						monitor.statusWriter.WriteStatus(processStatus)
						return
					default:
						break
					}
				}
				monitor.statusWriter.WriteStatus(processStatus)

				ticker = monitor.clock.After(MonitorLogInterval)
			}
		}
	}()
}
