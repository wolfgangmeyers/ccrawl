package commoncrawl

import (
	"fmt"
	"log"
	"net/http"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus"
)

const (
	// BufferSize is set to 1K. Certain channels that require a buffer will have one of this size.
	BufferSize = 100
)

// A ControllerConfiguration contains the necessary dependencies that a Controller
// will need to function. Some dependencies are optional and have default values.
type ControllerConfiguration struct {
	AWSRegion         string             // Required - AWS region to use for AWS services
	TaskConfiguration *TaskConfiguration // Required - Input channel of filenames to process
	InputSource       InputSource        // Required - Produces work items to be processed
	EndOfProcess      chan<- bool        // Required - Output channel for end of process signal
	RecordReader      RecordReader       // Required - Reads an open file into a stream of records

	Plugins map[string]Plugin // Plugins that will provide specific mappers and reducers for processing
	// RecordMapper  RecordMapper  // Required - Mapper to convert records to OutputItems
	// OutputReducer OutputReducer // Required - Reducer to combine OutputItems

	// TODO: configure output writer from plugin
	OutputFileWriter OutputFileWriter   // Optional - Writer to save data to output files
	StatusWriter     StatusWriter       // Optional - Sends status notifications.
	HTTPClient       HTTPClient         // Optional - Client to make http calls to S3. Defaults to http.DefaultClient
	Clock            clockwork.Clock    // Optional - Pauses and checks the time. Defaults to an instance of RealClock
	Logger           logrus.FieldLogger // Optional - passes in a logger that will be used by all components
	// OutputFilter OutputFilter // Optional - Conditional filtering of output items
}

// The Controller bootstraps the common crawl process
type Controller struct {
	awsRegion         string             // AWS region to use for AWS services
	taskConfiguration *TaskConfiguration // Common crawl configuration
	inputSource       InputSource        // Produces work items to be processed
	endOfProcess      chan<- bool        // Output channel for end of process signal
	recordReader      RecordReader       // Reads an open file into a stream of records

	recordMappers  []RecordMapper           // List of record mappers to convert records to OutputItems
	outputReducers map[string]OutputReducer // Map of datatype to reducer. Reducers combine OutputItems
	outputFilter   OutputFilter             // Map of datatype to output filter. Only supported in reduce mode

	outputWriters      map[string]*OutputWriter // Map of output set to writer to save data to output files
	httpClient         HTTPClient               // Client to make http calls to S3
	clock              clockwork.Clock          // Used for pausing and checking the time
	statusWriter       StatusWriter             // Sends status notifications
	logger             logrus.FieldLogger       // Log system events and errors
	outputSortFunction func([]OutputItem)       // Custom sorting of output data

	// outputFilter OutputFilter // Conditional filtering of output items
}

// NewController returns a new instance of `Controller`
//
// * configuration - Contains all necessary dependencies for the process to function.
func NewController(
	conf *ControllerConfiguration,
) *Controller {
	controller := new(Controller)
	controller.taskConfiguration = conf.TaskConfiguration
	controller.awsRegion = conf.AWSRegion

	controller.endOfProcess = conf.EndOfProcess
	controller.recordReader = conf.RecordReader
	controller.logger = conf.Logger
	if controller.logger == nil {
		controller.logger = logrus.New()
	}

	controller.outputReducers = make(map[string]OutputReducer, len(conf.Plugins))
	controller.outputWriters = make(map[string]*OutputWriter, len(conf.Plugins))

	// Map phase requires at least one plugin,
	// reduce phase requires exactly one plugin
	if conf.TaskConfiguration.Phase == PhaseMap {
		if len(conf.TaskConfiguration.PluginConfigurations) < 1 {
			controller.logger.Fatalf("At least one plugin is required for map phase")
		}
	} else {
		if len(conf.TaskConfiguration.PluginConfigurations) != 1 {
			controller.logger.Fatalf("Exactly one plugin is required for reduce phase")
		}
	}

	for _, pluginConfiguration := range conf.TaskConfiguration.PluginConfigurations {
		// If OutputSet is not configured, default to the plugin name
		if pluginConfiguration.OutputSet == "" {
			pluginConfiguration.OutputSet = pluginConfiguration.PluginName
		}
		plugin, ok := conf.Plugins[pluginConfiguration.PluginName]
		if !ok {
			controller.logger.Fatalf("Plugin not found: %v", pluginConfiguration.PluginName)
		}
		pluginInstance := plugin.NewInstance(pluginConfiguration)
		var pluginMapper RecordMapper
		if conf.TaskConfiguration.Phase == PhaseMap {
			pluginMapper = pluginInstance.Mapper()
		} else {
			pluginMapper = pluginInstance.ReductionMapper()
		}
		controller.recordMappers = append(controller.recordMappers, pluginMapper)
		controller.outputReducers[pluginConfiguration.OutputSet] = pluginInstance.Reducer()
		controller.outputWriters[pluginConfiguration.OutputSet] = NewOutputWriter(
			conf.OutputFileWriter,
			conf.TaskConfiguration.OutputFolder,
			controller.logger,
			pluginConfiguration.OutputShards,
		)
		// This only matters for reduce phase, in which case there
		// will only be one output filter
		if conf.TaskConfiguration.Phase == PhaseReduce {
			controller.outputFilter = pluginInstance.OutputFilter()
		}
	}

	controller.logger = controller.logger.WithField("component", "controller")
	controller.inputSource = conf.InputSource

	// Assign values to optional dependencies
	if conf.HTTPClient == nil {
		log.Println("Using default http client")
		controller.httpClient = http.DefaultClient
	} else {
		log.Println("Using custom http client")
		controller.httpClient = conf.HTTPClient
	}
	if conf.Clock == nil {
		log.Println("Using real clock")
		controller.clock = clockwork.NewRealClock()
	} else {
		log.Println("Using custom clock")
		controller.clock = conf.Clock
	}

	if conf.StatusWriter == nil {
		log.Println("Using logging status writer")
		controller.statusWriter = NewLogStatusWriter(controller.logger)
	} else {
		log.Println("Using custom status writer")
		controller.statusWriter = conf.StatusWriter
	}
	return controller
}

func (controller *Controller) filterOutputData(data []OutputItem) []OutputItem {
	if controller.outputFilter == nil {
		return data
	}
	filtered := make([]OutputItem, 0, len(data))
	for i := 0; i < len(data); i++ {
		if controller.outputFilter.TestOutputItem(data[i]) {
			filtered = append(filtered, data[i])
		}
	}
	return filtered
}

// Run launches its own goroutine. It will continue running
// until an endOfInput signal is received, after which it will
// emit an endOfProcess signal and terminate.
func (controller *Controller) Run() {
	go func() {
		defer func() {
			err := recover()
			if err != nil {
				stack := debug.Stack()
				controller.logger.Fatalf("Fatal error in processing: %v\n%v", err, string(stack))
			}
		}()

		started := time.Now()
		controller.logger.Info(fmt.Sprintf(
			"Process starting at %v; %v downloaders, %v processors",
			started, controller.taskConfiguration.DownloadPoolSize,
			runtime.NumCPU()))
		// Create channels for communication between components
		fileDownloadsChannel := make(chan FileDownload)
		dataReduceChannel := make(chan RecordOutput, BufferSize*runtime.NumCPU())
		flushSignalChannel := make(chan FlushRequest)
		parserRecordOutputChannel := make(chan ParsedRecord, BufferSize+5)
		recordParsedListener := make(chan string, BufferSize*runtime.NumCPU())
		recordProcessedListener := make(chan string, BufferSize*runtime.NumCPU())
		outputsMappedListener := make(chan string, BufferSize*runtime.NumCPU())
		outputsReducedListener := make(chan string, BufferSize*runtime.NumCPU())
		fileParsingListener := make(chan string, BufferSize)
		fileParsedListener := make(chan string, BufferSize)
		completedWorkItems := make(chan string)
		stuckFilesDataPurge := make(chan string, BufferSize)
		workItemReceivedListener := make(chan *WorkItem, BufferSize)
		endOfInput := make(chan bool, 1)

		// Create pool of Parsers
		for i := 0; i < controller.taskConfiguration.DownloadPoolSize; i++ {
			parser := NewParser(
				controller.recordReader,
				fileDownloadsChannel,
				parserRecordOutputChannel,
				fileParsingListener,
				fileParsedListener,
				recordParsedListener,
				controller.logger)
			parser.Run()
		}

		fileDownloadFactory := NewFileDownloadFactory(
			controller.awsRegion,
			controller.taskConfiguration,
			controller.clock,
			controller.httpClient,
			controller.logger)
		downloader := NewDownloader(
			fileDownloadFactory,
			controller.clock,
			controller.inputSource.WorkItemsOutput(),
			workItemReceivedListener,
			fileDownloadsChannel,
			controller.logger)
		downloader.Run()

		// Create pool of processors
		processorPoolSize := runtime.NumCPU()
		for i := 0; i < processorPoolSize; i++ {
			processor := NewRecordProcessor(
				controller.recordMappers,
				parserRecordOutputChannel,
				dataReduceChannel,
				recordProcessedListener,
				outputsMappedListener,
				controller.logger)
			processor.Run()
		}

		// Create the OutputStore
		outputStore := NewOutputStore(
			controller.outputReducers,
			dataReduceChannel,
			flushSignalChannel,
			outputsReducedListener,
			stuckFilesDataPurge,
			controller.logger)
		outputStore.Run()

		// Create the monitor
		monitor := NewMonitor(
			controller.statusWriter,
			controller.clock,
			recordParsedListener,
			recordProcessedListener,
			outputsMappedListener,
			outputsReducedListener,
			workItemReceivedListener,
			fileParsingListener,
			fileParsedListener,
			completedWorkItems,
			endOfInput,
			controller.logger)
		monitor.Run()

		controller.inputSource.Run()
		// main process
		for {
			select {

			case workItemID := <-completedWorkItems:
				flushRequest := make(chan map[string]map[string]OutputItem)
				flushSignalChannel <- FlushRequest{
					Callback:   flushRequest,
					WorkItemID: workItemID,
				}
				data := <-flushRequest
				if data != nil && len(data) > 0 {
					for outputSet, dataItems := range data {
						outputItems := controller.filterOutputData(outputMapToSlice(dataItems))
						controller.logger.Infof("Saving %v output items of output set '%v' for work item '%v'", len(outputItems), outputSet, workItemID)
						controller.outputWriters[outputSet].WriteOutputData(outputSet, outputItems)
					}
				} else {
					controller.logger.Infof("Work item '%v' had no data", workItemID)
				}

				controller.inputSource.CompletedWorkItemsInput() <- workItemID
				// controller.logger.Info("Item marked as completed")
			case <-endOfInput:
				controller.logger.Infof("Controller received end of input signal")
				// Time to say goodbye
				// give input time to shut down and mark all tasks as completed
				controller.inputSource.CompletedWorkItemsInput() <- ""
				controller.logger.Info(fmt.Sprintf("Process complete. Time elapsed: %v\n", time.Since(started)))
				controller.endOfProcess <- true
				return
			}
		}
	}()
}
