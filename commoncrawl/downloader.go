package commoncrawl

import (
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus"
)

const (
	// PublicDatasetsPrefix is the source of all WARC files
	PublicDatasetsPrefix = "https://commoncrawl.s3.amazonaws.com/"
	// DownloadTimeout represents the time that the downloader will sit idle
	// before emitting a checkpoint signal.
	DownloadTimeout = time.Second * 10
	// DownloadDelay is the interval between each file download
	// This is to make sure that file downloads are staggered so that
	// a somewhat steady stream of data flows out of the system
	// without huge bursts.
	DownloadDelay = time.Millisecond
)

// WithRetries will continue to attempt an operation
// up to the specified number of retries with an exponential
// backoff. If the operation is still unsuccessful
// after all retries are exhausted (operation returns an error),
// that error is returned.
func WithRetries(clock clockwork.Clock, retries int, operation func() error) error {
	err := operation()
	backoff := time.Second * 2
	for err != nil && retries > 0 {
		clock.Sleep(backoff)
		retries--
		backoff *= 2
		err = operation()
	}
	return err
}

// HTTPClient is a stripped-down interface for fetching files over http. `http.DefaultClient` satisfies this interface.
type HTTPClient interface {
	// Get issues a GET to the specified URL
	Get(url string) (*http.Response, error)
}

// Downloader coordinates WARC file downloads
type Downloader struct {
	fileDownloadFactory      FileDownloadFactory // Creates FileDownload instances
	clock                    clockwork.Clock     // Used for exponential backoff
	workItemsInput           <-chan *WorkItem    // Input channel of file paths to download
	workItemReceivedListener chan<- *WorkItem    // Output listener for work item received signal
	downloadOutput           chan<- FileDownload // Output channel of `FileDownload` instances
	logger                   logrus.FieldLogger  // Log events
}

// NewDownloader returns a new instance of Downloader
//
// * fileDownloadFactory - Creates FileDownload instances
// * clock - Used for expoential backoff
// * checkpointSize - The number of files to emit before emitting a checkpoint signal
// * filenamesInput - Input channel of file paths to download
// * downloadOutput - Output channel of `FileDownload` instances
// * logger - Log events
func NewDownloader(
	fileDownloadFactory FileDownloadFactory,
	clock clockwork.Clock,
	workItemsInput <-chan *WorkItem,
	workItemReceivedListener chan<- *WorkItem,
	downloadOutput chan<- FileDownload,
	logger logrus.FieldLogger,
) *Downloader {
	downloader := new(Downloader)
	downloader.fileDownloadFactory = fileDownloadFactory
	downloader.clock = clock
	downloader.workItemsInput = workItemsInput
	downloader.workItemReceivedListener = workItemReceivedListener
	downloader.downloadOutput = downloadOutput
	downloader.logger = logger.WithField("component", "downloader")
	return downloader
}

// Run launches its own goroutine. It will begin pulling filenames from filenamesInput and emitting FileDownload
// objects from downloadOutput. Once a checkpoint is reached, a checkpoint signal
// will be emitted from checkpointsOutput. The value of the checkpoint signal will
// be the number of files processed since the last checkpoint. This may be less
// than the checkpoint size if the downloader has stopped receiving file names
// from filenamesInput.
func (downloader *Downloader) Run() {
	downloader.logger.Info("Downloader.Run() called")
	go func() {
		downloader.logger.Info("Downloader is running")
		defer func() {
			err := recover()
			if err != nil {
				stack := debug.Stack()
				downloader.logger.Fatalf("Fatal error in processing: %v\n%v", err, string(stack))
			}
		}()
		for {
			select {
			case workItem := <-downloader.workItemsInput:
				if workItem == nil {
					downloader.logger.Info("Downloader shutting down")
					return
				}
				downloader.workItemReceivedListener <- workItem
				for _, filename := range workItem.SourceFiles {
					filename = strings.TrimSpace(filename)
					downloader.downloadFile(workItem.WorkItemID, filename)
					downloader.clock.Sleep(DownloadDelay)
				}
			}
		}
	}()
}

func (downloader *Downloader) downloadFile(workItemID string, filename string) {
	download := downloader.fileDownloadFactory.NewFileDownload(workItemID, filename)
	// downloader.logger.Info(fmt.Sprintf("Sending file download '%v' downstream for work item '%v'", filename, workItemID))
	downloader.downloadOutput <- download
	// downloader.logger.Info(fmt.Sprintf("File download '%v' sent downstream for work item '%v'", filename, workItemID))
}
