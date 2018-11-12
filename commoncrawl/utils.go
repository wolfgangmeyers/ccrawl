package commoncrawl

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"net/http"
	"os"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"runtime"

	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus"
)

// FileMockHTTPClient emulates an http client by reading files out of testdata/
type FileMockHTTPClient struct {
	Clock   clockwork.Clock
	Latency time.Duration
}

// Get gets the file name from the specified URL and opens it as a file from the testdata directory.
// The only populated field of the returned Response is the Body, which is
// a handle on the open file.
func (client *FileMockHTTPClient) Get(url string) (*http.Response, error) {
	if client.Clock != nil {
		<-client.Clock.After(client.Latency)
	}
	i := strings.LastIndex(url, "/")
	filename := "../testdata/" + url[i:]
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	return &http.Response{
		Body: file,
	}, nil
}

// LoadFileLines reads a file into a slice of strings
// Each line has whitespace trimmed.
//
// * filename - Name of the file that should be loaded
func LoadFileLines(filename string) ([]string, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(data), "\n")
	for i := 0; i < len(lines); i++ {
		lines[i] = strings.TrimSpace(lines[i])
	}
	return lines, nil
}

// WriteFileLines writes a slice of strings to a file
// Each string is written to the file, followed by a newline character.
//
// * filename - Name of the destination file
// * lines - Slice of strings that should be written
func WriteFileLines(filename string, lines []string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	for _, line := range lines {
		file.WriteString(line)
		file.WriteString("\n")
	}
	return file.Close()
}

// CreateFileIfAbsent creates the specified file if it
// does not already exist.
//
// * filename - Name of the file that should be created
func CreateFileIfAbsent(filename string) error {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		file, err := os.Create(filename)
		if err != nil {
			return err
		}
		file.Close()
	}
	return nil
}

// StringSliceToSet converts a slice of strings
// into a set of strings
func StringSliceToSet(strings []string) map[string]bool {
	result := make(map[string]bool, len(strings))
	for _, s := range strings {
		result[s] = true
	}
	return result
}

// StringSetToSlice converts a set of strings
// into a slice of strings
func StringSetToSlice(strings map[string]bool) []string {
	result := make([]string, 0, len(strings))
	for s := range strings {
		result = append(result, s)
	}
	return result
}

// SerializeOutputItemsInParallel converts output items into JSON byte slices using
// every CPU available.
//
// * items - The items that need to be serialized into json
// * logger - Log events
func SerializeOutputItemsInParallel(items []OutputItem, logger logrus.FieldLogger) [][]byte {
	type itemInput struct {
		item  OutputItem
		index int
	}
	type itemOutput struct {
		data  []byte
		index int
	}
	inChan := make(chan *itemInput, len(items))
	outChan := make(chan *itemOutput)
	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			for {
				select {
				case item := <-inChan:
					if item == nil {
						return
					}
					blob, err := json.Marshal(item.item.Value())
					if err != nil {
						logger.WithField("item", item.item.Key()).Fatalf("Error serializing item: '%v'", err.Error())
					}
					outChan <- &itemOutput{blob, item.index}
				}
			}
		}()
	}
	result := make([][]byte, len(items))
	for i := 0; i < len(items); i++ {
		inChan <- &itemInput{items[i], i}
	}
	for i := 0; i < len(items); i++ {
		select {
		case data := <-outChan:
			result[data.index] = data.data
		}
	}
	close(inChan)
	close(outChan)
	return result
}

func min(x int, y int) int {
	if x < y {
		return x
	}
	return y
}

// SegmentOutputItems will break up a slice of output items into multiple segments,
// invoking the callback for each segment. All segments will be at most maxSegmentSize.
//
// * items - Input list of items to break into segments
// * maxSegmentSize - No segment should be larger than this
// * callback - Invoke for each segment
func SegmentOutputItems(items []OutputItem, maxSegmentSize int, callback func([]OutputItem)) {
	for i := 0; i < len(items); i++ {
		if i%maxSegmentSize == 0 {
			j := min(len(items), i+maxSegmentSize)
			callback(items[i:j])
		}
	}
}

// def segment_items_best(items, max_count_per_segment):
//     for i in (i for i in xrange(len(items)) if i % max_count_per_segment == 0):
//         yield items[i:i + max_count_per_segment]

func outputMapToSlice(outputItems map[string]OutputItem) []OutputItem {
	result := make([]OutputItem, 0, len(outputItems))
	for _, outputItem := range outputItems {
		result = append(result, outputItem)
	}
	sort.Sort(OutputItems(result))
	return result
}

func subscribeFileListeners(
	filenamesInput <-chan string,
	listeners []chan<- string,
	listenerTimeout time.Duration,
) {
	go func() {
		for {
			select {
			case filename := <-filenamesInput:
				for _, listener := range listeners {
					select {
					// Wait up to a second to deliver a message to each listener
					case listener <- filename:
					case <-time.After(time.Second):
						break
					}
				}
			}
		}

	}()
}

// tick pauses execution for one millisecond.
func tick() {
	time.Sleep(time.Millisecond)
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

// CatchFatalError returns a function that can be used
// to report panics. Once the error is reported, the process
// exits abnormally.
//
// Usage:
// ```
// defer commoncrawl.CatchFatalError(logger)()
// ```
func CatchFatalError(logger logrus.FieldLogger) func() {
	return func() {
		err := recover()
		if err != nil {
			stack := debug.Stack()
			logger.Fatalf("Fatal error in processing: %v\n%v", err, string(stack))
		}
	}
}
