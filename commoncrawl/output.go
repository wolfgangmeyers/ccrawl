package commoncrawl

import (
	"runtime/debug"

	"github.com/sirupsen/logrus"
)

const (
	// DefaultOutputStoreCapacity is set to 100K. This is the initial capacity of the OutputStore.
	DefaultOutputStoreCapacity = 100 * 1000
)

// OutputItem represents output data produced when processing a `WARCRecord`.
type OutputItem interface {
	// OutputSet indicates the output folder for the data
	OutputSet() string
	// Key is the unique identifier for this output.
	Key() string
	// Value is the data contained by the output. This can be the same object as the OutputItem.
	Value() interface{}
}

// OutputItems implements sort.Interface for []OutputItem based on
// the Key() return value.
type OutputItems []OutputItem

func (a OutputItems) Len() int           { return len(a) }
func (a OutputItems) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a OutputItems) Less(i, j int) bool { return a[i].Key() < a[j].Key() }

// An OutputReducer is responsible for reducing the values of OutputItems with the same key
// into a single OutputItem.
type OutputReducer interface {
	// Reduce merges two OutputItems with the same key into a single item.
	// The details of how the merge is performed is dependent on the implementation.
	Reduce(existingItem OutputItem, newItem OutputItem) OutputItem
}

// FlushRequest represents a request to flush the OutputStore
type FlushRequest struct {
	WorkItemID string                                  // ID of work item data to flush
	Callback   chan<- map[string]map[string]OutputItem // Callback for flushed data
}

// The OutputStore accumulates data that is output by the processor pool
//
// Each OutputItem that is sent to the OutputStore is accumulated by unique key returned by the Key() method.
// Upon receiving a flush signal, the OutputStore will send the current data set and clear it,
// resetting the OutputStore to its initial state.
type OutputStore struct {
	reducers            map[string]OutputReducer         // Map of data type to reducer. Used for reducing OutputItems
	data                map[string]map[string]OutputItem // Where all of the data is stored
	dataInput           <-chan RecordOutput              // Input channel of OutputItems
	flushSignal         <-chan FlushRequest              // InputChannel of flush signals
	outputReducedOutput chan<- string                    // Output channel of reduced outputs
	stuckFileInput      <-chan string                    // Input channel of stuck file names
	logger              logrus.FieldLogger               // Log events
}

// NewOutputStore returns a new instance of OutputStore
//
// * reducer - Used for reducing OutputItems
// * dataInput - Input channel of OutputItems
// * flushSignal - InputChannel of flush signals
// * outputReducedOutput - Output channel of reduced outputs
func NewOutputStore(
	reducers map[string]OutputReducer,
	dataInput <-chan RecordOutput,
	flushSignal <-chan FlushRequest,
	outputReducedOutput chan<- string,
	stuckFileInput <-chan string,
	logger logrus.FieldLogger,
) *OutputStore {
	store := new(OutputStore)
	store.dataInput = dataInput
	store.flushSignal = flushSignal
	store.reducers = reducers
	store.outputReducedOutput = outputReducedOutput
	store.stuckFileInput = stuckFileInput
	store.data = make(map[string]map[string]OutputItem)
	store.logger = logger.WithField("component", "store")
	return store
}

// Run launches its own goroutine. The store will listen to both
// input channels. When it receives OutputItems from the dataInput channel,
// it will use the reducer to combine it with any existing OutputItem and save
// the result, or simply save the OutputItem if no previous entry exists.
// Upon receiving a flush signal, the entire data set will be returned on the
// provided channel, and a new empty dataset will replace it in the store.
func (store *OutputStore) Run() {
	go func() {
		defer func() {
			err := recover()
			if err != nil {
				stack := debug.Stack()
				store.logger.Fatalf("Fatal error in processing: %v\n%v", err, string(stack))
			}
		}()
		workerFlushReqs := map[string]chan FlushRequest{}
		for {
			select {
			case filename := <-store.stuckFileInput:
				// purge stuck files from the store
				delete(store.data, filename)
				delete(workerFlushReqs, filename)
			case recordOutput := <-store.dataInput:
				data, exists := store.data[recordOutput.OutputItem.OutputSet()]
				if !exists {
					// store.logger.Infof("Received new work item: %v", recordOutput.WorkItemID)
					store.data[recordOutput.OutputItem.OutputSet()] = make(map[string]OutputItem, DefaultOutputStoreCapacity)
					data = store.data[recordOutput.OutputItem.OutputSet()]
				}
				outputItem := recordOutput.OutputItem
				// workerInputs[recordOutput.WorkItemID] <- outputItem
				existingItem, exists := data[outputItem.Key()]
				if exists {
					reducer, has := store.reducers[outputItem.OutputSet()]
					if !has {
						store.logger.Fatalf("No reducer found for output item with output set '%v' and key '%v'", outputItem.OutputSet(), outputItem.Key())
					}
					outputItem = reducer.Reduce(existingItem, outputItem)
				}
				data[outputItem.Key()] = outputItem
				store.outputReducedOutput <- recordOutput.SourceFile

			case flushRequest := <-store.flushSignal:
				data := store.data
				store.data = make(map[string]map[string]OutputItem)
				flushRequest.Callback <- data
			}
		}
	}()
}

// An OutputFilter can modify the stream of output items before they are processed by the OutputWriter
type OutputFilter interface {
	// TestOutputItem returns true if the output item should be saved
	TestOutputItem(item OutputItem) bool
}
