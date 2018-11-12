package main

import (
	"fmt"
	"log"
	"os"
	"path"

	"regexp"

	"net/http"
	"net/url"

	"github.com/wolfgangmeyers/ccrawl/commoncrawl"
	"github.com/jonboulle/clockwork"
	"github.com/wolfgangmeyers/go-warc/warc"
)

var nonWordCharacters = regexp.MustCompile("[^a-zA-Z]")

// This package implements a domain word count over the common crawl, and
// serves as an example of all of the boilerplate needed to extract data
// from the common crawl.

// DomainWordCount is the output data that we care about.
//
// The Domain name is the key for each item, so the data for each webpage will
// be combined together with any other webpages found on the same domain.
type DomainWordCount struct {
	Domain     string         `json:"domain"`     // Domain Name is the key
	WordCounts map[string]int `json:"wordCounts"` // WordCounts is the number of times each word was found on the domain
}

// Key is the unique identifier for this output.
func (domainWordCount *DomainWordCount) Key() string {
	return domainWordCount.Domain
}

// Value is the data contained by the output.
func (domainWordCount *DomainWordCount) Value() interface{} {
	// This is called when data is being saved
	// filter out any words that occurred less than 3 times
	toRemove := []string{}
	for word, count := range domainWordCount.WordCounts {
		if count < 3 {
			toRemove = append(toRemove, word)
		}
	}
	for _, word := range toRemove {
		delete(domainWordCount.WordCounts, word)
	}
	return domainWordCount
}

// WordcountRecordMapper maps WARCRecords to word counts.
//
// Note that WordcountRecordMapper expects data to be in the WET file format.
// See http://commoncrawl.org/the-data/get-started/#WET-Format
type WordcountRecordMapper struct{}

// MapRecord maps a WARCRecord into zero to many OutputItems.
//
// * record - A WARCRecord object containing data about a webpage
// * output - Each OutputItem that is sent to this callback will be reduced in the output.
func (mapper *WordcountRecordMapper) MapRecord(record *warc.WARCRecord, output func(commoncrawl.OutputItem)) {
	if record.GetType() == "response" {
		data, err := record.GetPayload().Read(-1)
		if err != nil {
			// This really shouldn't happen
			panic(err)
		}
		u, err := url.Parse(record.GetUrl())
		// just ignore invalid urls
		if err == nil {
			domainName := u.Host
			if domainName == "" {
				return
			}
			tokens := nonWordCharacters.Split(string(data), -1)
			counts := map[string]int{}
			for i := 0; i < len(tokens); i++ {
				token := tokens[i]
				if len(token) > 0 {
					counts[token]++
				}
			}
			if len(counts) > 0 {
				output(&DomainWordCount{
					Domain:     domainName,
					WordCounts: counts,
				})
			}
		}
	}
}

// WordcountReducer combines domain word counts
type WordcountReducer struct{}

// Reduce merges two OutputItems with the same key into a single item.
// The details of how the merge is performed is dependent on the implementation.
func (reducer *WordcountReducer) Reduce(existingItem commoncrawl.OutputItem, newItem commoncrawl.OutputItem) commoncrawl.OutputItem {
	existingWordcounts, existingOk := existingItem.(*DomainWordCount)
	newWordcounts, newOk := newItem.(*DomainWordCount)
	if !existingOk || !newOk {
		log.Fatal("Invalid inputs for reducer")
	}
	if existingWordcounts.Domain != newWordcounts.Domain {
		log.Fatalf("Mismatching keys for reducer: %v, %v\n", existingWordcounts.Domain, newWordcounts.Domain)
	}
	for word, count := range newWordcounts.WordCounts {
		existingWordcounts.WordCounts[word] += count
	}
	return existingWordcounts
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Usage: wordcount <warc paths file>")
	}
	conf := commoncrawl.Configuration{}
	err := commoncrawl.LoadConfiguration(&conf)
	if err != nil {
		panic(err)
	}
	warcPathsFile := os.Args[1]
	pwd, _ := os.Getwd()
	warcPathsFile = path.Join(pwd, warcPathsFile)
	fmt.Printf("Loading lines from %v\n", warcPathsFile)
	filenamesChannel := make(chan string)
	checkpointCompletedFilesChannel := make(chan []string)
	filenamesInput := commoncrawl.NewFilesystemInput(warcPathsFile, filenamesChannel, checkpointCompletedFilesChannel)
	filenamesInput.Run()
	endOfProcess := make(chan bool)
	controller := commoncrawl.NewController(&commoncrawl.ControllerConfiguration{
		Configuration:   &conf,
		FilenamesInput:  filenamesChannel,
		FilenamesOutput: checkpointCompletedFilesChannel,
		EndOfProcess:    endOfProcess,
		RecordMapper:    new(WordcountRecordMapper),
		OutputReducer:   new(WordcountReducer),
		OutputWriter:    commoncrawl.NewFilesystemWriter("output"),
		HTTPClient:      http.DefaultClient,
		Clock:           clockwork.NewRealClock(),
		StatusWriter:    commoncrawl.NewLogStatusWriter(),
	})
	controller.Run()

	<-endOfProcess
	fmt.Println("Done!")
}
