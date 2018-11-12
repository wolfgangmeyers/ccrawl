# Example: word count
The Word Count example parses the text content of each record of the common crawl
(WET format) and outputs a list of word counts per domain name.

## Running the example
Run the following at the command prompt from this directory:

`go build`
`./domainwc ../../../testdata/wet.paths.1`

If all goes well this should create an output directory with a single output file.
