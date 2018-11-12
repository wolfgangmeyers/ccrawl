package metadata

// Metadata is a typed representation of the JSON metadata
// contained in a WAT file.
type Metadata struct {
	Envelope *Envelope `json:"Envelope"`
}

// Envelope is the top level "Envelope" property
type Envelope struct {
	WARCHeaderLength    string           `json:"WARC-Header-Length"`
	BlockDigest         string           `json:"Block-Digest"`
	Format              string           `json:"Format"`
	ActualContentLength string           `json:"Actual-Content-Length"`
	HeaderMetadata      *HeaderMetadata  `json:"WARC-Header-Metadata"`
	PayloadMetadata     *PayloadMetadata `json:"Payload-Metadata"`
}

// HeaderMetadata holds the WARC headers
type HeaderMetadata struct {
	WARCRecordID      string `json:"WARC-Record-ID"`
	WARCWarcinfoID    string `json:"WARC-Warcinfo-ID"`
	ContentLength     string `json:"Content-Length"`
	WARCDate          string `json:"WARC-Date"`
	ContentType       string `json:"Content-Type"`
	WARCTargetURI     string `json:"WARC-Target-URI"`
	WARCIPAddress     string `json:"WARC-IP-Address"`
	WARCBlockDigest   string `json:"WARC-Block-Digest"`
	WARCPayloadDigest string `json:"WARC-Payload-Digest"`
	WARCTruncated     string `json:"WARC-Truncated"`
	WARCConcurrentTo  string `json:"WARC-Concurrent-To"`
	WARCType          string `json:"WARC-Type"`
}

// Hyperlink represents the contents of a hyperlink from
// a webpage.
type Hyperlink struct {
	Href   string `json:"href"`
	Path   string `json:"path"`
	URL    string `json:"url"`
	Alt    string `json:"alt"`
	Target string `json:"target"`
	Title  string `json:"title"`
}

// PayloadMetadata is metadata about the payload
type PayloadMetadata struct {
	ActualContentType    string                `json:"Actual-Content-Type"`
	TrailingSlopLength   string                `json:"Trailing-Slop-Length"`
	HTTPResponseMetadata *HTTPResponseMetadata `json:"HTTP-Response-Metadata"`
}

type HTTPResponseMessage struct {
	Version string `json:"Version"`
	Reason  string `json:"Reason"`
	Status  string `json:"Status"`
}

// HTTPResponseMetadata is metadata about the http response
type HTTPResponseMetadata struct {
	EntityLength            string               `json:"Entity-Length"`
	HeadersLength           string               `json:"Headers-Length"`
	EntityTrailingSlopBytes string               `json:"Entity-Trailing-Slop-Bytes"`
	EntityDigest            string               `json:"Entity-Digest"`
	ResponseMessage         *HTTPResponseMessage `json:"Response-Message"`
	HTMLMetadata            *HTMLMetadata        `json:"HTML-Metadata"`
}

// HTMLMetadata contains metadata about an html page
type HTMLMetadata struct {
	Links []*Hyperlink `json:"Links"`
	Head  *HTMLHead    `json:"Head"`
}

// HTMLHeadLink represents a `<link>` element found in the `<head>` of a page.
type HTMLHeadLink struct {
	Rel  string `json:"rel"`
	Type string `json:"type"`
	URL  string `json:"url"`
	Path string `json:"path"`
}

// HTMLHeadScript represents a `<script>` element found in the `<head>` of a page
type HTMLHeadScript struct {
	Type string `json:"type"`
	URL  string `json:"url"`
	Path string `json:"path"`
}

// HTMLHeadMeta represents a `<meat>` element found in the `<head>` of a page
type HTMLHeadMeta struct {
	Content string `json:"content"`
	Name    string `json:"name"`
}

// HTMLHead represents the metadata about the `<head>` of a page
type HTMLHead struct {
	Title   string            `json:"Title"`
	Link    []*HTMLHeadLink   `json:"Link"`
	Scripts []*HTMLHeadScript `json:"Scripts"`
	Metas   []*HTMLHeadMeta   `json:"Metas"`
}
