package commoncrawl

const (
	// PhaseMap indicates that the common crawl processing is in map phase
	PhaseMap = "map"
	// PhaseReduce indicates that the common crawl processing is in reduce phase
	PhaseReduce = "reduce"
)

// PluginConfiguration provides a way to customize the behavior of
// a plugin, output subfolder name, and number of shards.
type PluginConfiguration struct {
	PluginName   string            // Indicate which plugin to use
	ConfigData   map[string]string // Custom configuration data for the plugin
	OutputSet    string            // Name of output data set - subfolder will be named after this
	OutputShards int               // Number of output shards for this plugin configuration
}

// TaskConfiguration is the payload that is expected from SQS
// that instructs a common crawl lambda task on how to run.
type TaskConfiguration struct {
	// How many threads are used for parsing data files
	DownloadPoolSize int
	// S3 bucket for input files. Necessary for pulling data from private buckets.
	InputBucket          string
	OutputBucket         string
	OutputFolder         string
	Phase                string
	InputFilename        string
	DatasetHost          string // Used to download non-commoncrawl data files through http
	PluginConfigurations []*PluginConfiguration
}
