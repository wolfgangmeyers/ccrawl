package commoncrawl

// Plugin provides specific functionality that can
// utilize the commoncrawl processing framework
type Plugin interface {
	// Creates a new plugin instance with the desired configuration
	NewInstance(config *PluginConfiguration) PluginInstance
}

// PluginInstance is a configured instance of a plugin
// that provides specific functionality that can utilize
// the commoncrawl processing framework
type PluginInstance interface {
	// Mapper that will process map-phase records for this plugin
	Mapper() RecordMapper
	// Mapper that will process reduce-phase records for this plugin.
	// Reduce-phase records are json blobs of the output items produced
	// by the map-phase mapper.
	ReductionMapper() RecordMapper
	// Reducer to use for this plugin
	Reducer() OutputReducer
	// Filter for output before it is saved
	OutputFilter() OutputFilter
}
