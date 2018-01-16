package process

import (
	"github.com/elastic/beats/libbeat/common"
	s "github.com/elastic/beats/metricbeat/schema"
	c "github.com/elastic/beats/metricbeat/schema/mapstrstr"
)

var (
	schema = s.Schema{
		"ID":      c.Str("ID"),
		"USER":    c.Str("USER"),
		"HOST":    c.Str("HOST"),
		"DB":      c.Str("DB"),
		"COMMAND": c.Str("COMMAND"),
		"TIME":    c.Str("TIME"),
		"STATE":   c.Str("STATE"),
		"INFO":    c.Str("INFO"),
	}
)

// Map data to MapStr of server stats variables: http://dev.mysql.com/doc/refman/5.7/en/server-status-variables.html
// This is only a subset of the available values
func eventMapping(status map[string]string) common.MapStr {
	source := map[string]interface{}{}
	for key, val := range status {
		source[key] = val
	}
	return schema.Apply(source)
}

func rawEventMapping(status map[string]string) common.MapStr {
	source := common.MapStr{}
	for key, val := range status {
		// Only adds events which are not in the mapping
		if schema.HasKey(key) {
			continue
		}

		source[key] = val
	}
	return source
}
