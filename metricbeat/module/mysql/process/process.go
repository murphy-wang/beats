/*
Package status fetches MySQL server status metrics.

For more information on the query it uses, see:
http://dev.mysql.com/doc/refman/5.7/en/show-status.html
*/
package process

import (
	"database/sql"

	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/metricbeat/mb"
	"github.com/elastic/beats/metricbeat/module/mysql"
	// c "github.com/elastic/beats/metricbeat/schema/mapstrstr"

	"fmt"
	"github.com/pkg/errors"
)

var (
	debugf = logp.MakeDebug("mysql-process")
)

func init() {
	if err := mb.Registry.AddMetricSet("mysql", "process", New, mysql.ParseDSN); err != nil {
		panic(err)
	}
}

// MetricSet for fetching MySQL server status.
type MetricSet struct {
	mb.BaseMetricSet
	db *sql.DB
}

// New creates and returns a new MetricSet instance.
func New(base mb.BaseMetricSet) (mb.MetricSet, error) {
	return &MetricSet{BaseMetricSet: base}, nil
}

// Fetch fetches status messages from a mysql host.
func (m *MetricSet) Fetch() (common.MapStr, error) {
	if m.db == nil {
		var err error
		m.db, err = mysql.NewDB(m.HostData().URI)
		if err != nil {
			return nil, errors.Wrap(err, "mysql-process fetch failed")
		}
	}

	status, err := m.loadStatus(m.db)
	if err != nil {
		return nil, err
	}

	event := eventMapping(status)

	if m.Module().Config().Raw {
		event["raw"] = rawEventMapping(status)
	}
	return event, nil
}

// loadStatus loads all status entries from the given database into an array.
func (m *MetricSet) loadStatus(db *sql.DB) (map[string]string, error) {
	// Returns the global status, also for versions previous 5.0.2
	rows, err := db.Query("SELECT ID,USER,HOST,DB,COMMAND,TIME,STATE,INFO FROM INFORMATION_SCHEMA.PROCESSLIST;")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	mysqlStatus := map[string]string{}

	for rows.Next() {
		var ID sql.NullString
		var USER sql.NullString
		var HOST sql.NullString
		var DB sql.NullString
		var COMMAND sql.NullString
		var TIME sql.NullString
		var STATE sql.NullString
		var INFO sql.NullString

		err = rows.Scan(&ID, &USER, &HOST, &DB, &COMMAND, &TIME, &STATE, &INFO)
		if err != nil {
			return nil, err
		}

		mysqlStatus["ID"] = ID.String
		mysqlStatus["USER"] = USER.String
		mysqlStatus["HOST"] = HOST.String
		mysqlStatus["DB"] = DB.String
		mysqlStatus["COMMAND"] = COMMAND.String
		mysqlStatus["TIME"] = TIME.String
		mysqlStatus["STATE"] = STATE.String
		mysqlStatus["INFO"] = INFO.String

	}

	return mysqlStatus, nil
}
